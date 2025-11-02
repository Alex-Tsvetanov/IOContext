#include "common/request_processing.hpp"
#include "server/client_workflow.hpp"
#include "server/server.hpp"
#include <cstddef>
#include <cstring>
#include <iostream>
#include <liburing.h>
#include <memory>
#include <sys/poll.h>
#include <thread>
#ifdef __linux__
#include "common/socket.hpp"
#include <liburing/io_uring.h>
#include <sys/types.h>
#include "common/workflow.hpp"
#include "server/worker.hpp"

#define DEBUG
#ifdef DEBUG
#include "common/debug_log.hpp"
#endif

Worker::Worker(Server* owner_, fd_t listen_fd, const WorkerConfig& cfg)
  : listen_fd_(listen_fd)
  , cfg_(cfg)
  , owner(owner_)
{
  io_uring_params p{};
#ifdef IORING_SETUP_SUBMIT_ALL
  p.flags |= IORING_SETUP_SUBMIT_ALL;
#endif
#ifdef IORING_SETUP_HYBRID_IOPOLL
  p.flags |= IORING_SETUP_HYBRID_IOPOLL;
#endif
#ifdef IORING_SETUP_DEFER_TASKRUN
  p.flags |= IORING_SETUP_DEFER_TASKRUN;
#endif
#ifdef IORING_SETUP_COOP_TASKRUN
  p.flags |= IORING_SETUP_COOP_TASKRUN;
#endif
#ifdef IORING_SETUP_TASKRUN_FLAG
  p.flags |= IORING_SETUP_TASKRUN_FLAG;
#endif
  if (int rc = io_uring_queue_init_params(8192, &ring_, &p); rc != 0)
  {
    io_uring_params zero{};
    rc = io_uring_queue_init_params(8192, &ring_, &zero);
    if (rc != 0)
    {
      std::cerr << "io_uring_queue_init_params failed with error code: " << -rc << std::flush;
      std::exit(1);
    }
  }

  ring_fd_ = ring_.ring_fd; // liburing exposes this field
}

Worker::~Worker()
{
  io_uring_queue_exit(&ring_);
}

inline io_uring_sqe* Worker::new_event_for_posting()
{
  struct io_uring_sqe* sqe;
  do
  {
    sqe = io_uring_get_sqe(&ring_);
    if (sqe)
    {
      io_uring_sqe_set_data(sqe, nullptr);
      return sqe;
    }
    io_uring_submit(&ring_);
    need_submit_ = 0;
    std::this_thread::yield();
  } while (true);
  return sqe;
}

void Worker::post_accept()
{
  if (use_ms_accept_ && ms_accept_armed_)
  {
    return;
  }

  auto* sqe = new_event_for_posting();

  // For accept, we use a special marker value to distinguish it
  io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
  io_uring_sqe_set_data(sqe, nullptr);

  if (use_ms_accept_)
  {
    sqe->ioprio |= IORING_ACCEPT_MULTISHOT;
    ms_accept_armed_ = true;
  }
  need_submit_++;
  io_uring_submit(&ring_);
}

void Worker::handle_accept(io_uring_cqe* cqe)
{
  if (cqe->res <= 0)
  {
    if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP)
    {
      use_ms_accept_ = false;
      ms_accept_armed_ = false;
    }
    post_accept();
    return;
  }

  fd_t cfd = cqe->res;

  auto* c = table_.add(cfd);
  c->keepalive_limit = cfg_.max_keepalive_requests;
  c->owner = this;

  ts_std::cout << "Accepted new connection: fd=" << cfd << std::flush;

  // Start the client workflow coroutine on the server's coroutine pool
  owner->get_coro_pool().spawn(client_workflow_coro(this, c));

  if (use_ms_accept_)
  {
    if ((cqe->flags & IORING_CQE_F_MORE) == 0)
    {
      // The multishot accept is no longer armed; re-arm it once.
      ms_accept_armed_ = false;
      post_accept();
    }
    // If F_MORE is set: do not post another accept (it’s still armed).
  }
  else
  {
    // Single-shot mode: post one new accept per completion.
    post_accept();
  }
}

void Worker::post_recv(PerClientStorage* c, char* buffer, size_t buflen, void* coro_addr)
{
  auto* sqe = new_event_for_posting();

  io_uring_prep_recv(sqe, c->fd, buffer, buflen, 0);
  io_uring_sqe_set_data(sqe, coro_addr);

  need_submit_++;
  io_uring_submit(&ring_);
  ts_std::cout << "[Coroutine " << coro_addr << "] Posted recv for fd=" << c->fd << std::flush;
}

void Worker::handle_recv(io_uring_cqe* cqe, void* user_data)
{
  if (!user_data)
  {
    return;
  }

  // user_data is the coroutine handle address
  auto handle = std::coroutine_handle<Task::promise_type>::from_address(user_data);
  auto& promise = handle.promise();

  auto* c = table_.get(promise.client_fd);
  if (!c)
  {
    return;
  }

  if (cqe->res <= 0)
  {
    c->closing = true;
    promise.io_result = cqe->res; // Store error code
    if (cqe->res == -EBADF)
    {
      table_.erase(promise.client_fd);
      return;
    }
    // Still signal the coroutine so it can clean up
    owner->get_coro_pool().signal_ready(handle);
    return;
  }

#ifdef DEBUG
  ts_std::cout << "Recv completion: fd=" << promise.client_fd << ", res=" << cqe->res << std::flush;
#endif

  // Store the number of bytes received
  promise.io_result = cqe->res;

  // Signal the coroutine that recv is complete
  // The coroutine's buffer now contains the data
  owner->get_coro_pool().signal_ready(handle);
}

void Worker::post_send(PerClientStorage* c, const struct msghdr* msg, void* coro_addr)
{
  auto* sqe = new_event_for_posting();

  io_uring_prep_sendmsg(sqe, c->fd, msg, MSG_NOSIGNAL);
  io_uring_sqe_set_data(sqe, coro_addr);

  need_submit_++;
  io_uring_submit(&ring_);
}

void Worker::handle_send(io_uring_cqe* cqe, void* user_data)
{
  if (!user_data)
  {
    return;
  }

  // user_data is the coroutine handle address
  auto handle = std::coroutine_handle<Task::promise_type>::from_address(user_data);
  auto& promise = handle.promise();

  auto* c = table_.get(promise.client_fd);
  if (!c)
  {
    return;
  }

  if (cqe->res < 0)
  {
    promise.io_result = cqe->res; // Store error code
    if (cqe->res == -EAGAIN || cqe->res == -EWOULDBLOCK)
    {
      // Will retry in coroutine
      owner->get_coro_pool().signal_ready(handle);
      return;
    }
    c->closing = true;
    owner->get_coro_pool().signal_ready(handle);
    return;
  }

#ifdef DEBUG
  ts_std::cout << "Send completion: fd=" << promise.client_fd << ", res=" << cqe->res << std::flush;
#endif

  // Store the number of bytes sent
  promise.io_result = cqe->res;

  // Signal the coroutine with send result
  owner->get_coro_pool().signal_ready(handle);
}

void Worker::post_close(fd_t fd, void* coro_addr)
{
  auto* c = table_.get(fd);
  if (!c)
  {
    table_.erase(fd);
  }
  else
  {
    return;
  }

  {
    auto* sqe = new_event_for_posting();
    io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(-1));
  }

  {
    auto* sqe = new_event_for_posting();
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_flags(sqe, IOSQE_IO_HARDLINK);
    io_uring_sqe_set_data(sqe, coro_addr);
  }
}

void Worker::handle_close([[maybe_unused]] io_uring_cqe* cqe, [[maybe_unused]] void* user_data)
{
  // Nothing to do for close completions
}

void Worker::run()
{
#ifdef DEBUG
  ts_std::cout << "worker thread started, ring_fd=" << ring_fd_ << ", listen_fd=" << listen_fd_ << std::flush;
#endif
  for (int i = 0; i < cfg_.accepts_per_worker; ++i)
  {
    post_accept();
  }
  if (need_submit_)
  {
    io_uring_submit(&ring_);
    need_submit_ = 0;
  }

  io_uring_cqe* cqes[8192];

  while (true)
  {
    io_uring_submit_and_wait(&ring_, 1);

    const unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, sizeof(cqes) / sizeof(cqes[0]));

    for (unsigned i = 0; i < n; ++i)
    {
      io_uring_cqe* cqe = cqes[i];
      void* coro_addr = io_uring_cqe_get_data(cqe);

#ifdef DEBUG
      ts_std::cout << "Completion: coro_addr=" << coro_addr << ", res=" << cqe->res << std::flush;
#endif

      // Check if this is an accept operation (coro_addr == listen_fd)
      if (coro_addr == nullptr)
      {
        handle_accept(cqe);
      }
      else if (coro_addr == reinterpret_cast<void*>(-1))
      {
        // noop for cancel completion
      }
      else
      {
        // This is a coroutine handle address (recv or send)
        // We can distinguish by checking the promise's state or just handle both
        // For now, we'll call a generic handler that checks the operation
        auto handle = std::coroutine_handle<Task::promise_type>::from_address(coro_addr);

        // The coroutine will know what operation it was waiting for
        // based on its own state. Just signal it.
        owner->get_coro_pool().signal_ready(handle);
      }

      io_uring_cqe_seen(&ring_, cqe);
    }

    if (need_submit_)
    {
      io_uring_submit(&ring_);
      need_submit_ = 0;
    }
  }
}

#endif
