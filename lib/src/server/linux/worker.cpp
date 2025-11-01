#include "common/request_processing.hpp"
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
  // #ifdef IORING_SETUP_SINGLE_ISSUER
  //   p.flags |= IORING_SETUP_SINGLE_ISSUER;
  // #endif
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

  EventData* ev = new EventData;
  ev->fd = listen_fd_;
  ev->op = Workflow::Accept;
  ev->hold = nullptr;

  io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
  io_uring_sqe_set_data(sqe, ev);

  if (use_ms_accept_)
  {
    sqe->ioprio |= IORING_ACCEPT_MULTISHOT;
  }
  need_submit_++;
}

void Worker::handle_accept(io_uring_cqe* cqe, [[maybe_unused]] EventData* data)
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
  set_nonblock(cfd);
  set_tcp_opts(cfd);

  auto* c = table_.add(cfd);
  c->keepalive_limit = cfg_.max_keepalive_requests;
  c->owner = this;

  post_recv(c);

  if ((cqe->flags & IORING_CQE_F_MORE) == 0)
  {
    use_ms_accept_ = true;
    ms_accept_armed_ = false;
  }
  post_accept();
}

void Worker::post_recv(PerClientStorage* c)
{
  auto* sqe = new_event_for_posting();

  std::shared_ptr<std::vector<char>> newbuf = std::make_shared<std::vector<char>>(Worker::BUF_SZ);

  EventData* ev = new EventData;
  ev->fd = c->fd;
  ev->op = Workflow::Recv;
  ev->hold = newbuf;

  io_uring_prep_recv(sqe, c->fd, newbuf->data(), newbuf->size(), 0);
  io_uring_sqe_set_data(sqe, ev);

  need_submit_++;
}

void Worker::handle_recv(io_uring_cqe* cqe, EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }

  if (cqe->res <= 0)
  {
    c->closing = true;
    if (cqe->res == -EBADF)
    {
      table_.erase(data->fd);
      return;
    }
    post_close(data->fd);
    return;
  }

#ifdef DEBUG
  ts_std::cout << "Workflow::Recv completion: fd=" << data->fd << ", res=" << cqe->res
               << ", data ptr=" << data->hold.get() << " " << data->hold.use_count() << std::flush;
#endif // DEBUG

  std::shared_ptr<std::vector<char>> hold = std::static_pointer_cast<std::vector<char>>(data->hold);
  hold->resize(cqe->res);
#ifdef DEBUG
  ts_std::cout << "Data received at: " << hold.get() << " " << hold.use_count();
  ts_std::cout << "\nData received:\n" << std::string_view(hold->data(), hold->size()) << std::flush;
#endif // DEBUG
  c->completed_recv_buffs.push(hold);
  post_internal_event(c, Workflow::Parse);
  post_recv(c);
}

void Worker::post_send(PerClientStorage* c)
{
  if (c->send_inflight || c->requests_ready.empty())
  {
    return;
  }

  c->send_inflight = true;
  io_uring_sqe* sqe;
  for (const auto& req : c->requests_ready)
  {
    sqe = new_event_for_posting();
    EventData* ev = new EventData;
    ev->fd = c->fd;
    ev->op = Workflow::Send;
    ev->hold = req;
    io_uring_prep_sendmsg(sqe, c->fd, &req->batched_send_data.msg, MSG_NOSIGNAL);
    io_uring_sqe_set_data(sqe, ev);
    need_submit_++;
    c->requests_in_flight.insert(req);
  }
  c->requests_ready.clear();
}

void Worker::handle_send(io_uring_cqe* cqe, EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }

  if (cqe->res < 0)
  {
    if (cqe->res == -EAGAIN || cqe->res == -EWOULDBLOCK)
    {
      std::shared_ptr<RequestProcessing> req = std::static_pointer_cast<RequestProcessing>(data->hold);
      c->requests_in_flight.erase(req);
      c->requests_ready.insert(req);
      post_send(c);
      return;
    }
    c->closing = true;
    post_close(data->fd);
    return;
  }

  std::shared_ptr<RequestProcessing> req = std::static_pointer_cast<RequestProcessing>(data->hold);

  size_t sent = static_cast<size_t>(cqe->res);

  // total bytes in this batch
  size_t total = 0;
  for (size_t i = 0; i < req->batched_send_data.msg.msg_iovlen; ++i)
  {
    total += req->batched_send_data.msg.msg_iov[i].iov_len;
  }

  if (sent < total)
  {
    // advance iovecs by 'sent'
    size_t off = sent;
    int i = 0;
    auto iov = req->batched_send_data.iov;
    int cnt = req->batched_send_data.msg.msg_iovlen;

    while (i < cnt && off >= iov[i].iov_len)
    {
      off -= iov[i].iov_len;
      ++i;
    }
    if (i < cnt)
    {
      // trim first unsent iovec
      iov[i].iov_base = static_cast<char*>(iov[i].iov_base) + off;
      iov[i].iov_len -= off;
      // point msg to the remaining tail
      req->batched_send_data.msg.msg_iov = &iov[i];
      req->batched_send_data.msg.msg_iovlen = cnt - i;

      // re-submit the remaining part; keep tx_inflight intact
      if (auto* sqe = new_event_for_posting())
      {
        EventData* ev = new EventData;
        ev->fd = c->fd;
        ev->op = Workflow::Send;
        ev->hold = req;
        io_uring_prep_sendmsg(sqe, c->fd, &req->batched_send_data.msg, MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, ev);
        need_submit_++;
        return;
      }
      // If we fail to get an SQE, fall back to POLLOUT so we don't lose progress
      return;
    }
  }

  // Full batch sent
  c->requests_in_flight.erase(req);
  c->send_inflight = false;

  if (c->on_send_completed())
  {
    c->closing = true;
    post_close(data->fd);
  }
  else
  {
    post_send(c); // continue draining
  }
}

void Worker::post_close(fd_t fd)
{
  auto* c = table_.get(fd);
  if (!c)
    return;

  c->closing = true;
  if (auto* sqe = new_event_for_posting())
  {
    EventData* ev = new EventData;
    ev->fd = fd;
    ev->op = Workflow::Cancelling;
    ev->hold = nullptr;

    io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_flags(sqe, IOSQE_IO_HARDLINK);
    io_uring_sqe_set_data(sqe, ev);
  }
  if (auto* sqe = new_event_for_posting())
  {
    EventData* ev = new EventData;
    ev->fd = fd;
    ev->op = Workflow::Closed;
    ev->hold = nullptr;
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, ev);
  }
}

void Worker::handle_close([[maybe_unused]] io_uring_cqe* cqe, EventData* data)
{
  table_.erase(data->fd);
}

void Worker::post_internal_event(PerClientStorage* c, Workflow wf)
{
  auto* sqe = new_event_for_posting();

  EventData* ev = new EventData;
  ev->fd = c->fd;
  ev->op = wf;
  ev->hold = nullptr;

  io_uring_prep_nop(sqe);
  io_uring_sqe_set_data(sqe, ev);
}

void Worker::handle_internal_event(EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }
  c->handle(data->op);
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
      EventData* data = reinterpret_cast<EventData*>(io_uring_cqe_get_data(cqe));
#ifdef DEBUG
      ts_std::cout << "\nCompletion: fd=" << data->fd << ", res=" << cqe->res << ", op=" << static_cast<int>(data->op)
                   << ", data ptr=" << data->hold.get() << " " << (data->hold ? data->hold.use_count() : 0)
                   << std::flush;
#endif // DEBUG
      switch (data->op)
      {
      case Workflow::Accept:
        handle_accept(cqe, data);
        break;

      case Workflow::Recv:
        handle_recv(cqe, data);
        break;

      case Workflow::Send:
        handle_send(cqe, data);
        break;

      case Workflow::Parse:
      case Workflow::FindHandler:
      case Workflow::GenerateResponse:
        handle_internal_event(data);
        break;

      case Workflow::RequestFlush: {
        auto* c = table_.get(data->fd);
        if (!c)
        {
          break;
        }
        // A send is about to be completed and it would fire the next batch of data to send
        if (c->send_inflight)
        {
          break;
        }

        post_send(c);
      }
      break;

      case Workflow::RequestClose:
        handle_close(cqe, data);
        break;

      case Workflow::Cancelling:
        // no-op, just a barrier
        break;

      case Workflow::Closed:
        handle_close(cqe, data);
        break;
      } // switch
      io_uring_cqe_seen(&ring_, cqe);
      switch (data->op)
      {
      case Workflow::Accept: {
        if (!use_ms_accept_)
          delete data;
        break;
      }
      default:
       delete data;
       break;
      }
    }

    if (need_submit_)
    {
      io_uring_submit(&ring_);
      need_submit_ = 0;
    }
  }
}

#endif
