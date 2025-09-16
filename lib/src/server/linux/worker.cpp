#include "common/request_processing.hpp"
#include "ds/set.hpp"
#include <cstddef>
#include <cstring>
#include <iostream>
#include <liburing.h>
#include <memory>
#include <sys/poll.h>
#ifdef __linux__
#include "common/socket.hpp"
#include <liburing/io_uring.h>
#include <sys/types.h>
#include "common/workflow.hpp"
#include "server/worker.hpp"

#ifdef DEBUG
#include "common/debug_log.hpp"
#endif

Worker::~Worker()
{
  io_uring_queue_exit(&ring_);
}

Worker::Worker(Server* owner_, fd_t listen_fd, const WorkerConfig& cfg)
  : listen_fd_(listen_fd)
  , cfg_(cfg)
  , owner(owner_)
{
  // Try SQPOLL + COOP; fallback if unsupported.
  io_uring_params p{};
  p.flags |= IORING_SETUP_SQPOLL;
#ifdef IORING_SETUP_COOP_TASKRUN
  p.flags |= IORING_SETUP_COOP_TASKRUN;
#endif
#ifdef IORING_SETUP_TASKRUN_FLAG
  p.flags |= IORING_SETUP_TASKRUN_FLAG;
#endif
  p.sq_thread_idle = 2000; // ms
  int rc = io_uring_queue_init_params(8192, &ring_, &p);
  if (rc != 0)
  {
    io_uring_params zero{};
    rc = io_uring_queue_init_params(8192, &ring_, &zero);
    if (rc != 0)
    {
      std::cerr << "io_uring_queue_init_params failed with error code: " << -rc << std::endl;
      std::exit(1);
    }
  }

  ring_fd_ = ring_.ring_fd; // liburing exposes this field

  // init_buffer_pool();
}

inline io_uring_sqe* Worker::get_sqe_or_submit()
{
  return ::get_sqe_or_submit(ring_);
}

bool Worker::post_accept()
{
  if (auto* sqe = get_sqe_or_submit())
  {
    EventData* ev = new EventData;
    ev->fd = listen_fd_;
    ev->op = Workflow::Accept;
    ev->hold = nullptr;

    io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, ev);
    return true;
  }
  return false;
}

void Worker::handle_accept(io_uring_cqe* cqe, [[maybe_unused]] EventData* data)
{
#ifdef DEBUG
  ts_std::cout << "Workflow::Accept completion for fd=" << data->fd << ", res=" << cqe->res << std::endl;
#endif // DEBUG

  if (cqe->res <= 0)
  {
    if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP)
    {
      use_ms_accept_ = false;
      ms_accept_armed_ = false;
    }
    if (post_accept())
    {
      need_submit_++;
    }
    return;
  }

  fd_t cfd = cqe->res;
  set_nonblock(cfd);
  set_tcp_opts(cfd);

  auto* c = table_.add(cfd);
  c->keepalive_limit = cfg_.max_keepalive_requests;
  c->owner = this;

#ifdef DEBUG
  ts_std::cout << "Accepted new connection, fd=" << cfd << std::endl;
#endif

  if (post_recv(c))
  {
    need_submit_++;
  }

  if (!use_ms_accept_)
  {
    if (post_accept())
    {
      need_submit_++;
    }
  }
  else if ((cqe->flags & IORING_CQE_F_MORE) == 0)
  {
    ms_accept_armed_ = false;
    if (post_accept())
    {
      need_submit_++;
    }
  }
}

bool Worker::post_recv(PerClientStorage* c)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    TSSet<std::vector<char>>::element newbuf = std::make_shared<std::vector<char>>(Worker::BUF_SZ);
    EventData* ev = new EventData;
    ev->fd = c->fd;
    ev->op = Workflow::Recv;
    ev->hold = newbuf;
    io_uring_prep_recv(sqe, c->fd, newbuf->data(), newbuf->size(), 0);
    io_uring_sqe_set_data(sqe, ev);
    c->upcoming_recv_buffs.add(ev->hold);
    return true;
  }
  return false;
}

void Worker::handle_recv(io_uring_cqe* cqe, EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }

#ifdef DEBUG
  ts_std::cout << "Upcoming buffs num: " << c->upcoming_recv_buffs.size() << std::endl;
  c->upcoming_recv_buffs.print();
#endif

  if (cqe->res <= 0)
  {
    c->ms_recv_armed = false;

    if (cqe->res == -EINVAL || cqe->res == -EOPNOTSUPP)
    {
      use_ms_recv_ = false;
      if (!c->closing && post_recv(c))
      {
        need_submit_++;
      }
      return;
    }

    c->closing = true;
    if (cqe->res == -EBADF)
    {
#ifdef DEBUG
      std::cerr << "Workflow::Recv encountered EBADF: fd=" << data->fd << ", removing from table." << std::endl;
#endif
      table_.erase(data->fd);
      return;
    }

#ifdef DEBUG
    ts_std::cout << "Workflow::Recv completion: fd=" << data->fd << ", res=" << cqe->res << ", aka closing"
                 << std::endl;
#endif // DEBUG

    if (post_close(data->fd))
    {
      need_submit_++;
    }
    return;
  }

#ifdef DEBUG
  ts_std::cout << "Workflow::Recv completion: fd=" << data->fd << ", res=" << cqe->res
               << ", data ptr=" << data->hold.get() << " " << data->hold.use_count() << std::endl;
#endif // DEBUG

  // singleshot fallback path
  if (data->hold)
  {
    TSSet<std::vector<char>>::element hold = std::static_pointer_cast<std::vector<char>>(data->hold);
    hold->resize(cqe->res);
#ifdef DEBUG
    ts_std::cout << "Data received at: " << hold.get() << " " << hold.use_count();
    ts_std::cout << "\nData received:\n" << std::string_view(hold->data(), hold->size()) << std::endl;
    ts_std::cout << "upcoming_recv_buffs:\n";
    c->upcoming_recv_buffs.print();
#endif // DEBUG
    c->completed_recv_buffs.push(hold);
#ifdef DEBUG
    ts_std::cout << "Add to the list of filled buffers" << std::endl;
#endif // DEBUG
    c->upcoming_recv_buffs.remove(data->hold);
#ifdef DEBUG
    ts_std::cout << "Remove from the list of awaiting buffers" << std::endl;
#endif // DEBUG
    if (post_parse(c))
    {
      need_submit_++;
    }
    if (post_recv(c))
    {
      need_submit_++;
    }
  }
}

bool Worker::post_parse(PerClientStorage* c)
{
  EventData* ev = new EventData;
  ev->fd = c->fd;
  ev->op = Workflow::Parse;
  ev->hold = nullptr;

  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, ev);
    return true;
  }
  return false;
}

void Worker::handle_parse(EventData* data)
{
#ifdef DEBUG
  ts_std::cout << "Parsing data for fd=" << data->fd << std::endl;
#endif // DEBUG
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }
  c->parse_step();
}

bool Worker::post_find_handler(PerClientStorage* c)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    EventData* ev = new EventData;
    ev->fd = c->fd;
    ev->op = Workflow::FindHandler;
    ev->hold = nullptr;
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, ev);
    return true;
  }
  return false;
}

void Worker::handle_find_handler(EventData* data)
{
#ifdef DEBUG
  ts_std::cout << "Finding handler for fd=" << data->fd << std::endl;
#endif // DEBUG
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }
  c->find_handler();
}

bool Worker::post_generate_response(PerClientStorage* c)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    EventData* ev = new EventData;
    ev->fd = c->fd;
    ev->op = Workflow::GenerateResponse;
    ev->hold = nullptr;
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, ev);
    return true;
  }
  return false;
}

void Worker::handle_generate_response(EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }
  c->generate_response();
}

bool Worker::post_request_flush(PerClientStorage* c)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    EventData* ev = new EventData;
    ev->fd = c->fd;
    ev->op = Workflow::RequestFlush;
    ev->hold = nullptr;
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, ev);
    return true;
  }
  return false;
}

void Worker::handle_request_flush(EventData* data)
{
  auto* c = table_.get(data->fd);
  if (!c)
  {
    return;
  }
  // A send is about to be completed and it would fire the next batch of data to send
  if (c->send_inflight)
  {
    return;
  }

  post_send(c);
}

void Worker::post_send(PerClientStorage* c)
{
  c->requests_ready.lock();
  const auto& reqs = c->requests_ready.unsafe_get_set();
  for (const auto& req : reqs)
  {
    io_uring_sqe* sqe;
    do
    {
      sqe = get_sqe_or_submit();
    } while (!sqe);
    {
      EventData* ev = new EventData;
      ev->fd = c->fd;
      ev->op = Workflow::Send;
      ev->hold = req;
      io_uring_prep_sendmsg(sqe, c->fd, &req->batched_send_data.msg, MSG_NOSIGNAL);
      io_uring_sqe_set_data(sqe, ev);
      need_submit_++;
      c->requests_in_flight.add(req);
    }
  }
  c->requests_ready.unlock();
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
      TSSet<RequestProcessing>::element req = std::static_pointer_cast<RequestProcessing>(data->hold);
      c->requests_in_flight.remove(req);
      c->requests_ready.add(req);
      post_send(c);
      return;
    }
    c->closing = true;
    if (post_close(data->fd))
    {
      need_submit_++;
    }
    return;
  }

  TSSet<RequestProcessing>::element req = std::static_pointer_cast<RequestProcessing>(data->hold);

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
      if (auto* sqe = get_sqe_or_submit())
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
  c->requests_in_flight.remove(req);
  c->send_inflight = false;

  if (c->on_send_completed())
  {
    c->closing = true;
    if (post_close(data->fd))
    {
      need_submit_++;
    }
  }
  else
  {
    post_send(c); // continue draining
  }
}

bool Worker::post_close(fd_t fd)
{
  auto* c = table_.get(fd);
  if (c)
  {
    c->closing = true;

    if (auto* sqe = get_sqe_or_submit())
    {
      EventData* ev = new EventData;
      ev->fd = fd;
      ev->op = Workflow::Closed;
      ev->hold = nullptr;
      io_uring_prep_close(sqe, fd);
      io_uring_sqe_set_data(sqe, ev);
      return true;
    }
  }
  return false;
}

void Worker::handle_close(io_uring_cqe*, EventData* data)
{
#ifdef DEBUG
  ts_std::cout << "Workflow::Close/Closed completion for fd=" << data->fd << ", res=" << cqe->res << std::endl;
#endif
  table_.erase(data->fd);
}

void Worker::run()
{
#ifdef DEBUG
  ts_std::cout << "worker thread started, ring_fd=" << ring_fd_ << ", listen_fd=" << listen_fd_ << std::endl;
#endif
  for (int i = 0; i < cfg_.accepts_per_worker; ++i)
  {
    if (post_accept())
    {
      ++need_submit_;
    }
  }
  if (need_submit_)
  {
    io_uring_submit(&ring_);
    need_submit_ = 0;
  }

  io_uring_cqe* cqes[512];

  while (true)
  {
    io_uring_submit_and_wait(&ring_, 1);

    const unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, sizeof(cqes) / sizeof(cqes[0]));

    for (unsigned i = 0; i < n; ++i)
    {
      io_uring_cqe* cqe = cqes[i];
      EventData* data = reinterpret_cast<EventData*>(io_uring_cqe_get_data(cqe));
      io_uring_cqe_seen(&ring_, cqe);

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
        handle_parse(data);
        break;

      case Workflow::FindHandler:
        handle_find_handler(data);
        break;

      case Workflow::RequestFlush:
        handle_request_flush(data);
        break;

      case Workflow::GenerateResponse:
        handle_generate_response(data);
        break;

      case Workflow::RequestClose:
        handle_close(cqe, data);
        break;

      case Workflow::Closed:
        handle_close(cqe, data);
        break;
      } // switch
      delete data;
    }

    if (need_submit_)
    {
      io_uring_submit(&ring_);
      need_submit_ = 0;
    }
  }
}

#endif
