#include <cstring>
#include <iostream>
#include <sys/poll.h>
#include <thread>
#ifdef __linux__
#include "common/socket.hpp"
#include <liburing/io_uring.h>
#include <sys/types.h>
#include "common/workflow.hpp"
#include "server/worker.hpp"

static inline uint64_t pack_ud(int fd, Workflow op)
{
  return (uint64_t(uint32_t(fd)) << 35) | (uint64_t(0) << 3) | uint64_t(op);
}

static inline uint64_t pack_ud_aux(int fd, Workflow op, uint32_t aux)
{
  return (uint64_t(uint32_t(fd)) << 35) | (uint64_t(aux & 0x1FFFFFFF) << 3) | uint64_t(op);
}

static inline void unpack_ud(uint64_t u, int& fd, Workflow& op, uint32_t& aux)
{
  op = Workflow(u & 0x7ull);
  aux = uint32_t((u >> 3) & 0x1FFFFFFF);
  fd = int((u >> 35) & 0xFFFFFFFFu);
}

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
  p.flags |= IORING_SETUP_COOP_TASKRUN;
  p.flags |= IORING_SETUP_TASKRUN_FLAG;
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

  init_buffer_pool();
}

inline io_uring_sqe* Worker::get_sqe_or_submit()
{
  return ::get_sqe_or_submit(ring_);
}

void Worker::init_buffer_pool()
{
  buf_pool_.reset(new (std::nothrow) char[BUF_SZ * BUF_CNT]);
  if (!buf_pool_)
  {
    have_buf_pool_ = false;
    return;
  }

  int posted = 0;
  for (int i = 0; i < BUF_CNT; ++i)
  {
    auto* sqe = io_uring_get_sqe(&ring_);
    if (!sqe)
    {
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe)
      {
        break;
      }
    }
    io_uring_prep_provide_buffers(sqe, buf_pool_.get() + i * BUF_SZ, BUF_SZ, 1, BUF_GRP, i);
    io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Workflow::FreeBuffer, i));
    ++posted;
  }
  if (posted)
  {
    io_uring_submit(&ring_);
  }
  have_buf_pool_ = (posted > 0);
}

uint16_t Worker::post_initial_accepts()
{
  uint16_t posted = 0;
  if (use_ms_accept_)
  {
    if (post_accept())
    {
      ++posted;
    }
  }
  else
  {
    for (int i = 0; i < cfg_.accepts_per_worker; ++i)
    {
      if (post_accept())
      {
        ++posted;
      }
    }
  }
  return posted;
}

bool Worker::post_accept()
{
  if (use_ms_accept_)
  {
    if (ms_accept_armed_)
    {
      return false;
    }
    if (auto* sqe = get_sqe_or_submit())
    {
      io_uring_prep_multishot_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
      io_uring_sqe_set_data64(sqe, pack_ud(listen_fd_, Workflow::Accept));
      // std::cout << std::this_thread::get_id() << ": posted multishot accept for listen_fd=" << listen_fd_ <<
      // std::endl;
      ms_accept_armed_ = true;
      return true;
    }
    return false;
  }
  else
  {
    if (auto* sqe = get_sqe_or_submit())
    {
      io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
      io_uring_sqe_set_data64(sqe, pack_ud(listen_fd_, Workflow::Accept));
      return true;
    }
    return false;
  }
}

void Worker::post_task(int fd, Workflow kind)
{
  auto* sqe = get_sqe_or_submit();
  if (!sqe)
  {
    // If ring is momentarily full, fall back to inline for RequestFlush; others can be retried quickly.
    if (kind == Workflow::RequestFlush)
    {
      if (auto* c = table_.get(fd))
      {
        sendkick_inline(c);
      }
    }
    return;
  }
  // Pass our standard user_data through MSG_RING, it will surface in CQ as-is
  const uint64_t tag = pack_ud(fd, kind);
  // signature: (sqe, target_ring_fd, len, data, flags)
  io_uring_prep_msg_ring(sqe, ring_fd_, 0, tag, 0);
  need_submit_++;
}

bool Worker::start_recv(PerClientStorage* c)
{
  // std::cout << std::this_thread::get_id() << ": starting recv for fd=" << c->fd << std::endl;
  if (use_ms_recv_ && have_buf_pool_)
  {
    if (c->ms_recv_armed)
    {
      return false;
    }
    if (auto* sqe = get_sqe_or_submit())
    {
      io_uring_prep_recv_multishot(sqe, c->fd, nullptr, 0, 0);
      sqe->buf_group = BUF_GRP;
      io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
      io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Workflow::Recv));
      c->ms_recv_armed = true;
      return true;
    }
    return false;
  }
  // std::cout << std::this_thread::get_id() << ": starting singleshot recv for fd=" << c->fd << std::endl;
  // singleshot fallback: allocate hold and keep it alive on the PCS
  c->rx_hold = std::make_shared<std::vector<char>>(RX_CAP);
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_recv(sqe, c->fd, c->rx_hold->data(), c->rx_hold->size(), 0);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Workflow::Recv));
    return true;
  }
  c->rx_hold.reset();
  return false;
}

bool Worker::close_async(fd_t fd)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data64(sqe, pack_ud(fd, Workflow::Closed));
    return true;
  }
  return false;
}

bool Worker::update_pollout(PerClientStorage* c)
{
  if (c->pollout_armed)
  {
    return false;
  }
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_poll_add(sqe, c->fd, POLLOUT);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Workflow::PollOut));
    c->pollout_armed = true;
    return true;
  }
  return false;
}

void Worker::sendkick_inline(PerClientStorage* c)
{
  if (c->send_inflight || c->txq.empty())
  {
    return;
  }

  size_t cnt = 0;
  bool eor = false;
  for (auto it = c->txq.begin(); it != c->txq.end() && cnt < MAX_IOV; ++it, ++cnt)
  {
    c->batched_send_data.iov[cnt].iov_base = const_cast<char*>(it->wb.buf);
    c->batched_send_data.iov[cnt].iov_len = it->wb.len;
    if (it->eor)
    {
      eor = true;
      ++cnt;
      break;
    }
  }
  if (cnt == 0)
  {
    return;
  }

  c->batched_send_data.msg = {};
  c->batched_send_data.msg.msg_iov = c->batched_send_data.iov;
  c->batched_send_data.msg.msg_iovlen = cnt;

  c->tx_inflight.clear();
  for (size_t i = 0; i < cnt && !c->txq.empty(); ++i)
  {
    c->tx_inflight.emplace_back(std::move(c->txq.front()));
    c->txq.pop_front();
  }

  c->send_inflight = true;
  c->inflight_eor = eor;

  auto* sqe = get_sqe_or_submit();
  if (!sqe)
  {
    // rollback into queue and re-post a RequestFlush task so we don't lose progress
    for (size_t i = 0; i < c->tx_inflight.size(); ++i)
    {
      c->txq.emplace_front(std::move(c->tx_inflight[c->tx_inflight.size() - 1 - i]));
    }
    c->tx_inflight.clear();
    c->send_inflight = false;
    c->inflight_eor = false;
    post_task(c->fd, Workflow::RequestFlush);
    return;
  }
  io_uring_prep_sendmsg(sqe, c->fd, &c->batched_send_data.msg, MSG_NOSIGNAL);
  io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Workflow::Send));
  need_submit_++;
}

void Worker::run()
{
  // std::cout << std::this_thread::get_id() << ": worker thread started, ring_fd=" << ring_fd_
  //           << ", listen_fd=" << listen_fd_ << std::endl;
  need_submit_ += post_initial_accepts();
  if (need_submit_)
  {
    io_uring_submit(&ring_);
    need_submit_ = 0;
  }

  io_uring_cqe* cqes[512];

  while (true)
  {
    io_uring_submit_and_wait(&ring_, 1);

    const unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, 512);

    for (unsigned i = 0; i < n; ++i)
    {
      io_uring_cqe* cqe = cqes[i];
      fd_t fd;
      Workflow op;
      uint32_t aux;
      unpack_ud((uint64_t) io_uring_cqe_get_data64(cqe), fd, op, aux);
      int res = cqe->res;
      unsigned fl = cqe->flags;
      io_uring_cqe_seen(&ring_, cqe);

      switch (op)
      {
      case Workflow::Accept: {
        if (res <= 0)
        {
          if (res == 0)
          {
            // std::cerr << "Accept returned 0, which is not a valid file descriptor." << std::endl;
          }
          if (res == -EINVAL || res == -EOPNOTSUPP)
          {
            use_ms_accept_ = false;
            ms_accept_armed_ = false;
          }
          if (post_accept())
          {
            need_submit_++;
          }
          break;
        }

        int cfd = res;
        set_nonblock(cfd);
        set_tcp_opts(cfd);

        auto* c = table_.add(cfd);
        c->keepalive_limit = cfg_.max_keepalive_requests;
        c->owner = this;

        // std::cout << std::this_thread::get_id() << ": accepted fd=" << cfd << " on listen_fd=" << listen_fd_
        //           << std::endl;

        if (start_recv(c))
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
        else if ((fl & IORING_CQE_F_MORE) == 0)
        {
          ms_accept_armed_ = false;
          if (post_accept())
          {
            need_submit_++;
          }
        }
        break;
      }

      case Workflow::Recv: {
        auto* c = table_.get(fd);
        if (!c)
        {
          break;
        }

        if (res <= 0)
        {
          c->ms_recv_armed = false;

          if (res == -EINVAL || res == -EOPNOTSUPP)
          {
            use_ms_recv_ = false;
            if (!c->closing && start_recv(c))
            {
              need_submit_++;
            }
            break;
          }

          c->closing = true;
          if (close_async(fd))
          {
            need_submit_++;
          }
          break;
        }

        if (use_ms_recv_ && have_buf_pool_ && (fl & IORING_CQE_F_BUFFER))
        {
          int bid = (fl >> IORING_CQE_BUFFER_SHIFT);
          char* p = buf_pool_.get() + bid * BUF_SZ;
          size_t nbytes = (size_t) res;
          // std::cout << std::this_thread::get_id() << ": recv " << nbytes << " bytes on fd=" << fd << ", bid=" << bid
          //           << std::endl;

          auto vec = std::make_shared<std::vector<char>>(nbytes);
          std::memcpy(vec->data(), p, nbytes);
          c->rxq.emplace_back(OwnedBuf{StringBuf{nbytes, vec->data()}, false, vec});

          // Return buffer to pool
          if (auto* sqe = get_sqe_or_submit())
          {
            io_uring_prep_provide_buffers(sqe, p, BUF_SZ, 1, BUF_GRP, bid);
            io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Workflow::FreeBuffer, bid));
            need_submit_++;
          }

          // parse now
          c->parse_step();

          // Hybrid: if we now have data to send, kick inline
          if (!c->send_inflight && !c->txq.empty())
          {
            sendkick_inline(c);
          }

          if ((fl & IORING_CQE_F_MORE) == 0)
          {
            c->ms_recv_armed = false;
            if (!c->closing && start_recv(c))
            {
              need_submit_++;
            }
          }
        }
        else
        {
          // singleshot fallback path
          if (c->rx_hold)
          {
            auto hold = c->rx_hold;
            c->rx_hold.reset();
            // std::cout << std::this_thread::get_id() << ": recv " << res << " bytes on fd=" << fd << std::endl;
            c->rxq.emplace_back(OwnedBuf{StringBuf{(size_t) res, hold->data()}, false, hold});
            c->parse_step();
            if (!c->send_inflight && !c->txq.empty())
            {
              sendkick_inline(c);
            }
            if (!c->closing && start_recv(c))
            {
              need_submit_++;
            }
          }
        }
        break;
      }

      case Workflow::Send: {
        auto* c = table_.get(fd);
        if (!c)
        {
          break;
        }

        if (res < 0)
        {
          if (res == -EAGAIN || res == -EWOULDBLOCK)
          {
            for (size_t i = 0; i < c->tx_inflight.size(); ++i)
            {
              c->txq.emplace_front(std::move(c->tx_inflight[c->tx_inflight.size() - 1 - i]));
            }
            c->tx_inflight.clear();
            c->send_inflight = false;
            c->inflight_eor = false;
            if (update_pollout(c))
            {
              need_submit_++;
            }
            break;
          }
          c->closing = true;
          if (close_async(fd))
          {
            need_submit_++;
          }
          break;
        }

        if (c->on_send_completed())
        {
          c->closing = true;
          if (close_async(fd))
          {
            need_submit_++;
          }
        }
        else
        {
          // Hybrid: inline next chunk
          sendkick_inline(c);
        }
        break;
      }

      case Workflow::PollOut: {
        auto* c = table_.get(fd);
        if (!c)
        {
          break;
        }
        c->pollout_armed = false;
        sendkick_inline(c);
        break;
      }

      case Workflow::Closed: {
        table_.erase(fd);
        break;
      }

      case Workflow::Parse: {
        if (auto* c = table_.get(fd))
        {
          c->parse_step();
        }
        break;
      }

      case Workflow::FindHandler: {
        if (auto* c = table_.get(fd))
        {
          // std::cout << std::this_thread::get_id() << ": finding handler for fd=" << fd << std::endl;
          c->find_handler();
        }
        break;
      }

      case Workflow::RequestFlush: {
        if (auto* c = table_.get(fd))
        {
          c->sendkick_pending.clear(std::memory_order_release);
          sendkick_inline(c);
        }
        break;
      }

      case Workflow::GenerateResponse: {
        if (auto* c = table_.get(fd))
        {
          c->generate_response();
        }
        break;
      }

      case Workflow::RequestClose: {
        if (auto* c = table_.get(fd))
        {
          c->closing = true;
          if (close_async(fd))
          {
            need_submit_++;
          }
        }
        break;
      }

      case Workflow::FreeBuffer: {
        // buffer returned to pool
        break;
      }
      } // switch
    } // batch

    if (need_submit_)
    {
      io_uring_submit(&ring_);
      need_submit_ = 0;
    }
  }
}

#endif
