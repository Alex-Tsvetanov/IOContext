// SPDX-License-Identifier: MIT
// Build: g++ -O2 -std=gnu++20 -pthread linux/http11_uring_server_one_ring_fixed.cpp -luring -o http_uring_one_ring
// Run  : ./http_uring_one_ring [port] [threads]

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <liburing.h>
#include <strings.h> // strncasecmp

// ----- your common headers -----
#include "common/string_buffer.hpp" // struct StringBuf { size_t len; const char* buf; };
#include "common/owned_buf.hpp"     // struct OwnedBuf { StringBuf wb; bool eor; std::shared_ptr<void> guard; ... }

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_IOV = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int BACKLOG = 16384;

static constexpr int PROC_MAX_SEGMENTS = 32;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(200);

// Buffer pool (provided buffers)
static constexpr int BUF_SZ = RX_CAP;
static constexpr int BUF_CNT = 8192; // ~32MB total pool
static constexpr int BUF_GRP = 11;

// Demo payload
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

// ===================== OwnedBuf inline impl ==========================
#ifndef OWNED_BUF_INLINE_IMPL
#  define OWNED_BUF_INLINE_IMPL
inline OwnedBuf OwnedBuf::literal(const char* p, size_t n, bool eor)
{
  return OwnedBuf{StringBuf{n, p}, eor, {}};
}
inline OwnedBuf OwnedBuf::copy(std::string_view sv, bool eor)
{
  auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
  return OwnedBuf{StringBuf{buf->size(), buf->data()}, eor, buf};
}
#endif

// ===================== CQE user_data packing =========================
// [ fd:32 ][ aux:29 ][ op:3 ]
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  PollOut,
  Close,
  Buf
};

static inline uint64_t pack_ud(int fd, Op op)
{
  return (uint64_t(uint32_t(fd)) << 35) | uint64_t(op);
}
static inline uint64_t pack_ud_aux(int fd, Op op, uint32_t aux)
{
  return (uint64_t(uint32_t(fd)) << 35) | (uint64_t(aux & 0x1FFFFFFF) << 3) | uint64_t(op);
}
static inline void unpack_ud(uint64_t u, int& fd, Op& op, uint32_t& aux)
{
  op = Op(u & 0x7ull);
  aux = uint32_t((u >> 3) & 0x1FFFFFFF);
  fd = int((u >> 35) & 0xFFFFFFFFu);
}

// ===================== Helpers ======================================
static int set_nonblock(int fd)
{
  int f = fcntl(fd, F_GETFL, 0);
  if (f < 0)
  {
    return -1;
  }
  return fcntl(fd, F_SETFL, f | O_NONBLOCK);
}
static void set_tcp_opts(int fd)
{
  int nd = 1;
  (void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
#ifdef TCP_QUICKACK
  int qa = 1;
  (void) setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &qa, sizeof(qa));
#endif
  int sndbuf = 256 * 1024;
  (void) setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
}
static int make_listener(uint16_t port)
{
  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0)
  {
    std::perror("socket");
    std::exit(1);
  }
  int on = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = INADDR_ANY;
  a.sin_port = htons(port);
  if (bind(s, (sockaddr*) &a, sizeof(a)) < 0)
  {
    std::perror("bind");
    std::exit(1);
  }
  if (listen(s, BACKLOG) < 0)
  {
    std::perror("listen");
    std::exit(1);
  }
  set_nonblock(s);
  return s;
}

// ===================== Global ring + listener ========================
static io_uring g_ring{};
static int g_listen_fd = -1;
static int g_ring_fd = -1;

static std::unique_ptr<char[]> g_buf_pool;
static bool g_have_pool = false;

static std::atomic<bool> g_ms_accept_armed{false};
static std::atomic<bool> g_use_ms_accept{true};
static std::atomic<bool> g_use_ms_recv{true};

// ---- SQE helper (shared ring) ----
static inline io_uring_sqe* get_sqe_or_submit()
{
  if (auto* sqe = io_uring_get_sqe(&g_ring))
  {
    return sqe;
  }
  io_uring_submit(&g_ring);
  if (auto* sqe2 = io_uring_get_sqe(&g_ring))
  {
    return sqe2;
  }
  io_uring_submit_and_wait(&g_ring, 1);
  return io_uring_get_sqe(&g_ring);
}

// ===================== Per-client state ==============================
struct PerClientStorage: public std::enable_shared_from_this<PerClientStorage>
{
  int fd{-1};
  uint32_t served{0};
  bool closing{false};
  bool keep_alive{true};
  size_t body_need{0};

  // serialize state across threads
  std::mutex mtx;

  // RX
  std::string acc;
  std::shared_ptr<std::vector<char>> rx_hold; // singleshot fallback buffer

  // TX
  std::deque<OwnedBuf> txq;
  std::deque<OwnedBuf> tx_inflight;
  bool send_inflight{false};
  bool inflight_eor{false};
  struct iovec iov[MAX_IOV];
  struct msghdr msg{};

  // poll/recv state
  bool ms_recv_armed{false};
  bool pollout_armed{false};

  // policy
  uint16_t keepalive_limit{65000};

  // ---- parse (budgeted) ----
  void parse_step_locked()
  {
    auto t0 = std::chrono::steady_clock::now();
    int processed = 0;

    for (;;)
    {
      for (;;)
      {
        size_t hdr_end = acc.find("\r\n\r\n");
        if (hdr_end == std::string::npos)
        {
          break;
        }

        bool keep = true;
        size_t clen = 0;
        size_t start = 0;
        while (start < hdr_end)
        {
          auto end = acc.find("\r\n", start);
          if (end == std::string::npos || end > hdr_end)
          {
            break;
          }
          auto colon = acc.find(':', start);
          if (colon != std::string::npos && colon < end)
          {
            auto k = acc.substr(start, colon - start);
            size_t vbeg = colon + 1;
            while (vbeg < end && (acc[vbeg] == ' ' || acc[vbeg] == '\t'))
            {
              ++vbeg;
            }
            auto v = acc.substr(vbeg, end - vbeg);

            if (k.size() == 10 && ::strncasecmp(k.c_str(), "Connection", 10) == 0)
            {
              std::string vl = v;
              std::transform(vl.begin(), vl.end(), vl.begin(), ::tolower);
              keep = (vl.find("close") == std::string::npos);
            }
            else if (k.size() == 14 && ::strncasecmp(k.c_str(), "Content-Length", 14) == 0)
            {
              clen = (size_t) std::strtoull(v.c_str(), nullptr, 10);
            }
          }
          start = end + 2;
        }

        size_t body_off = hdr_end + 4;
        if (acc.size() < body_off + clen)
        {
          keep_alive = keep;
          body_need = (body_off + clen) - acc.size();
          break;
        }

        // consume one full request
        acc.erase(0, body_off + clen);
        keep_alive = keep;
        body_need = 0;

        bool keepconn = keep_alive && (served < keepalive_limit - 1);
        const char* hdr = keepconn ? kHdrKeep : kHdrClose;
        txq.emplace_back(OwnedBuf::literal(hdr, std::strlen(hdr), false));
        txq.emplace_back(OwnedBuf::literal(kBody, sizeof(kBody) - 1, true));
      }

      if (++processed >= PROC_MAX_SEGMENTS)
      {
        break;
      }
      if (std::chrono::steady_clock::now() - t0 >= PROC_TIME_BUDGET)
      {
        break;
      }

      break;
    }
  }
};

// ===================== Connection table (global, shared_ptr) =========
struct ConnTable
{
  std::mutex mtx;
  std::unordered_map<int, std::shared_ptr<PerClientStorage>> map;

  std::shared_ptr<PerClientStorage> get(int fd)
  {
    std::lock_guard lk(mtx);
    auto it = map.find(fd);
    return (it == map.end()) ? nullptr : it->second;
  }
  std::shared_ptr<PerClientStorage> add(int fd)
  {
    auto p = std::make_shared<PerClientStorage>();
    p->fd = fd;
    std::lock_guard lk(mtx);
    map.emplace(fd, p);
    return p;
  }
  void erase(int fd)
  {
    std::lock_guard lk(mtx);
    map.erase(fd);
  }
};
static ConnTable g_table;

// ===================== Buffer pool init ==============================
static void init_buffer_pool()
{
  g_buf_pool.reset(new (std::nothrow) char[BUF_SZ * BUF_CNT]);
  if (!g_buf_pool)
  {
    g_have_pool = false;
    return;
  }

  int posted = 0;
  for (int i = 0; i < BUF_CNT; ++i)
  {
    auto* sqe = io_uring_get_sqe(&g_ring);
    if (!sqe)
    {
      io_uring_submit(&g_ring);
      sqe = io_uring_get_sqe(&g_ring);
      if (!sqe)
      {
        break;
      }
    }
    io_uring_prep_provide_buffers(sqe, g_buf_pool.get() + i * BUF_SZ, BUF_SZ, 1, BUF_GRP, i);
    io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Op::Buf, i));
    ++posted;
  }
  if (posted)
  {
    io_uring_submit(&g_ring);
  }
  g_have_pool = (posted > 0);
}

// ===================== I/O posting (shared ring) =====================
static bool post_accept()
{
  if (!g_use_ms_accept.load(std::memory_order_acquire))
  {
    if (auto* sqe = get_sqe_or_submit())
    {
      io_uring_prep_accept(sqe, g_listen_fd, nullptr, nullptr, SOCK_NONBLOCK);
      io_uring_sqe_set_data64(sqe, pack_ud(g_listen_fd, Op::Accept));
      io_uring_submit(&g_ring);
      return true;
    }
    return false;
  }

  // multishot accept (only one armed)
  bool expected = false;
  if (!g_ms_accept_armed.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
  {
    return false;
  }
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_multishot_accept(sqe, g_listen_fd, nullptr, nullptr, SOCK_NONBLOCK);
    io_uring_sqe_set_data64(sqe, pack_ud(g_listen_fd, Op::Accept));
    io_uring_submit(&g_ring);
    return true;
  }
  g_ms_accept_armed.store(false, std::memory_order_release);
  return false;
}

static bool start_recv(std::shared_ptr<PerClientStorage>& c)
{
  if (g_use_ms_recv.load(std::memory_order_acquire) && g_have_pool)
  {
    if (c->ms_recv_armed)
    {
      return false;
    }
    auto* sqe = get_sqe_or_submit();
    if (!sqe)
    {
      return false;
    }
    io_uring_prep_recv_multishot(sqe, c->fd, nullptr, 0, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
    sqe->buf_group = BUF_GRP;
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Recv));
    c->ms_recv_armed = true;
    io_uring_submit(&g_ring);
    return true;
  }
  else
  {
    // Proper singleshot fallback: allocate buffer and arm recv into it
    c->rx_hold = std::make_shared<std::vector<char>>(RX_CAP);
    auto* sqe = get_sqe_or_submit();
    if (!sqe)
    {
      return false;
    }
    io_uring_prep_recv(sqe, c->fd, c->rx_hold->data(), c->rx_hold->size(), 0);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Recv));
    io_uring_submit(&g_ring);
    return true;
  }
}
// Force singleshot recv regardless of global multishot setting
// (used to recover from temporary -ENOBUFS in the provided-buffer group).
static bool start_recv_singleshot_force(std::shared_ptr<PerClientStorage>& c)
{
  c->rx_hold = std::make_shared<std::vector<char>>(RX_CAP);
  auto* sqe = get_sqe_or_submit();
  if (!sqe)
  {
    return false;
  }
  io_uring_prep_recv(sqe, c->fd, c->rx_hold->data(), c->rx_hold->size(), 0);
  io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Recv));
  io_uring_submit(&g_ring);
  return true;
}

static bool close_async(int fd)
{
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data64(sqe, pack_ud(fd, Op::Close));
    return true;
  }
  return false;
}

static bool arm_pollout(std::shared_ptr<PerClientStorage>& c)
{
  if (c->pollout_armed)
  {
    return false;
  }
  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_poll_add(sqe, c->fd, POLLOUT);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::PollOut));
    c->pollout_armed = true;
    return true;
  }
  return false;
}

static void sendkick_inline(std::shared_ptr<PerClientStorage>& c, int& local_submit)
{
  if (c->send_inflight || c->txq.empty())
  {
    return;
  }

  size_t cnt = 0;
  bool eor = false;
  for (auto it = c->txq.begin(); it != c->txq.end() && cnt < MAX_IOV; ++it, ++cnt)
  {
    c->iov[cnt].iov_base = const_cast<char*>(it->wb.buf);
    c->iov[cnt].iov_len = it->wb.len;
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

  c->msg = {};
  c->msg.msg_iov = c->iov;
  c->msg.msg_iovlen = cnt;

  c->tx_inflight.clear();
  for (size_t i = 0; i < cnt && !c->txq.empty(); ++i)
  {
    c->tx_inflight.emplace_back(std::move(c->txq.front()));
    c->txq.pop_front();
  }

  c->send_inflight = true;
  c->inflight_eor = eor;

  if (auto* sqe = get_sqe_or_submit())
  {
    io_uring_prep_sendmsg(sqe, c->fd, &c->msg, MSG_NOSIGNAL);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Send));
    ++local_submit;
  }
  else
  {
    // rollback into queue
    for (size_t i = 0; i < c->tx_inflight.size(); ++i)
    {
      c->txq.emplace_front(std::move(c->tx_inflight[c->tx_inflight.size() - 1 - i]));
    }
    c->tx_inflight.clear();
    c->send_inflight = false;
    c->inflight_eor = false;
  }
}

// ===================== Worker thread (shared ring) ===================
static void worker_loop()
{
  while (true)
  {
    io_uring_cqe* cqes[256];
    unsigned n = io_uring_peek_batch_cqe(&g_ring, cqes, 256);
    if (n == 0)
    {
      io_uring_submit_and_wait(&g_ring, 1);
      continue;
    }

    int local_submit = 0;

    for (unsigned i = 0; i < n; ++i)
    {
      io_uring_cqe* cqe = cqes[i];
      int fd;
      Op op;
      uint32_t aux;
      unpack_ud((uint64_t) io_uring_cqe_get_data64(cqe), fd, op, aux);
      int res = cqe->res;
      unsigned fl = cqe->flags;
      io_uring_cqe_seen(&g_ring, cqe);

      switch (op)
      {
      case Op::Accept: {
        if (res < 0)
        {
          if (res == -EINVAL || res == -EOPNOTSUPP)
          {
            g_use_ms_accept.store(false, std::memory_order_release);
          }
          g_ms_accept_armed.store(false, std::memory_order_release);
          if (post_accept())
          {
            ++local_submit;
          }
          break;
        }

        int cfd = res;
        set_nonblock(cfd);
        set_tcp_opts(cfd);
        auto c = g_table.add(cfd);
        c->keepalive_limit = 65000;

        {
          std::scoped_lock lk(c->mtx);
          if (start_recv(c))
          {
            ++local_submit;
          }
        }

        if ((fl & IORING_CQE_F_MORE) == 0)
        {
          g_ms_accept_armed.store(false, std::memory_order_release);
          if (post_accept())
          {
            ++local_submit;
          }
        }
        break;
      }

      case Op::Recv: {
        auto c = g_table.get(fd);
        if (!c)
        {
          break;
        }

        std::unique_lock lk(c->mtx);

        if (res <= 0)
        {
          c->ms_recv_armed = false;

          if (res == -EINVAL || res == -EOPNOTSUPP)
          {
            g_use_ms_recv.store(false, std::memory_order_release);
            // switch to singleshot on next start_recv()
            if (start_recv(c))
            {
              ++local_submit;
            }
            break;
          }
          // Provided-buffer group temporarily empty — not fatal. Fall back to one-shot recv.
          if (res == -ENOBUFS)
          {
            // Keep connection open; arm singleshot immediately to keep data flowing.
            if (start_recv_singleshot_force(c))
            {
              ++local_submit;
            }
            break;
          }
          c->closing = true;
          lk.unlock();
          (void) close_async(fd);
          ++local_submit;
          break;
        }

        if (g_have_pool && (fl & IORING_CQE_F_BUFFER))
        {
          int bid = (fl >> IORING_CQE_BUFFER_SHIFT);
          char* p = g_buf_pool.get() + bid * BUF_SZ;
          size_t nbytes = (size_t) res;

          c->acc.append(p, nbytes);

          // Return buffer to pool
          if (auto* sqe = get_sqe_or_submit())
          {
            io_uring_prep_provide_buffers(sqe, p, BUF_SZ, 1, BUF_GRP, bid);
            io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Op::Buf, bid));
            ++local_submit;
          }

          c->parse_step_locked();

          if (!c->send_inflight && !c->txq.empty())
          {
            sendkick_inline(c, local_submit);
          }

          if ((fl & IORING_CQE_F_MORE) == 0)
          {
            c->ms_recv_armed = false;
            if (start_recv(c))
            {
              ++local_submit;
            }
          }
        }
        else
        {
          // singleshot fallback completion: rx_hold contains buffer
          if (c->rx_hold)
          {
            c->acc.append(c->rx_hold->data(), (size_t) res);
            c->rx_hold.reset();

            c->parse_step_locked();
            if (!c->send_inflight && !c->txq.empty())
            {
              sendkick_inline(c, local_submit);
            }

            if (start_recv(c))
            {
              ++local_submit; // post next singleshot
            }
          }
        }

        break;
      }

      case Op::Send: {
        auto c = g_table.get(fd);
        if (!c)
        {
          break;
        }
        std::unique_lock lk(c->mtx);

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
            if (arm_pollout(c))
            {
              ++local_submit;
            }
            break;
          }
          c->closing = true;
          lk.unlock();
          (void) close_async(fd);
          ++local_submit;
          break;
        }

        // normal completion
        {
          c->tx_inflight.clear();
          bool finished = c->inflight_eor;
          c->send_inflight = false;
          c->inflight_eor = false;

          if (finished)
          {
            c->served++;
            if (c->served >= c->keepalive_limit || !c->keep_alive || c->closing)
            {
              c->closing = true;
              lk.unlock();
              (void) close_async(fd);
              ++local_submit;
              break;
            }
          }

          if (!c->txq.empty())
          {
            sendkick_inline(c, local_submit);
          }
        }
        break;
      }

      case Op::PollOut: {
        auto c = g_table.get(fd);
        if (!c)
        {
          break;
        }
        std::unique_lock lk(c->mtx);
        c->pollout_armed = false;
        if (!c->txq.empty())
        {
          sendkick_inline(c, local_submit);
        }
        break;
      }

      case Op::Close: {
        // Safe: map holds shared_ptr; erasing doesn’t destroy if handlers still hold refs.
        g_table.erase(fd);
        break;
      }

      case Op::Buf:
      default:
        break;
      }
    } // batch

    if (local_submit)
    {
      io_uring_submit(&g_ring);
    }
  }
}

// ===================== Server scaffolding ============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = (uint16_t) std::max<unsigned>(1u, std::thread::hardware_concurrency());
};

int main(int argc, char** argv)
{
  ServerConfig cfg{};
  if (argc > 1)
  {
    cfg.port = (uint16_t) std::strtoul(argv[1], nullptr, 10);
  }
  if (argc > 2)
  {
    cfg.threads = (uint16_t) std::strtoul(argv[2], nullptr, 10);
  }

  // Raise file limits
  struct rlimit rl{.rlim_cur = 1 << 20, .rlim_max = 1 << 20};
  setrlimit(RLIMIT_NOFILE, &rl);

  // Listener
  g_listen_fd = make_listener(cfg.port);

  // Ring init (SQPOLL+COOP if available)
  io_uring_params p{};
  // p.p.flags |= IORING_SETUP_SQPOLL;flags |= IORING_SETUP_SQPOLL;
  p.flags |= IORING_SETUP_COOP_TASKRUN;
  p.flags |= IORING_SETUP_TASKRUN_FLAG;
  p.sq_thread_idle = 2000;
  if (io_uring_queue_init_params(8192, &g_ring, &p) != 0)
  {
    io_uring_params z{};
    if (io_uring_queue_init_params(8192, &g_ring, &z) != 0)
    {
      std::perror("io_uring_queue_init_params");
      return 1;
    }
  }
  g_ring_fd = g_ring.ring_fd;

  // Buffer pool
  init_buffer_pool();

  // Arm accept
  (void) post_accept();
  io_uring_submit(&g_ring);

  // Workers (all share the same ring)
  std::vector<std::thread> threads;
  threads.reserve(cfg.threads);
  for (uint16_t i = 0; i < cfg.threads; ++i)
  {
    threads.emplace_back(worker_loop);
  }

  for (auto& t : threads)
  {
    t.join();
  }

  io_uring_queue_exit(&g_ring);
  return 0;
}
