#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
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
struct StringBuf
{
  size_t len;
  const char* buf;
};

struct OwnedBuf
{
  StringBuf wb{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false);

  static OwnedBuf copy(std::string_view sv, bool eor = false);
};

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_IOV = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int ACCEPTS_PER_WORKER = 256;

static constexpr int PROC_MAX_SEGMENTS = 32;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(200);
static constexpr int BACKLOG = 16384;

// Demo payload
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

// ===================== OwnedBuf inline impl ==========================
#ifndef OWNED_BUF_INLINE_IMPL
#define OWNED_BUF_INLINE_IMPL
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
// We use low 3 bits for Op code, 29 aux bits, and 32 bits for fd.
// Layout: [ fd:32 ][ aux:29 ][ op:3 ]
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  Task,
  Close,
  PollOut,
  Buf
};

static inline uint64_t pack_ud(int fd, Op op)
{
  return (uint64_t(uint32_t(fd)) << 35) | (uint64_t(0) << 3) | uint64_t(op);
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

// ===================== Task kinds posted via MSG_RING ================
enum class TaskKind : uint8_t
{
  Parse = 0,
  Generate = 1,
  SendKick = 2,
  Close = 3
};

// fwd
struct Worker;

// ===================== Per-client state ==============================
struct PerClientStorage
{
  int fd{-1};
  uint32_t served{0};
  bool closing{false};
  bool keep_alive{true};
  size_t body_need{0};

  Worker* owner{nullptr};

  // RX
  std::deque<OwnedBuf> rxq;                   // worker-thread only
  std::shared_ptr<std::vector<char>> rx_hold; // singleshot fallback

  // TX
  std::deque<OwnedBuf> txq;
  std::deque<OwnedBuf> tx_inflight;
  bool send_inflight{false};
  bool inflight_eor{false};

  struct iovec iov[MAX_IOV];
  struct msghdr msg{};

  // policy
  uint16_t keepalive_limit{65000};

  // state
  bool ms_recv_armed{false};
  bool pollout_armed{false};

  // edge-triggered send kick
  std::atomic_flag sendkick_pending = ATOMIC_FLAG_INIT;

  // posts (MSG_RING into our worker ring)
  void post_parse();
  void post_generate();
  void post_sendkick();
  void post_close();

  // parse (budgeted)
  void parse_step();

  // small accumulator
  std::string acc;

  bool on_send_complete()
  {
    bool finished = false;
    tx_inflight.clear();
    send_inflight = false;
    if (inflight_eor)
    {
      finished = true;
    }
    inflight_eor = false;

    if (finished)
    {
      served++;
      if (served >= keepalive_limit || !keep_alive || closing)
      {
        return true;
      }
    }
    return false;
  }
};

// ===================== Per-worker connection table ===================
struct ConnTable
{
  std::unordered_map<int, std::unique_ptr<PerClientStorage>> map;

  PerClientStorage* get(int fd)
  {
    auto it = map.find(fd);
    return (it == map.end()) ? nullptr : it->second.get();
  }
  PerClientStorage* add(int fd)
  {
    auto p = std::make_unique<PerClientStorage>();
    p->fd = fd;
    auto* raw = p.get();
    map.emplace(fd, std::move(p));
    return raw;
  }
  void erase(int fd) { map.erase(fd); }
};

// ===================== Helpers =======================================
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
static int make_listener(uint16_t port, bool reuseport)
{
  int s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0)
  {
    std::perror("socket");
    std::exit(1);
  }
  int on = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (reuseport)
  {
    setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
  }

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

// ===================== Worker ========================================
struct WorkerConfig
{
  uint16_t accepts_per_worker{ACCEPTS_PER_WORKER};
  uint16_t max_keepalive_requests{65000};
};

class Worker
{
public:
  Worker(Worker&&) = default;
  Worker(int listen_fd, const WorkerConfig& cfg)
    : listen_fd_(listen_fd)
    , cfg_(cfg)
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
      if (io_uring_queue_init_params(8192, &ring_, &zero) != 0)
      {
        std::perror("io_uring_queue_init_params");
        std::exit(1);
      }
    }

    ring_fd_ = ring_.ring_fd; // liburing exposes this field

    init_buffer_pool();
  }

  ~Worker() { io_uring_queue_exit(&ring_); }

  void operator()() { run(); }

  // ---- tiny helpers used by PCS ----
  void append_acc(PerClientStorage& c, std::string_view s) { c.acc.append(s.data(), s.size()); }

  size_t find_hdr_end(const std::string& a) { return a.find("\r\n\r\n"); }
  void scan_headers(const std::string& acc, size_t hdr_end, bool& keep, size_t& clen)
  {
    keep = true;
    clen = 0;
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
  }

  // ---- inline sendkick called on worker thread ----
  void sendkick_inline(PerClientStorage* c)
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

    auto* sqe = get_sqe_or_submit();
    if (!sqe)
    {
      // rollback into queue and re-post a SendKick task so we don't lose progress
      for (size_t i = 0; i < c->tx_inflight.size(); ++i)
      {
        c->txq.emplace_front(std::move(c->tx_inflight[c->tx_inflight.size() - 1 - i]));
      }
      c->tx_inflight.clear();
      c->send_inflight = false;
      c->inflight_eor = false;
      post_task(c->fd, TaskKind::SendKick);
      return;
    }
    io_uring_prep_sendmsg(sqe, c->fd, &c->msg, MSG_NOSIGNAL);
    // sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    // io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS);
    io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Send));
    need_submit_++;
  }

  // ---- task posting via MSG_RING into our own ring ----
  void post_task(int fd, TaskKind kind)
  {
    auto* sqe = get_sqe_or_submit();
    if (!sqe)
    {
      // If ring is momentarily full, fall back to inline for SendKick; others can be retried quickly.
      if (kind == TaskKind::SendKick)
      {
        if (auto* c = table_.get(fd))
        {
          sendkick_inline(c);
        }
      }
      return;
    }
    // Pass our standard user_data through MSG_RING, it will surface in CQ as-is
    const uint64_t tag = pack_ud_aux(fd, Op::Task, static_cast<uint32_t>(kind));
    // signature: (sqe, target_ring_fd, len, data, flags)
    io_uring_prep_msg_ring(sqe, ring_fd_, 0 /*len*/, tag, 0 /*flags*/);
    need_submit_++;
  }

private:
  int listen_fd_;
  WorkerConfig cfg_;
  io_uring ring_{};
  int ring_fd_{-1};
  ConnTable table_;

  int need_submit_{0};

  // --- multishot accept state ---
  bool use_ms_accept_{true};
  bool ms_accept_armed_{false};

  // --- multishot recv + provided buffers ---
  static constexpr int BUF_SZ = RX_CAP;
  static constexpr int BUF_CNT = 2048; // per worker (~16MB)
  static constexpr int BUF_GRP = 7;

  std::unique_ptr<char[]> buf_pool_;
  bool have_buf_pool_{false};
  bool use_ms_recv_{true};

  // ---- sqe helper ----
  static inline io_uring_sqe* get_sqe_or_submit(io_uring& ring)
  {
    if (auto* sqe = io_uring_get_sqe(&ring))
    {
      return sqe;
    }
    io_uring_submit(&ring);
    if (auto* sqe2 = io_uring_get_sqe(&ring))
    {
      return sqe2;
    }
    io_uring_submit_and_wait(&ring, 1);
    return io_uring_get_sqe(&ring);
  }
  inline io_uring_sqe* get_sqe_or_submit() { return get_sqe_or_submit(ring_); }

  void init_buffer_pool()
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
      io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Op::Buf, i));
      ++posted;
    }
    if (posted)
    {
      io_uring_submit(&ring_);
    }
    have_buf_pool_ = (posted > 0);
  }

  // ---- Accept posting (multishot if possible) ----
  bool post_accept()
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
        io_uring_sqe_set_data64(sqe, pack_ud(listen_fd_, Op::Accept));
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
        io_uring_sqe_set_data64(sqe, pack_ud(listen_fd_, Op::Accept));
        return true;
      }
      return false;
    }
  }

  int post_initial_accepts()
  {
    int n = 0;
    if (use_ms_accept_)
    {
      if (post_accept())
      {
        ++n;
      }
    }
    else
    {
      for (int i = 0; i < cfg_.accepts_per_worker; ++i)
      {
        if (post_accept())
        {
          ++n;
        }
      }
    }
    return n;
  }

  bool start_recv(PerClientStorage* c)
  {
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
        // sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Recv));
        c->ms_recv_armed = true;
        return true;
      }
      return false;
    }
    else
    {
      // singleshot fallback: allocate hold and keep it alive on the PCS
      c->rx_hold = std::make_shared<std::vector<char>>(RX_CAP);
      if (auto* sqe = get_sqe_or_submit())
      {
        io_uring_prep_recv(sqe, c->fd, c->rx_hold->data(), c->rx_hold->size(), 0);
        // sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
        io_uring_sqe_set_data64(sqe, pack_ud(c->fd, Op::Recv));
        return true;
      }
      c->rx_hold.reset();
      return false;
    }
  }

  bool close_async(int fd)
  {
    if (auto* sqe = get_sqe_or_submit())
    {
      io_uring_prep_close(sqe, fd);
      io_uring_sqe_set_data64(sqe, pack_ud(fd, Op::Close));
      return true;
    }
    return false;
  }

  bool arm_pollout(PerClientStorage* c)
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

  void run()
  {
    need_submit_ += post_initial_accepts();
    if (need_submit_)
    {
      io_uring_submit(&ring_);
      need_submit_ = 0;
    }

    while (true)
    {
      io_uring_submit_and_wait(&ring_, 1);

      io_uring_cqe* cqes[512];
      unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, 512);

      for (unsigned i = 0; i < n; ++i)
      {
        io_uring_cqe* cqe = cqes[i];
        int fd;
        Op op;
        uint32_t aux;
        unpack_ud((uint64_t) io_uring_cqe_get_data64(cqe), fd, op, aux);
        int res = cqe->res;
        unsigned fl = cqe->flags;
        io_uring_cqe_seen(&ring_, cqe);

        switch (op)
        {
        case Op::Accept: {
          if (res < 0)
          {
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

        case Op::Recv: {
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

            auto vec = std::make_shared<std::vector<char>>(nbytes);
            std::memcpy(vec->data(), p, nbytes);
            c->rxq.emplace_back(OwnedBuf{StringBuf{nbytes, vec->data()}, false, vec});

            // Return buffer to pool
            if (auto* sqe = get_sqe_or_submit())
            {
              io_uring_prep_provide_buffers(sqe, p, BUF_SZ, 1, BUF_GRP, bid);
              io_uring_sqe_set_data64(sqe, pack_ud_aux(0, Op::Buf, bid));
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

        case Op::Send: {
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
              if (arm_pollout(c))
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

          if (c->on_send_complete())
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

        case Op::PollOut: {
          auto* c = table_.get(fd);
          if (!c)
          {
            break;
          }
          c->pollout_armed = false;
          sendkick_inline(c);
          break;
        }

        case Op::Close: {
          table_.erase(fd);
          break;
        }

        case Op::Task: {
          // task posted via MSG_RING: aux carries TaskKind
          TaskKind tk = static_cast<TaskKind>(aux & 0xFF);
          if (auto* c = table_.get(fd))
          {
            switch (tk)
            {
            case TaskKind::Parse:
              c->parse_step();
              break;
            case TaskKind::Generate: {
              bool keep = c->keep_alive && (c->served < c->keepalive_limit - 1);
              c->txq.emplace_back(OwnedBuf::literal(keep ? kHdrKeep : kHdrClose,
                                                    keep ? sizeof(kHdrKeep) - 1 : sizeof(kHdrClose) - 1, false));
              c->txq.emplace_back(OwnedBuf::literal(kBody, sizeof(kBody) - 1, true));
              // don’t inline here; Recv/Send handlers already inline when appropriate
              c->post_sendkick();
            }
            break;
            case TaskKind::SendKick: {
              c->sendkick_pending.clear(std::memory_order_release);
              sendkick_inline(c);
            }
            break;
            case TaskKind::Close:
              c->closing = true;
              if (close_async(c->fd))
              {
                need_submit_++;
              }
              break;
            }
          }
          break;
        }

        case Op::Buf: {
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
};

// ---- PerClientStorage method defs that need Worker ----
inline void PerClientStorage::post_parse()
{
  owner->post_task(fd, TaskKind::Parse);
}
inline void PerClientStorage::post_generate()
{
  owner->post_task(fd, TaskKind::Generate);
}
inline void PerClientStorage::post_sendkick()
{
  if (!sendkick_pending.test_and_set(std::memory_order_acq_rel))
  {
    owner->post_task(fd, TaskKind::SendKick);
  }
}
inline void PerClientStorage::post_close()
{
  owner->post_task(fd, TaskKind::Close);
}

inline void PerClientStorage::parse_step()
{
  auto t0 = std::chrono::steady_clock::now();
  int processed = 0;

  for (;;)
  {
    if (rxq.empty())
    {
      break;
    }
    int take = std::min<int>(PROC_MAX_SEGMENTS, (int) rxq.size());
    for (int i = 0; i < take; ++i)
    {
      auto seg = std::move(rxq.front());
      rxq.pop_front();

      // Append to accumulator; demo HTTP/1.1 parse
      std::string_view s(seg.wb.buf, seg.wb.len);
      owner->append_acc(*this, s);

      for (;;)
      {
        size_t hdr_end = owner->find_hdr_end(acc);
        if (hdr_end == std::string::npos)
        {
          break;
        }

        bool keep = true;
        size_t clen = 0;
        owner->scan_headers(acc, hdr_end, keep, clen);

        size_t body_off = hdr_end + 4;
        if (acc.size() < body_off + clen)
        {
          keep_alive = keep;
          body_need = (body_off + clen) - acc.size();
          break;
        }

        acc.erase(0, body_off + clen);
        keep_alive = keep;
        body_need = 0;

        bool keepconn = keep_alive && (served < keepalive_limit - 1);
        const char* hdr = keepconn ? kHdrKeep : kHdrClose;
        txq.emplace_back(OwnedBuf::literal(hdr, std::strlen(hdr), false));
        txq.emplace_back(OwnedBuf::literal(kBody, sizeof(kBody) - 1, true));
      }
    }

    if (++processed >= PROC_MAX_SEGMENTS)
    {
      break;
    }

    if (std::chrono::steady_clock::now() - t0 >= PROC_TIME_BUDGET)
    {
      break;
    }
  }
}

// ===================== Server scaffolding =============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = (uint16_t) std::max<unsigned>(1u, std::thread::hardware_concurrency());
  uint16_t pending_accepts_per_worker = ACCEPTS_PER_WORKER;
  uint16_t max_keepalive_requests = 65000;
  bool reuseport = true;
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
  {}

  void run()
  {
    const uint16_t N = std::max<uint16_t>(1, cfg_.threads);

    for (uint16_t i = 0; i < N; ++i)
    {
      listeners_.push_back(make_listener(cfg_.port, cfg_.reuseport));
    }

    WorkerConfig wcfg{};
    wcfg.accepts_per_worker = cfg_.pending_accepts_per_worker;
    wcfg.max_keepalive_requests = cfg_.max_keepalive_requests;

    workers_.reserve(N);
    threads_.reserve(N);
    for (uint16_t i = 0; i < N; ++i)
    {
      workers_.emplace_back(listeners_[i], wcfg);
      threads_.emplace_back(std::ref(workers_.back()));
    }
    for (auto& t : threads_)
    {
      t.join();
    }
  }

private:
  ServerConfig cfg_;
  std::vector<int> listeners_;
  std::vector<Worker> workers_;
  std::vector<std::thread> threads_;
};

// ===================== main ==========================================
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

  struct rlimit rl{.rlim_cur = 1 << 20, .rlim_max = 1 << 20};
  setrlimit(RLIMIT_NOFILE, &rl);

  Server srv(cfg);
  srv.run();
  return 0;
}
