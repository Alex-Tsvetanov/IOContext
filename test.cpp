// SPDX-License-Identifier: MIT
// Build: g++ -O2 -std=gnu++20 -pthread linux/http11_uring_server.cpp -luring -o http_uring
// Run  : ./http_uring [port] [threads]

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
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include <liburing.h>
#include <strings.h> // strncasecmp

// ----- your common headers -----
#include "common/string_buffer.hpp" // struct StringBuf { size_t len; const char* buf; };
#include "common/owned_buf.hpp"     // struct OwnedBuf { StringBuf wb; bool eor; std::shared_ptr<void> guard; ... }
// #include "common/processing_machine.hpp" // (optional) plug later

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_IOV = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int ACCEPTS_PER_WORKER = 256;

static constexpr int PROC_MAX_SEGMENTS = 16;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(250);
static constexpr int BACKLOG = 16384;

// Demo payload (parity with your Windows build)
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

// ===================== OwnedBuf helpers (inline impl) ================
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

static inline io_uring_sqe* get_sqe_or_submit(io_uring& ring)
{
  if (auto* sqe = io_uring_get_sqe(&ring))
  {
    return sqe;
  }
  io_uring_submit(&ring); // flush queued SQEs to free space
  if (auto* sqe2 = io_uring_get_sqe(&ring))
  {
    return sqe2;
  }
  io_uring_submit_and_wait(&ring, 1);
  return io_uring_get_sqe(&ring);
}

// ===================== CQE user_data packing =========================
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  Event,
  Close
};

static inline uint64_t pack_udata(int fd, Op op)
{
  return (uint64_t(uint32_t(fd)) << 3) | uint64_t(op);
}
static inline void unpack_udata(uint64_t u, int& fd, Op& op)
{
  op = Op(u & 0x7ull);
  fd = int((u >> 3) & 0xFFFFFFFFu);
}

// ===================== Non-IO task queue via eventfd =================
enum class TaskKind : uint8_t
{
  Parse,
  Generate,
  SendKick,
  Close
};

struct Task
{
  int fd;
  TaskKind kind;
};

struct TaskQueue
{
  int efd{-1}; // EFD_SEMAPHORE
  std::mutex mtx;
  std::deque<Task> q;

  void init()
  {
    efd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
    if (efd < 0)
    {
      std::perror("eventfd");
      std::exit(1);
    }
  }

  void push(Task t)
  {
    {
      std::lock_guard lk(mtx);
      q.emplace_back(t);
    }
    eventfd_t one = 1;
    (void) eventfd_write(efd, one); // best-effort
  }

  bool pop(Task& out)
  {
    std::lock_guard lk(mtx);
    if (q.empty())
    {
      return false;
    }
    out = q.front();
    q.pop_front();
    return true;
  }
};

static TaskQueue g_tasks;

// ===================== Per-client state ==============================
struct PerClientStorage
{
  int fd{-1};
  std::chrono::steady_clock::time_point last_active{std::chrono::steady_clock::now()};
  uint32_t served{0};
  bool closing{false};

  // --- Parser scratch (swap with your ProcessingMachine later) ---
  std::mutex rx_mtx;
  std::deque<OwnedBuf> rxq;
  std::shared_ptr<std::vector<char>> rx_hold;
  bool parse_inflight{false};
  std::string acc;
  size_t body_need{0};
  bool keep_alive{true};

  // --- Response generation ---
  std::mutex resp_mtx;
  uint32_t pending_responses{0};
  bool respond_inflight{false};

  // --- TX path ---
  std::mutex tx_mtx;
  std::deque<OwnedBuf> txq;
  std::deque<OwnedBuf> tx_inflight;
  bool send_inflight{false};
  bool inflight_eor{false};
  size_t inflight_count{0};
  struct iovec iov[MAX_IOV];
  struct msghdr msg{};

  // --- policy ---
  uint16_t keepalive_limit{65000};

  // --- recv state (multishot) ---
  bool ms_recv_armed{false};

  void mark_activity() { last_active = std::chrono::steady_clock::now(); }

  // ---- posts (non-IO go through eventfd) ----
  void post_parse()
  {
    bool need = false;
    {
      std::lock_guard lk(rx_mtx);
      if (!parse_inflight)
      {
        parse_inflight = true;
        need = true;
      }
    }
    if (need)
    {
      g_tasks.push(Task{fd, TaskKind::Parse});
    }
  }

  void post_generate()
  {
    bool need = false;
    {
      std::lock_guard lk(resp_mtx);
      ++pending_responses;
      if (!respond_inflight)
      {
        respond_inflight = true;
        need = true;
      }
    }
    if (need)
    {
      g_tasks.push(Task{fd, TaskKind::Generate});
    }
  }

  void post_sendkick() { g_tasks.push(Task{fd, TaskKind::SendKick}); }
  void post_close() { g_tasks.push(Task{fd, TaskKind::Close}); }

  // ---- parse step (simple end-of-request) ----
  void parse_step()
  {
    auto t0 = std::chrono::steady_clock::now();
    int processed = 0;

    for (;;)
    {
      std::vector<OwnedBuf> segs;
      {
        std::lock_guard lk(rx_mtx);
        int take = std::min<int>(PROC_MAX_SEGMENTS, (int) rxq.size());
        for (int i = 0; i < take; ++i)
        {
          segs.emplace_back(std::move(rxq.front()));
          rxq.pop_front();
        }
      }
      if (segs.empty())
      {
        break;
      }

      for (auto& seg : segs)
      {
        acc.append(seg.wb.buf, seg.wb.len);

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
          { // wait for body
            keep_alive = keep;
            body_need = (body_off + clen) - acc.size();
            break;
          }

          // full request present: consume (headers+body)
          acc.erase(0, body_off + clen);

          keep_alive = keep;
          body_need = 0;

          post_generate(); // schedule response
          // loop to catch pipelined requests
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

    bool repost = false;
    {
      std::lock_guard lk(rx_mtx);
      if (!rxq.empty())
      {
        repost = true;
      }
      else
      {
        parse_inflight = false;
      }
    }
    if (repost)
    {
      g_tasks.push(Task{fd, TaskKind::Parse});
    }
  }

  // ---- generate step -> fill txq with header+body, mark EOR ----
  void generate_step()
  {
    uint32_t jobs = 0;
    {
      std::lock_guard lk(resp_mtx);
      jobs = pending_responses;
      pending_responses = 0;
      respond_inflight = false;
    }
    for (uint32_t i = 0; i < jobs; ++i)
    {
      bool keep = keep_alive && (served < keepalive_limit - 1);
      const char* hdr = keep ? kHdrKeep : kHdrClose;
      {
        std::lock_guard lk(tx_mtx);
        txq.emplace_back(OwnedBuf::literal(hdr, std::strlen(hdr), false));
        txq.emplace_back(OwnedBuf::literal(kBody, sizeof(kBody) - 1, true)); // EOR on body
      }
      post_sendkick();
    }

    bool again = false;
    {
      std::lock_guard lk(resp_mtx);
      if (pending_responses && !respond_inflight)
      {
        respond_inflight = true;
        again = true;
      }
    }
    if (again)
    {
      g_tasks.push(Task{fd, TaskKind::Generate});
    }
  }

  // ---- prepare one sendmsg; returns true if an SQE was prepped ----
  bool sendkick_step(struct io_uring& ring)
  {
    std::lock_guard lk(tx_mtx);
    if (send_inflight || txq.empty())
    {
      return false;
    }

    size_t cnt = 0;
    bool eor = false;
    for (auto it = txq.begin(); it != txq.end() && cnt < MAX_IOV; ++it, ++cnt)
    {
      iov[cnt].iov_base = const_cast<char*>(it->wb.buf);
      iov[cnt].iov_len = it->wb.len;
      if (it->eor)
      {
        eor = true;
        ++cnt;
        break;
      }
    }
    if (cnt == 0)
    {
      return false;
    }

    msg = {};
    msg.msg_iov = iov;
    msg.msg_iovlen = cnt;

    tx_inflight.clear();
    for (size_t i = 0; i < cnt && !txq.empty(); ++i)
    {
      tx_inflight.emplace_back(std::move(txq.front()));
      txq.pop_front();
    }

    inflight_count = cnt;
    inflight_eor = eor;
    send_inflight = true;

    if (auto* sqe = get_sqe_or_submit(ring))
    {
      io_uring_prep_sendmsg(sqe, fd, &msg, MSG_NOSIGNAL);
      io_uring_sqe_set_data64(sqe, pack_udata(fd, Op::Send));
      return true;
    }
    else
    {
      // give buffers back if we couldn't queue *right now*
      for (size_t i = 0; i < tx_inflight.size(); ++i)
      {
        txq.emplace_front(std::move(tx_inflight[tx_inflight.size() - 1 - i]));
      }
      tx_inflight.clear();
      send_inflight = false;
      inflight_eor = false;
      inflight_count = 0;

      // schedule a retry so we don't stall this response
      post_sendkick();
      return false;
    }
  }

  // ---- on send completion; returns true if connection should close ----
  bool on_send_complete(ssize_t /*bytes*/)
  {
    bool finished = false;
    {
      std::lock_guard lk(tx_mtx);
      tx_inflight.clear();
      send_inflight = false;
      if (inflight_eor)
      {
        finished = true;
      }
      inflight_eor = false;
      inflight_count = 0;
    }
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

// ===================== Connection table ==============================
struct ConnTable
{
  std::mutex mtx;
  std::unordered_map<int, std::unique_ptr<PerClientStorage>> map;

  PerClientStorage* get(int fd)
  {
    std::lock_guard lk(mtx);
    auto it = map.find(fd);
    return (it == map.end()) ? nullptr : it->second.get();
  }
  PerClientStorage* add(int fd)
  {
    auto p = std::make_unique<PerClientStorage>();
    p->fd = fd;
    auto* raw = p.get();
    std::lock_guard lk(mtx);
    map.emplace(fd, std::move(p));
    return raw;
  }
  void erase(int fd)
  {
    std::lock_guard lk(mtx);
    map.erase(fd);
  }
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
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
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
  uint16_t idle_seconds{300}; // (sweep not implemented here; add timerfd if needed)
};

class Worker
{
public:
  Worker(Worker&&) = default;
  Worker(int listen_fd, ConnTable& table, const WorkerConfig& cfg)
    : listen_fd_(listen_fd)
    , table_(table)
    , cfg_(cfg)
  {
    // Try with SQPOLL + cooperative flags; fall back to plain if unsupported
    io_uring_params p{};
    p.flags |= IORING_SETUP_SQPOLL;
    p.flags |= IORING_SETUP_COOP_TASKRUN;
    p.flags |= IORING_SETUP_TASKRUN_FLAG;
    p.sq_thread_idle = 2000; // ms
    int rc = io_uring_queue_init_params(8192, &ring_, &p);
    if (rc != 0)
    {
      // Fallback
      io_uring_params zero{};
      if (io_uring_queue_init_params(8192, &ring_, &zero) != 0)
      {
        std::perror("io_uring_queue_init_params");
        std::exit(1);
      }
    }

    init_buffer_pool();
  }

  ~Worker() { io_uring_queue_exit(&ring_); }

  void operator()() { run(); }

private:
  int listen_fd_;
  ConnTable& table_;
  WorkerConfig cfg_;
  io_uring ring_{};

  // --- eventfd read batching ---
  struct EfdReadCtx
  {
    uint64_t val;
  };
  std::vector<std::unique_ptr<EfdReadCtx>> efd_reads_;
  static constexpr int EFD_READS_TARGET = 64;

  // --- multishot accept state ---
  bool use_ms_accept_{true};
  bool ms_accept_armed_{false};

  // --- multishot recv + provided buffers ---
  static constexpr int BUF_SZ = RX_CAP;
  static constexpr int BUF_CNT = 2048; // per worker (~8MB)
  static constexpr int BUF_GRP = 7;    // arbitrary group id within this ring

  std::unique_ptr<char[]> buf_pool_;
  bool have_buf_pool_{false};
  bool use_ms_recv_{true}; // fallback to singleshot if err

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
          have_buf_pool_ = (i > 0);
          break;
        }
      }
      io_uring_prep_provide_buffers(sqe, buf_pool_.get() + i * BUF_SZ, BUF_SZ, 1, BUF_GRP, i);
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
      if (auto* sqe = get_sqe_or_submit(ring_))
      {
        io_uring_prep_multishot_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
        io_uring_sqe_set_data64(sqe, pack_udata(listen_fd_, Op::Accept));
        ms_accept_armed_ = true;
        return true;
      }
      return false;
    }
    else
    {
      if (auto* sqe = get_sqe_or_submit(ring_))
      {
        io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, SOCK_NONBLOCK);
        io_uring_sqe_set_data64(sqe, pack_udata(listen_fd_, Op::Accept));
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

  int ensure_eventfd_reads()
  {
    int posted = 0;
    while ((int) efd_reads_.size() < EFD_READS_TARGET)
    {
      auto ctx = std::make_unique<EfdReadCtx>();
      ctx->val = 0;
      auto raw = ctx.get();
      if (auto* sqe = get_sqe_or_submit(ring_))
      {
        io_uring_prep_read(sqe, g_tasks.efd, &raw->val, sizeof(raw->val), 0);
        io_uring_sqe_set_data64(sqe, pack_udata(g_tasks.efd, Op::Event));
        efd_reads_.emplace_back(std::move(ctx));
        ++posted;
      }
      else
      {
        break;
      }
    }
    return posted;
  }

  bool start_recv(PerClientStorage* c)
  {
    if (use_ms_recv_ && have_buf_pool_)
    {
      if (c->ms_recv_armed)
      {
        return false;
      }
      if (auto* sqe = get_sqe_or_submit(ring_))
      {
        io_uring_prep_recv_multishot(sqe, c->fd, nullptr, 0, 0);
        sqe->buf_group = BUF_GRP; // pick from provided-buf group
        io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
        io_uring_sqe_set_data64(sqe, pack_udata(c->fd, Op::Recv));
        c->ms_recv_armed = true;
        return true;
      }
      return false;
    }
    else
    {
      // Fallback: single-shot recv using heap buffer (legacy path)
      auto hold = std::make_shared<std::vector<char>>(RX_CAP);
      {
        std::lock_guard lk(c->rx_mtx);
        c->rx_hold = hold;
      }
      if (auto* sqe = get_sqe_or_submit(ring_))
      {
        io_uring_prep_recv(sqe, c->fd, hold->data(), hold->size(), 0);
        io_uring_sqe_set_data64(sqe, pack_udata(c->fd, Op::Recv));
        return true;
      }
      return false;
    }
  }

  bool close_async(int fd)
  {
    if (auto* sqe = get_sqe_or_submit(ring_))
    {
      io_uring_prep_close(sqe, fd);
      io_uring_sqe_set_data64(sqe, pack_udata(fd, Op::Close));
      return true;
    }
    return false;
  }

  void handle_eventfd(int& need_submit)
  {
    Task t;
    if (!g_tasks.pop(t))
    {
      return;
    }
    if (auto* c = table_.get(t.fd))
    {
      switch (t.kind)
      {
      case TaskKind::Parse:
        c->parse_step();
        break;

      case TaskKind::Generate:
        c->generate_step();
        break;

      case TaskKind::SendKick:
        if (c->sendkick_step(ring_))
        {
          ++need_submit;
        }
        break;

      case TaskKind::Close:
        c->closing = true;
        if (close_async(c->fd))
        {
          ++need_submit;
        }
        break;
      }
    }
  }

  void run()
  {
    int need_submit = 0;
    need_submit += post_initial_accepts();
    need_submit += ensure_eventfd_reads();
    if (need_submit)
    {
      io_uring_submit(&ring_);
      need_submit = 0;
    }

    // Event loop
    while (true)
    {
      io_uring_submit_and_wait(&ring_, 1);

      io_uring_cqe* cqes[256];
      unsigned n = io_uring_peek_batch_cqe(&ring_, cqes, 256);

      for (unsigned i = 0; i < n; ++i)
      {
        io_uring_cqe* cqe = cqes[i];
        int fd = -1;
        Op op{};
        unpack_udata((uint64_t) io_uring_cqe_get_data64(cqe), fd, op);
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
              // kernel does not support multishot accept
              use_ms_accept_ = false;
              ms_accept_armed_ = false;
            }
            // Re-arm as appropriate
            if (post_accept())
            {
              ++need_submit;
            }
            break;
          }

          // New client fd
          int cfd = res;
          set_nonblock(cfd);
          set_tcp_opts(cfd);
          auto* c = table_.add(cfd);
          c->keepalive_limit = cfg_.max_keepalive_requests;
          c->mark_activity();

          if (start_recv(c))
          {
            ++need_submit;
          }

          if (!use_ms_accept_)
          {
            // single-shot: maintain accept depth
            if (post_accept())
            {
              ++need_submit;
            }
          }
          else
          {
            // multishot accept: if series ended, rearm
            if ((fl & IORING_CQE_F_MORE) == 0)
            {
              ms_accept_armed_ = false;
              if (post_accept())
              {
                ++need_submit;
              }
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

          // Common fast path
          if (res > 0)
          {
            c->mark_activity();

            if (use_ms_recv_ && have_buf_pool_ && (fl & IORING_CQE_F_BUFFER))
            {
              // Provided buffer path
              int bid = (fl >> IORING_CQE_BUFFER_SHIFT);
              char* p = buf_pool_.get() + bid * BUF_SZ;
              size_t nbytes = (size_t) res;

              auto vec = std::make_shared<std::vector<char>>(nbytes);
              std::memcpy(vec->data(), p, nbytes);

              // return buffer to pool
              if (auto* sqe = get_sqe_or_submit(ring_))
              {
                io_uring_prep_provide_buffers(sqe, p, BUF_SZ, 1, BUF_GRP, bid);
                ++need_submit;
              }

              {
                std::lock_guard lk(c->rx_mtx);
                c->rxq.emplace_back(OwnedBuf{StringBuf{nbytes, vec->data()}, false, vec});
              }
              c->post_parse();

              // If the multishot stream has ended, re-arm
              if ((fl & IORING_CQE_F_MORE) == 0)
              {
                c->ms_recv_armed = false;
                if (!c->closing && start_recv(c))
                {
                  ++need_submit;
                }
              }
              break;
            }

            // Fallback single-shot heap buffer path
            std::shared_ptr<std::vector<char>> hold;
            {
              std::lock_guard lk(c->rx_mtx);
              hold = c->rx_hold;
              c->rx_hold.reset();
              if (hold)
              {
                c->rxq.emplace_back(OwnedBuf{StringBuf{(size_t) res, hold->data()}, false, hold});
              }
            }
            c->post_parse();
            if (!c->closing && start_recv(c))
            {
              ++need_submit;
            }
            break;
          }

          // res <= 0 here: handle non-fatal multishot cases first
          if (use_ms_recv_ && have_buf_pool_)
          {
            // -ENOBUFS: kernel ran out of provided buffers temporarily.
            // DO NOT close; just re-arm the multishot recv.
            if (res == -ENOBUFS)
            {
              c->ms_recv_armed = false;
              if (!c->closing && start_recv(c))
              {
                ++need_submit;
              }
              break;
            }
            // -EAGAIN, -ECANCELED: transient; re-arm and continue
            if (res == -EAGAIN || res == -ECANCELED)
            {
              c->ms_recv_armed = false;
              if (!c->closing && start_recv(c))
              {
                ++need_submit;
              }
              break;
            }
            // -EINVAL / -EOPNOTSUPP: kernel doesn’t support multishot recv -> fallback permanently
            if (res == -EINVAL || res == -EOPNOTSUPP)
            {
              use_ms_recv_ = false;
              c->ms_recv_armed = false;
              if (!c->closing && start_recv(c))
              {
                ++need_submit; // single-shot arming
              }
              break;
            }
          }

          // EOF or fatal error: close
          c->ms_recv_armed = false;
          c->closing = true;
          if (close_async(fd))
          {
            ++need_submit;
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
            c->closing = true;
            if (close_async(fd))
            {
              ++need_submit;
            }
            break;
          }

          bool done = c->on_send_complete((ssize_t) res);
          if (done)
          {
            c->closing = true;
            if (close_async(fd))
            {
              ++need_submit;
            }
          }
          else
          {
            c->post_sendkick(); // will prep next send if queued
          }
          break;
        }

        case Op::Close: {
          table_.erase(fd);
          break;
        }

        case Op::Event: {
          // One of our outstanding eventfd reads completed; re-arm to maintain target
          if (!efd_reads_.empty())
          {
            efd_reads_.pop_back();
          }
          need_submit += ensure_eventfd_reads();
          handle_eventfd(need_submit); // run exactly one task
          break;
        }
        } // switch
      } // for batch

      if (need_submit)
      {
        io_uring_submit(&ring_);
        need_submit = 0;
      }
    }
  }
};

// ===================== Server scaffolding =============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = (uint16_t) std::max<unsigned>(1u, std::thread::hardware_concurrency());
  uint16_t pending_accepts_per_worker = ACCEPTS_PER_WORKER;
  uint16_t max_keepalive_requests = 65000;
  uint16_t idle_seconds = 300;
  bool reuseport = true; // one listener per worker for kernel load-balance
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
  {
    g_tasks.init();
  }

  void run()
  {
    const uint16_t N = std::max<uint16_t>(1, cfg_.threads);

    // SO_REUSEPORT: distinct listener per worker
    for (uint16_t i = 0; i < N; ++i)
    {
      listeners_.push_back(make_listener(cfg_.port, cfg_.reuseport));
    }

    WorkerConfig wcfg{};
    wcfg.accepts_per_worker = cfg_.pending_accepts_per_worker;
    wcfg.max_keepalive_requests = cfg_.max_keepalive_requests;
    wcfg.idle_seconds = cfg_.idle_seconds;

    workers_.reserve(N);
    threads_.reserve(N);
    for (uint16_t i = 0; i < N; ++i)
    {
      workers_.emplace_back(listeners_[i], table_, wcfg);
      threads_.emplace_back(std::ref(workers_.back()));
    }
    for (auto& t : threads_)
    {
      t.join();
    }
  }

private:
  ServerConfig cfg_;
  ConnTable table_;
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
