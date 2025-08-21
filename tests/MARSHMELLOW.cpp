// linux_epoll_fast.cpp
// Build: g++ -O3 -DNDEBUG -march=native -pthread linux_epoll_fast.cpp -o server
//
// High-performance HTTP/1.1 "Hello, World!" on Linux with epoll ET.
// - One listener per worker via SO_REUSEPORT (OS load-balancing).
// - Edge-triggered EPOLLIN/EPOLLOUT, no re-arm churn.
// - No eventfd tasks; processing inline on the worker thread.
// - TX batching via sendmsg/writev, "eor" delimits full response.
// - Stable (index,generation) handles like your IOCP version.
//
// TCP sockets only. POSIX APIs. C++20.

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

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

using namespace std::chrono;

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_IOV = 8;
static constexpr int DEFAULT_PORT = 8080;

static constexpr int PROC_MAX_SEGMENTS = 16;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(250);

// Demo payload
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

// ===================== HTTP model (placeholders) =====================
struct Request
{
  std::string method, path, protocol;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};
struct Response
{
  std::string protocol{"HTTP/1.1"};
  std::string code{"200 OK"};
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

// ===================== Core ops (for symmetry with IOCP) =============
enum class Op : uint8_t
{
  Accept,   // (epoll-driven)
  Recv,     // (epoll-driven)
  Send,     // (epoll-driven)
  Kick,     // (not used on Linux fast path)
  Process,  // (inlined)
  Respond   // (inlined)
};

// ===================== Stable handle (index,generation) ===============
struct ConnHandle
{
  uint32_t index{0}, generation{0};
};

// ===================== Helpers =======================================
[[noreturn]] static void die(const char* msg)
{
  std::perror(msg);
  std::exit(1);
}

static int set_nonblock(int fd)
{
  int fl = fcntl(fd, F_GETFL, 0);
  if (fl < 0) return -1;
  return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

static int make_listen_socket(uint16_t port, bool reuseport)
{
  int s = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (s < 0) die("socket");

  int on = 1;
  if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
    die("setsockopt(SO_REUSEADDR)");
#ifdef SO_REUSEPORT
  if (reuseport)
  {
    if (setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on)) < 0)
      die("setsockopt(SO_REUSEPORT)");
  }
#else
  (void)reuseport;
#endif

  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_ANY);
  a.sin_port = htons(port);
  if (bind(s, (sockaddr*)&a, sizeof(a)) < 0) die("bind");
  if (listen(s, SOMAXCONN) < 0) die("listen");

  if (set_nonblock(s) < 0) die("set_nonblock(listen)");
  return s;
}

// ===================== Buffer wrapper (TX/RX segments) ================
struct OwnedBuf
{
  iovec iv{};                  // base+len
  bool eor{false};             // marks end-of-response (for batching)
  std::shared_ptr<void> guard; // keeps backing memory alive (for copies)

  static OwnedBuf literal(const char* p, size_t n, bool eor = false)
  {
    return {iovec{const_cast<char*>(p), n}, eor, {}};
  }
  static OwnedBuf copy(std::string_view sv, bool eor = false)
  {
    auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
    return {iovec{buf->data(), buf->size()}, eor, buf};
  }
};

// ===================== Forward decl ===================================
struct PerClientStorage;
struct Worker;

// ===================== RequestContext (streaming parser) ==============
struct RequestContext
{
  PerClientStorage* owner{nullptr};
  enum class PS { StartLine, Headers, Body } state{PS::StartLine};
  std::string acc;
  size_t body_bytes_needed{0};
  bool keep_alive{true};

  void reset_parser()
  {
    state = PS::StartLine;
    acc.clear();
    body_bytes_needed = 0;
    keep_alive = true;
  }

  static bool find_double_crlf(const std::string& s, size_t& pos)
  {
    auto i = s.find("\r\n\r\n");
    if (i == std::string::npos) return false;
    pos = i;
    return true;
  }
  static void parse_request_line(const std::string& line, Request& req)
  {
    auto p1 = line.find(' ');
    auto p2 = (p1 == std::string::npos) ? std::string::npos : line.find(' ', p1 + 1);
    req.method   = (p1 == std::string::npos) ? line : line.substr(0, p1);
    req.path     = (p1 == std::string::npos || p2 == std::string::npos) ? "/" : line.substr(p1 + 1, p2 - p1 - 1);
    req.protocol = (p2 == std::string::npos) ? "HTTP/1.1" : line.substr(p2 + 1);
  }
  static void parse_headers(const std::string& block, Request& req, bool& keep_alive, size_t& content_len)
  {
    keep_alive = true;
    content_len = 0;
    size_t start = 0;
    while (start < block.size())
    {
      auto end = block.find("\r\n", start);
      if (end == std::string::npos) end = block.size();
      if (end == start) break;
      auto colon = block.find(':', start);
      if (colon != std::string::npos && colon < end)
      {
        std::string k = block.substr(start, colon - start);
        size_t vbeg = colon + 1;
        while (vbeg < end && (block[vbeg] == ' ' || block[vbeg] == '\t')) ++vbeg;
        std::string v = block.substr(vbeg, end - vbeg);
        req.headers.emplace(std::move(k), std::move(v));
      }
      start = end + 2;
    }
    auto it = req.headers.find("Connection");
    if (it != req.headers.end())
    {
      std::string v = it->second;
      std::transform(v.begin(), v.end(), v.begin(), ::tolower);
      keep_alive = (v.find("close") == std::string::npos);
    }
    auto it2 = req.headers.find("Content-Length");
    if (it2 != req.headers.end())
    {
      content_len = (size_t) std::strtoull(it2->second.c_str(), nullptr, 10);
    }
  }

  void on_segment(const char* p, size_t n);
};

// ===================== Connection object ==============================
struct PerClientStorage
{
  int fd{-1};
  steady_clock::time_point last_active{steady_clock::now()};
  uint32_t served{0};
  bool closing{false};

  // Parser & app
  RequestContext http;
  std::shared_ptr<Request>  req{std::make_shared<Request>()};
  std::shared_ptr<Response> res{std::make_shared<Response>()};

  // RX hot buffer (stack, to avoid per-read heap alloc)
  char rx_tmp[RX_CAP];

  // TX queue (worker-thread owned; no locks)
  std::deque<OwnedBuf> txq;
  size_t tx_first_offset{0};
  bool want_out{false}; // whether EPOLLOUT currently enabled

  // cross
  Worker* owner{nullptr};
  ConnHandle self{};

  void clear()
  {
    fd = -1;
    last_active = steady_clock::now();
    served = 0;
    closing = false;

    req = std::make_shared<Request>();
    res = std::make_shared<Response>();
    http.owner = this;
    http.reset_parser();

    txq.clear();
    tx_first_offset = 0;
    want_out = false;

    owner = nullptr;
    self = {};
  }

  void init(int sock, Worker* w, ConnHandle h)
  {
    clear();
    fd = sock;
    owner = w;
    self = h;
    last_active = steady_clock::now();
  }

  void mark_activity() { last_active = steady_clock::now(); }

  // enqueue and try to flush immediately; arm EPOLLOUT only if needed
  void tx_enqueue_literal(const char* p, size_t n, bool eor = false);
  void tx_enqueue_copy(std::string_view sv, bool eor = false);
  void enqueue_request_for_response(std::shared_ptr<Request> r);
};

// ===================== Parser implementation ==========================
void RequestContext::on_segment(const char* p, size_t n)
{
  acc.append(p, n);

  for (;;)
  {
    if (state == PS::StartLine)
    {
      auto eol = acc.find("\r\n");
      if (eol == std::string::npos) return;
      parse_request_line(acc.substr(0, eol), *owner->req);
      acc.erase(0, eol + 2);
      state = PS::Headers;
    }
    if (state == PS::Headers)
    {
      size_t hdr_end = 0;
      if (!find_double_crlf(acc, hdr_end)) return;
      size_t content_len = 0;
      parse_headers(acc.substr(0, hdr_end + 2), *owner->req, keep_alive, content_len);
      acc.erase(0, hdr_end + 4);
      owner->req->body.clear();
      owner->req->body.reserve(content_len);
      body_bytes_needed = content_len;
      state = PS::Body;
    }
    if (state == PS::Body)
    {
      size_t take = std::min<size_t>(body_bytes_needed, acc.size());
      if (take > 0)
      {
        owner->req->body.append(acc.data(), take);
        acc.erase(0, take);
        body_bytes_needed -= take;
      }
      if (body_bytes_needed > 0) return;

      owner->enqueue_request_for_response(owner->req);
      owner->res = std::make_shared<Response>();
      owner->req = std::make_shared<Request>();
      state = PS::StartLine;
    }
  }
}

// ===================== ConnTable =====================================
struct ConnSlot
{
  std::atomic<uint32_t> generation{1};
  std::atomic<bool> in_use{false};
  PerClientStorage client{};
};

class ConnTable
{
public:
  explicit ConnTable(size_t cap) : slots_(cap) {}

  ConnHandle allocate()
  {
    for (;;)
    {
      for (uint32_t i = 0; i < slots_.size(); ++i)
      {
        bool expected = false;
        if (slots_[i].in_use.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
          auto& slot = slots_[i];
          slot.client.clear();
          return ConnHandle{i, slot.generation.load(std::memory_order_relaxed)};
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  PerClientStorage* try_get(ConnHandle h)
  {
    if (h.index >= slots_.size()) return nullptr;
    auto& slot = slots_[h.index];
    if (!slot.in_use.load(std::memory_order_acquire)) return nullptr;
    if (slot.generation.load(std::memory_order_acquire) != h.generation) return nullptr;
    return &slot.client;
  }

  void close_and_recycle(ConnHandle h)
  {
    if (h.index >= slots_.size()) return;
    auto& slot = slots_[h.index];
    if (slot.in_use.exchange(false, std::memory_order_acq_rel))
    {
      auto& c = slot.client;
      if (c.fd != -1)
      {
        ::close(c.fd);
        c.fd = -1;
      }
      c.txq.clear();
      c.tx_first_offset = 0;
      c.want_out = false;

      slot.generation.fetch_add(1, std::memory_order_acq_rel);
    }
  }

  template <class Fn> void sweep_idle(seconds idle, Fn&& on_close)
  {
    auto now = steady_clock::now();
    for (uint32_t i = 0; i < slots_.size(); ++i)
    {
      auto& slot = slots_[i];
      if (!slot.in_use.load(std::memory_order_acquire)) continue;
      auto* c = &slot.client;
      if (now - c->last_active > idle)
      {
        on_close(ConnHandle{i, slot.generation.load(std::memory_order_relaxed)});
      }
    }
  }

private:
  std::vector<ConnSlot> slots_;
};

// ===================== Worker =========================================
struct Task { Op op; ConnHandle h; }; // (kept for symmetry; not used in fast path)

class Worker
{
public:
  Worker(ConnTable& table, uint16_t keepalive_limit, seconds idle_timeout,
         uint16_t port, bool reuseport)
    : table_(table)
    , keepalive_limit_(keepalive_limit)
    , idle_timeout_(idle_timeout)
    , port_(port)
    , reuseport_(reuseport)
  {}

  void operator()() { run(); }

  uint16_t keepalive_limit() const { return keepalive_limit_; }

  // map fd -> handle (single-threaded per worker)
  void map_fd(int fd, ConnHandle h) { fd2h_[fd] = h; }
  ConnHandle lookup(int fd) const
  {
    auto it = fd2h_.find(fd);
    if (it == fd2h_.end()) return {};
    return it->second;
  }
  void unmap_fd(int fd) { fd2h_.erase(fd); }

  // Try to flush TX; toggle EPOLLOUT if needed; close on completion/errors.
  void try_flush_and_arm(PerClientStorage* c)
  {
    for (;;)
    {
      auto r = flush_send_once(c);
      if (r == FlushResult::Progress) continue;

      if (r == FlushResult::Finished)
      {
        c->served++;
        if (c->served >= keepalive_limit_ || c->closing)
        {
          table_.close_and_recycle(c->self);
          unmap_fd(c->fd);
          ep_del(c->fd);
          return;
        }
        c->mark_activity();
        // continue; there might be additional responses queued
        continue;
      }
      else if (r == FlushResult::Fatal)
      {
        table_.close_and_recycle(c->self);
        unmap_fd(c->fd);
        ep_del(c->fd);
        return;
      }
      // NoData or Blocked: just set EPOLLOUT state properly and stop
      bool need_out = !c->txq.empty();
      if (need_out != c->want_out)
      {
        c->want_out = need_out;
        uint32_t ev = EPOLLIN | EPOLLRDHUP | EPOLLET | (c->want_out ? EPOLLOUT : 0);
        ep_mod(c->fd, ev);
      }
      break;
    }
  }

private:
  ConnTable& table_;
  uint16_t keepalive_limit_;
  seconds idle_timeout_;
  uint16_t port_;
  bool reuseport_;

  int epfd_{-1};
  int listen_{-1};

  std::unordered_map<int, ConnHandle> fd2h_;

  void ep_add(int fd, uint32_t ev)
  {
    epoll_event e{};
    e.events = ev;
    e.data.fd = fd;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &e) < 0) die("epoll_ctl ADD");
  }
  void ep_mod(int fd, uint32_t ev)
  {
    epoll_event e{};
    e.events = ev;
    e.data.fd = fd;
    if (epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &e) < 0) die("epoll_ctl MOD");
  }
  void ep_del(int fd)
  {
    epoll_event e{};
    if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, &e) < 0) {
      // not fatal; kernel removes on close
    }
  }

  void set_nodelay(int fd)
  {
    int on = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
  }

  void accept_loop()
  {
    for (;;)
    {
      sockaddr_in sa{}; socklen_t sl = sizeof(sa);
      int cfd = ::accept4(listen_, (sockaddr*)&sa, &sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (cfd < 0)
      {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        if (errno == EINTR) continue;
        // other errors -> return; we'll try later
        break;
      }

      set_nodelay(cfd);

      ConnHandle nh = table_.allocate();
      if (auto* c = table_.try_get(nh))
      {
        c->init(cfd, this, nh);
        map_fd(cfd, nh);
        ep_add(cfd, EPOLLIN | EPOLLRDHUP | EPOLLET); // ET, no ONESHOT
        c->mark_activity();
      }
      else
      {
        ::close(cfd);
      }
    }
  }

  void handle_recv_et(int cfd, uint32_t events)
  {
    ConnHandle h = lookup(cfd);
    auto* c = table_.try_get(h);
    if (!c) { ep_del(cfd); return; }

    if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
    {
      table_.close_and_recycle(h);
      unmap_fd(cfd);
      ep_del(cfd);
      return;
    }

    for (;;)
    {
      ssize_t n = ::recv(cfd, c->rx_tmp, sizeof(c->rx_tmp), 0);
      if (n > 0)
      {
        c->mark_activity();
        c->http.on_segment(c->rx_tmp, (size_t)n); // inline processing
      }
      else if (n == 0)
      {
        // peer closed
        table_.close_and_recycle(h);
        unmap_fd(cfd);
        ep_del(cfd);
        return;
      }
      else
      {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        if (errno == EINTR) continue;
        // fatal read error
        table_.close_and_recycle(h);
        unmap_fd(cfd);
        ep_del(cfd);
        return;
      }
    }
    // ET: no re-arm needed
  }

  enum class FlushResult { NoData, Blocked, Progress, Finished, Fatal };

  FlushResult flush_send_once(PerClientStorage* c)
  {
    if (c->txq.empty()) return FlushResult::NoData;

    // prepare iovecs
    std::vector<iovec> iov;
    iov.reserve(std::min<int>(MAX_IOV, (int)c->txq.size()));

    size_t first_off = c->tx_first_offset;
    [[maybe_unused]] bool saw_eor = false;

    int count = 0;
    for (auto it = c->txq.begin(); it != c->txq.end() && count < MAX_IOV; ++it, ++count)
    {
      auto& ob = *it;
      char* base = static_cast<char*>(ob.iv.iov_base);
      size_t len = ob.iv.iov_len;
      if (it == c->txq.begin() && first_off)
      {
        if (first_off > len) first_off = len;
        base += first_off; len -= first_off;
      }
      iov.push_back(iovec{base, len});
      if (ob.eor) { saw_eor = true; ++count; break; }
    }
    if (iov.empty()) return FlushResult::NoData;

    msghdr msg{};
    msg.msg_iov = iov.data();
    msg.msg_iovlen = (int)iov.size();
    ssize_t sent = ::sendmsg(c->fd, &msg, MSG_NOSIGNAL);
    if (sent < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
        return FlushResult::Blocked;
      return FlushResult::Fatal;
    }
    if (sent == 0) return FlushResult::Blocked; // treat as no-progress

    // consume from txq
    ssize_t remain = sent;
    bool finished_response = false;
    while (remain > 0 && !c->txq.empty())
    {
      auto& front = c->txq.front();
      size_t avail = front.iv.iov_len - c->tx_first_offset;
      if ((size_t)remain < avail)
      {
        c->tx_first_offset += (size_t)remain;
        remain = 0;
        break;
      }
      remain -= (ssize_t)avail;
      bool eor = front.eor;
      c->txq.pop_front();
      c->tx_first_offset = 0;
      if (eor) { finished_response = true; break; }
    }
    if (finished_response && c->txq.empty()) return FlushResult::Finished;
    return FlushResult::Progress;
  }

  void handle_send_ready_et(int cfd)
  {
    ConnHandle h = lookup(cfd);
    auto* c = table_.try_get(h);
    if (!c) { ep_del(cfd); return; }
    try_flush_and_arm(c);
  }

  void run()
  {
    ::signal(SIGPIPE, SIG_IGN);

    epfd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) die("epoll_create1");

    listen_ = make_listen_socket(port_, reuseport_);
    ep_add(listen_, EPOLLIN | EPOLLET);

    std::vector<epoll_event> evs(2048);
    for (;;)
    {
      int n = epoll_wait(epfd_, evs.data(), (int)evs.size(), 1000);
      if (n < 0)
      {
        if (errno == EINTR) continue;
        die("epoll_wait");
      }
      if (n == 0)
      {
        // periodic idle sweep
        table_.sweep_idle(idle_timeout_, [&](ConnHandle h){
          if (auto* c = table_.try_get(h))
          {
            c->closing = true;
            table_.close_and_recycle(h);
            if (c->fd != -1) { unmap_fd(c->fd); ep_del(c->fd); }
          }
        });
        continue;
      }

      for (int i = 0; i < n; ++i)
      {
        auto &e = evs[i];
        int fd = e.data.fd;
        uint32_t events = e.events;

        if (fd == listen_)
        {
          accept_loop();
          continue;
        }

        if (events & EPOLLIN)  handle_recv_et(fd, events);
        if (events & EPOLLOUT) handle_send_ready_et(fd);
        if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
        {
          ConnHandle h = lookup(fd);
          table_.close_and_recycle(h);
          unmap_fd(fd);
          ep_del(fd);
        }
      }
    }
  }
};

// ==== PerClientStorage TX / Respond ===================================
void PerClientStorage::tx_enqueue_literal(const char* p, size_t n, bool eor)
{
  txq.emplace_back(OwnedBuf::literal(p, n, eor));
  owner->try_flush_and_arm(this);
}
void PerClientStorage::tx_enqueue_copy(std::string_view sv, bool eor)
{
  txq.emplace_back(OwnedBuf::copy(sv, eor));
  owner->try_flush_and_arm(this);
}
void PerClientStorage::enqueue_request_for_response(std::shared_ptr<Request> r)
{
  (void)r; // demo responder ignores request body/headers
  bool keep = http.keep_alive && (served < owner->keepalive_limit() - 1);
  const char* hdr = keep ? kHdrKeep : kHdrClose;
  tx_enqueue_literal(hdr, std::strlen(hdr), false);
  tx_enqueue_literal(kBody, sizeof(kBody) - 1, true);
}

// ===================== Server scaffolding =============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = (uint16_t) std::max<unsigned>(1u, std::thread::hardware_concurrency());
  uint16_t max_keepalive_requests = 65000; // keep connections alive during the run
  uint32_t table_capacity = 1u << 15;      // 32768 slots
  uint16_t idle_seconds = 300;             // generous idle window
  bool reuseport = true;                   // per-thread accept
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
    , table_(cfg.table_capacity)
  {}

  void run()
  {
    if (cfg_.threads == 0) cfg_.threads = 1;
    workers_.reserve(cfg_.threads);
    threads_.reserve(cfg_.threads);
    for (uint16_t i = 0; i < cfg_.threads; ++i)
    {
      workers_.emplace_back(std::make_unique<Worker>(
        table_, cfg_.max_keepalive_requests, seconds(cfg_.idle_seconds),
        cfg_.port, cfg_.reuseport));
      threads_.emplace_back(std::ref(*workers_.back()));
    }
    for (auto& t : threads_) t.join();
  }

private:
  ServerConfig cfg_;
  ConnTable table_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::vector<std::thread> threads_;
};

// ===================== main ==========================================
int main()
{
  ServerConfig cfg{};
  Server srv(cfg);
  srv.run();
  return 0;
}
