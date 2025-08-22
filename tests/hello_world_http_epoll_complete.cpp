#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <functional>

#ifndef __linux__
  #error This file targets Linux epoll.
#endif

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

using namespace std::chrono;

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_WSABUF = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int ACCEPTS_PER_WORKER = 128;
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

// ===================== IO Core types =================================
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  Process,
  Respond,
  Custom
};

struct IoHeader
{
  Op op{};
  int socket_fd{-1};
  void* user_data{nullptr};
};

// ===================== Stable handle (index,generation) ===============
struct ConnHandle
{
  uint32_t index{0}, generation{0};
};

static inline uintptr_t pack_key(ConnHandle h)
{
  return (uintptr_t) ((uint64_t(h.generation) << 32) | uint64_t(h.index));
}

static inline ConnHandle unpack_key(uintptr_t k)
{
  return ConnHandle{uint32_t(k & 0xFFFFFFFFu), uint32_t(k >> 32)};
}

// ===================== Buffer wrapper =================================
struct OwnedBuf
{
  struct iovec iov{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false)
  {
    return {struct iovec{(void*)p, n}, eor, {}};
  }

  static OwnedBuf copy(std::string_view sv, bool eor = false)
  {
    auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
    return {struct iovec{buf->data(), buf->size()}, eor, buf};
  }
};

// ===================== Forward decl ===================================
struct PerClientStorage;

// ===================== RequestContext (streaming parser) ==============
struct RequestContext
{
  PerClientStorage* owner{nullptr};
  enum class PS
  {
    StartLine,
    Headers,
    Body
  } state{PS::StartLine};
  std::string acc; // accumulates across segments
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
    if (i == std::string::npos)
      return false;
    pos = i;
    return true;
  }

  static void parse_request_line(const std::string& line, Request& req)
  {
    auto p1 = line.find(' ');
    auto p2 = (p1 == std::string::npos) ? std::string::npos : line.find(' ', p1 + 1);
    req.method = (p1 == std::string::npos) ? line : line.substr(0, p1);
    req.path = (p1 == std::string::npos || p2 == std::string::npos) ? "/" : line.substr(p1 + 1, p2 - p1 - 1);
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
      if (end == std::string::npos)
        end = block.size();
      if (end == start)
        break;
      auto colon = block.find(':', start);
      if (colon != std::string::npos && colon < end)
      {
        std::string k = block.substr(start, colon - start);
        size_t vbeg = colon + 1;
        while (vbeg < end && (block[vbeg] == ' ' || block[vbeg] == '\t'))
          ++vbeg;
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
  // ---- sockets & ops ----
  int socket_fd{-1};
  IoHeader rctx{}, wctx{}, proc{}, resp{};

  // ---- policy ----
  steady_clock::time_point last_active{steady_clock::now()};
  uint32_t served{0};
  bool closing{false};

  // ---- app state ----
  RequestContext http;
  std::shared_ptr<Request> req{std::make_shared<Request>()};
  std::shared_ptr<Response> res{std::make_shared<Response>()};

  // ============ RX side (mutex-protected) ============
  std::shared_ptr<std::vector<char>> rx_hold; // in-flight recv buffer
  std::mutex rx_mtx;
  std::deque<OwnedBuf> rxq;
  bool parse_inflight{false};

  // ============ Respond queue ============
  std::mutex resp_mtx;
  std::deque<std::shared_ptr<Request>> respq;
  bool respond_inflight{false};

  // ============ TX side (mutex-protected) ============
  std::mutex tx_mtx;
  std::deque<OwnedBuf> txq; // queue of chunks (front -> send)
  bool send_inflight{false};
  int inflight_count{0};
  bool inflight_eor{false};
  struct iovec tx[MAX_WSABUF];

  // cross-refs
  int epoll_fd{-1};
  ConnHandle self{};

  void clear()
  {
    socket_fd = -1;
    std::memset(&rctx, 0, sizeof(rctx));
    std::memset(&wctx, 0, sizeof(wctx));
    std::memset(&proc, 0, sizeof(proc));
    std::memset(&resp, 0, sizeof(resp));

    last_active = steady_clock::now();
    served = 0;
    closing = false;

    req = std::make_shared<Request>();
    res = std::make_shared<Response>();
    http.owner = this;
    http.reset_parser();

    rx_hold.reset();
    {
      std::scoped_lock lr(rx_mtx, resp_mtx, tx_mtx);
      rxq.clear();
      respq.clear();
      txq.clear();
      parse_inflight = false;
      respond_inflight = false;
      send_inflight = false;
      inflight_count = 0;
      inflight_eor = false;
    }

    epoll_fd = -1;
    self = {};
  }

  void init(int sock, int port, ConnHandle h)
  {
    clear();
    socket_fd = sock;
    epoll_fd = port;
    self = h;

    std::memset(&rctx, 0, sizeof(rctx));
    rctx.op = Op::Recv;
    rctx.socket_fd = socket_fd;
    std::memset(&wctx, 0, sizeof(wctx));
    wctx.op = Op::Send;
    wctx.socket_fd = socket_fd;
    std::memset(&proc, 0, sizeof(proc));
    proc.op = Op::Process;
    proc.socket_fd = socket_fd;
    std::memset(&resp, 0, sizeof(resp));
    resp.op = Op::Respond;
    resp.socket_fd = socket_fd;

    last_active = steady_clock::now();
  }

  void mark_activity() { last_active = steady_clock::now(); }

  // ==== TX publishing (thread-safe) ====
  void tx_enqueue_literal(const char* p, size_t n, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::literal(p, n, eor));
    if (!send_inflight)
    {
      send_inflight = true;
      // Post custom event to epoll to trigger send
      uint64_t value = pack_key(self);
      write(epoll_fd, &value, sizeof(value));
    }
  }

  void tx_enqueue_copy(std::string_view sv, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::copy(sv, eor));
    if (!send_inflight)
    {
      send_inflight = true;
      // Post custom event to epoll to trigger send
      uint64_t value = pack_key(self);
      write(epoll_fd, &value, sizeof(value));
    }
  }

  // ==== Respond scheduling (thread-safe) ====
  void enqueue_request_for_response(std::shared_ptr<Request> r)
  {
    std::lock_guard lr(resp_mtx);
    respq.emplace_back(std::move(r));
    if (!respond_inflight)
    {
      respond_inflight = true;
      // Post custom event to epoll to trigger respond
      uint64_t value = pack_key(self);
      write(epoll_fd, &value, sizeof(value));
    }
  }
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
      if (eol == std::string::npos)
        return;
      parse_request_line(acc.substr(0, eol), *owner->req);
      acc.erase(0, eol + 2);
      state = PS::Headers;
    }
    if (state == PS::Headers)
    {
      size_t hdr_end = 0;
      if (!find_double_crlf(acc, hdr_end))
        return;
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
      if (body_bytes_needed > 0)
        return;

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
  explicit ConnTable(size_t cap)
    : slots_(cap)
  {}

  ConnHandle allocate(int socket_fd)
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
      usleep(1000); // Sleep for 1ms
    }
  }

  PerClientStorage* try_get(ConnHandle h)
  {
    if (h.index >= slots_.size())
      return nullptr;
    auto& slot = slots_[h.index];
    if (!slot.in_use.load(std::memory_order_acquire))
      return nullptr;
    if (slot.generation.load(std::memory_order_acquire) != h.generation)
      return nullptr;
    return &slot.client;
  }

  void close_and_recycle(ConnHandle h)
  {
    if (h.index >= slots_.size())
      return;
    auto& slot = slots_[h.index];
    if (slot.in_use.exchange(false, std::memory_order_acq_rel))
    {
      auto& c = slot.client;

      if (c.socket_fd != -1)
      {
        ::close(c.socket_fd);
        c.socket_fd = -1;
      }

      // Leave txq intact if a send might still be in flight; it will be cleared at next init().
      {
        std::lock_guard lk(c.tx_mtx);
        c.send_inflight = false;
      }
      {
        std::lock_guard lr(c.rx_mtx);
        c.rxq.clear();
        c.parse_inflight = false;
      }
      {
        std::lock_guard lr(c.resp_mtx);
        c.respq.clear();
        c.respond_inflight = false;
      }

      slot.generation.fetch_add(1, std::memory_order_acq_rel);
    }
  }

  template <class Fn> void sweep_idle(seconds idle, Fn&& on_close)
  {
    auto now = steady_clock::now();
    for (uint32_t i = 0; i < slots_.size(); ++i)
    {
      auto& slot = slots_[i];
      if (!slot.in_use.load(std::memory_order_acquire))
        continue;
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

// ===================== Accept context =================================
struct AcceptCtx
{
  int listen_fd{-1};
  int accept_fd{-1};
  sockaddr_in client_addr{};
  socklen_t client_len{sizeof(client_addr)};
};

// ===================== Worker =========================================
class Worker
{
public:
  Worker(int epoll_fd, int listen_fd, ConnTable& table, uint16_t accepts_per_worker, uint16_t keepalive_limit,
         seconds idle_timeout)
    : epoll_fd_(epoll_fd)
    , listen_fd_(listen_fd)
    , table_(table)
    , accepts_per_worker_(accepts_per_worker)
    , keepalive_limit_(keepalive_limit)
    , idle_timeout_(idle_timeout)
    , accept_pool_(accepts_per_worker_)
  {}

  void operator()() { run(); }

private:
  int epoll_fd_;
  int listen_fd_;
  ConnTable& table_;
  uint16_t accepts_per_worker_;
  uint16_t keepalive_limit_;
  seconds idle_timeout_;
  std::vector<std::unique_ptr<AcceptCtx>> accept_pool_;

  void post_accept(AcceptCtx* ac)
  {
    if (ac->accept_fd != -1)
    {
      close(ac->accept_fd);
      ac->accept_fd = -1;
    }

    ac->accept_fd = accept4(listen_fd_, (sockaddr*)&ac->client_addr, &ac->client_len, SOCK_NONBLOCK);
    if (ac->accept_fd == -1)
    {
      if (errno != EAGAIN && errno != EWOULDBLOCK)
      {
        perror("accept4 failed");
      }
      return;
    }

    ConnHandle nh = table_.allocate(ac->accept_fd);
    if (auto* c = table_.try_get(nh))
    {
      c->init(ac->accept_fd, epoll_fd_, nh);
      c->mark_activity();

      // Add to epoll for reading
      struct epoll_event ev;
      ev.events = EPOLLIN | EPOLLET;
      ev.data.u64 = pack_key(nh);
      epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, ac->accept_fd, &ev);
    }
    post_accept(ac);
  }

  void maybe_start_send_worker(PerClientStorage* c)
  {
    int tx_count = 0;
    bool eor = false;

    {
      std::lock_guard lk(c->tx_mtx);
      c->send_inflight = false;
      if (c->txq.empty())
        return;

      for (auto it = c->txq.begin(); it != c->txq.end() && tx_count < MAX_WSABUF; ++it, ++tx_count)
      {
        c->tx[tx_count] = it->iov;
        if (it->eor)
        {
          eor = true;
          ++tx_count;
          break;
        }
      }
      if (tx_count == 0)
        return; // nothing to send (paranoia)

      c->inflight_count = tx_count;
      c->inflight_eor = eor;
      c->send_inflight = true;
    }

    c->wctx.op = Op::Send;
    c->wctx.socket_fd = c->socket_fd;

    // Use writev for vectored I/O
    ssize_t sent = writev(c->socket_fd, c->tx, tx_count);
    if (sent == -1)
    {
      if (errno != EAGAIN && errno != EWOULDBLOCK)
      {
        std::lock_guard lk(c->tx_mtx);
        c->send_inflight = false;
        ::close(c->socket_fd);
        c->closing = true;
      }
    }
  }

  void run()
  {
    // Accept pool
    for (uint16_t i = 0; i < accepts_per_worker_; ++i)
    {
      accept_pool_[i] = std::make_unique<AcceptCtx>();
      accept_pool_[i]->listen_fd = listen_fd_;
      post_accept(accept_pool_[i].get());
    }

    std::vector<struct epoll_event> events(1024);

    for (;;)
    {
      int nfds = epoll_wait(epoll_fd_, events.data(), events.size(), 1000);

      if (nfds == -1)
      {
        if (errno == EINTR)
          continue;
        perror("epoll_wait failed");
        break;
      }

      if (nfds == 0)
      {
        // Timeout - sweep idle connections
        table_.sweep_idle(idle_timeout_, [&](ConnHandle h) {
          if (auto* c = table_.try_get(h))
          {
            c->closing = true;
            table_.close_and_recycle(h);
          }
        });
        continue;
      }

      // First pass: handle network events
      for (int i = 0; i < nfds; ++i)
      {
        uint64_t key = events[i].data.u64;
        uint32_t epev = events[i].events;

        // Check if this is our custom event
        if (key == 0)
        {
          // Consume custom events
          uint64_t value;
          read(epoll_fd_, &value, sizeof(value));
          continue;
        }

        ConnHandle h = unpack_key(key);

        if (epev & (EPOLLERR | EPOLLHUP))
        {
          table_.close_and_recycle(h);
          continue;
        }

        if (epev & EPOLLIN)
        {
          auto* c = table_.try_get(h);
          if (!c)
            continue;

          auto hold = std::make_shared<std::vector<char>>(RX_CAP);
          ssize_t recvd = read(c->socket_fd, hold->data(), hold->size());

          if (recvd <= 0)
          {
            if (recvd == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
            {
              table_.close_and_recycle(h);
            }
            continue;
          }

          c->mark_activity();

          std::shared_ptr<std::vector<char>> hold_shared;
          {
            std::lock_guard lk(c->rx_mtx);
            hold_shared = c->rx_hold;
            c->rx_hold.reset();
            if (hold_shared)
              c->rxq.emplace_back(OwnedBuf{struct iovec{hold_shared->data(), (size_t)recvd}, false, hold_shared});
            if (!c->parse_inflight)
            {
              c->parse_inflight = true;
              // Post custom event for processing
              uint64_t value = pack_key(h);
              write(epoll_fd_, &value, sizeof(value));
            }
          }
        }

        if (epev & EPOLLOUT)
        {
          auto* c = table_.try_get(h);
          if (!c)
            continue;
          maybe_start_send_worker(c);
        }
      }

      // Second pass: handle custom events (process, respond, etc.)
      for (int i = 0; i < nfds; ++i)
      {
        uint64_t key = events[i].data.u64;
        if (key == 0)
          continue;

        ConnHandle h = unpack_key(key);

        // Handle process events
        {
          auto* c = table_.try_get(h);
          if (!c || !c->parse_inflight)
            continue;

          auto t0 = steady_clock::now();
          int processed = 0;

          for (;;)
          {
            std::vector<OwnedBuf> segs;
            {
              std::lock_guard lk(c->rx_mtx);
              int take = std::min<int>(PROC_MAX_SEGMENTS, (int) c->rxq.size());
              for (int i = 0; i < take; ++i)
              {
                segs.emplace_back(std::move(c->rxq.front()));
                c->rxq.pop_front();
              }
            }
            if (segs.empty())
              break;

            for (auto& seg : segs)
            {
              c->http.on_segment((char*)seg.iov.iov_base, seg.iov.iov_len);
              if (++processed >= PROC_MAX_SEGMENTS)
                break;
            }
            if (processed >= PROC_MAX_SEGMENTS)
              break;
            if (steady_clock::now() - t0 >= PROC_TIME_BUDGET)
              break;
          }

          bool repost = false;
          {
            std::lock_guard lk(c->rx_mtx);
            if (!c->rxq.empty())
              repost = true;
            else
              c->parse_inflight = false;
          }
          if (repost)
          {
            uint64_t value = pack_key(h);
            write(epoll_fd_, &value, sizeof(value));
          }
        }

        // Handle respond events
        {
          auto* c = table_.try_get(h);
          if (!c || !c->respond_inflight)
            continue;

          std::vector<std::shared_ptr<Request>> reqs;
          {
            std::lock_guard lr(c->resp_mtx);
            while (!c->respq.empty())
            {
              reqs.emplace_back(std::move(c->respq.front()));
              c->respq.pop_front();
            }
            c->respond_inflight = false;
          }

          for (auto& rq : reqs)
          {
            bool keep = c->http.keep_alive && (c->served < keepalive_limit_ - 1);
            const char* hdr = keep ? kHdrKeep : kHdrClose;
            c->tx_enqueue_literal(hdr, std::strlen(hdr), false);
            c->tx_enqueue_literal(kBody, sizeof(kBody) - 1, true);
          }

          {
            std::lock_guard lr(c->resp_mtx);
            if (!c->respq.empty() && !c->respond_inflight)
            {
              c->respond_inflight = true;
              uint64_t value = pack_key(h);
              write(epoll_fd_, &value, sizeof(value));
            }
          }
        }

        // Handle send completion
        {
          auto* c = table_.try_get(h);
          if (!c)
            continue;

          bool finished = false;
          {
            std::lock_guard lk(c->tx_mtx);
            for (int i = 0; i < c->inflight_count && !c->txq.empty(); ++i)
              c->txq.pop_front();
            finished = c->inflight_eor;
            c->inflight_count = 0;
            c->inflight_eor = false;
            c->send_inflight = false;

            if (!c->txq.empty())
            {
              uint64_t value = pack_key(h);
              write(epoll_fd_, &value, sizeof(value));
            }
          }

          if (finished)
          {
            c->served++;
            if (c->served < keepalive_limit_ && !c->closing)
            {
              c->mark_activity();
            }
            else
            {
              table_.close_and_recycle(h);
            }
          }
        }
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
  uint16_t max_keepalive_requests = 65000; // keep connections alive for the whole run
  uint32_t table_capacity = 1u << 15;      // 32768 slots
  uint16_t idle_seconds = 300;             // generous idle window
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
    // Create epoll instance
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1)
    {
      perror("epoll_create1 failed");
      exit(1);
    }

    // Create eventfd for custom events
    event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd_ == -1)
    {
      perror("eventfd failed");
      exit(1);
    }

    // Add eventfd to epoll
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.u64 = 0; // 0 indicates this is our eventfd
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev) == -1)
    {
      perror("epoll_ctl add eventfd failed");
      exit(1);
    }

    // Create listen socket
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
    if (listen_fd_ == -1)
    {
      perror("socket failed");
      exit(1);
    }

    int on = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));

    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = INADDR_ANY;
    a.sin_port = htons(cfg_.port);
    if (bind(listen_fd_, (sockaddr*) &a, sizeof(a)) == -1)
    {
      perror("bind failed");
      exit(1);
    }

    if (listen(listen_fd_, SOMAXCONN) == -1)
    {
      perror("listen failed");
      exit(1);
    }

    printf("Server listening on port %d\n", cfg_.port);

    if (cfg_.threads == 0)
      cfg_.threads = 1;
    workers_.reserve(cfg_.threads);
    threads_.reserve(cfg_.threads);
    for (uint16_t i = 0; i < cfg_.threads; ++i)
    {
      workers_.emplace_back(epoll_fd_, listen_fd_, table_, cfg_.pending_accepts_per_worker, cfg_.max_keepalive_requests,
                            seconds(cfg_.idle_seconds));
      threads_.emplace_back(std::ref(workers_.back()));
    }
    for (auto& t : threads_)
      t.join();
  }

private:
  ServerConfig cfg_;
  int listen_fd_{-1};
  int epoll_fd_{-1};
  int event_fd_{-1};
  ConnTable table_;
  std::vector<Worker> workers_;
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