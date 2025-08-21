// iocp_server_structured_fixed.cpp
// Build: link Ws2_32 + Mswsock (CMake below)

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef _WIN32
#  define _WIN32_WINNT 0x0600
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <winsock2.h>
#  include <mswsock.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "Ws2_32.lib")
#  pragma comment(lib, "Mswsock.lib")
#else
#  error This file targets Windows IOCP.
#endif

using namespace std::chrono;

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

// Forward decl
struct PerClientStorage;

// ===================== RequestContext (chunk-based) ==================
struct RequestContext
{
  PerClientStorage* owner{nullptr};
  bool headers_complete{false};

  void on_data(std::string_view chunk);
  void produce_hello(bool keep_alive);
  void reset_parser() { headers_complete = false; }
};

// ===================== IO Core types =================================
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send
};

struct IoHeader
{
  OVERLAPPED ol{}; // must be first
  Op op{};
  SOCKET s{INVALID_SOCKET};
};

#ifndef CONTAINING_RECORD
#  include <cstddef>
#  define CONTAINING_RECORD(ptr, type, member)                                                                         \
    (reinterpret_cast<type*>(reinterpret_cast<char*>(ptr) - offsetof(type, member)))
#endif

static constexpr DWORD ADDR_LEN = sizeof(SOCKADDR_STORAGE) + 16;
static constexpr int RX_CAP = 8192; // receive chunk capacity
static constexpr int MAX_WSABUF = 8;
static constexpr int DEFAULT_PORT = 8081;

static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

static inline bool has_double_crlf(const char* buf, size_t len)
{
  if (len < 4)
    return false;
  for (size_t i = 3; i < len; ++i)
    if (buf[i - 3] == '\r' && buf[i - 2] == '\n' && buf[i - 1] == '\r' && buf[i] == '\n')
      return true;
  return false;
}

// ===================== Stable handle (index,generation) ==============
struct ConnHandle
{
  uint32_t index{0}, generation{0};
};
static_assert(sizeof(ULONG_PTR) >= 8, "This sample packs handle into 64-bit IOCP key (build x64).");

static inline ULONG_PTR pack_key(ConnHandle h)
{
  return (ULONG_PTR) ((uint64_t(h.generation) << 32) | uint64_t(h.index));
}
static inline ConnHandle unpack_key(ULONG_PTR k)
{
  return ConnHandle{uint32_t(k & 0xFFFFFFFFu), uint32_t(k >> 32)};
}

// ===================== PerClientStorage ===============================
struct PerClientStorage
{
  // ---- lifetime & ownership ----
  SOCKET s{INVALID_SOCKET};
  IoHeader rctx{}, wctx{}; // op set in init

  // ---- activity & policy ----
  steady_clock::time_point last_active{steady_clock::now()};
  uint32_t served{0}; // completed requests
  bool closing{false};

  // ---- HTTP runtime ----
  RequestContext http;
  std::shared_ptr<Request> req{std::make_shared<Request>()};
  std::shared_ptr<Response> res{std::make_shared<Response>()};

  // ---- RX storage ----
  alignas(64) char rx[RX_CAP];
  size_t rx_len{0};

  // ---- TX queue: gather I/O segments ----
  WSABUF tx[MAX_WSABUF];
  int tx_count{0};

  // ---- Inflight flags (prevent double posts) ----
  std::atomic<bool> recv_inflight{false};
  std::atomic<bool> send_inflight{false};

  // ---- Header readiness flag (set by RequestContext) ----
  bool headers_ready{false};

  // Reset object to a clean state (without closing any socket).
  void clear()
  {
    s = INVALID_SOCKET;
    std::memset(&rctx, 0, sizeof(rctx));
    std::memset(&wctx, 0, sizeof(wctx));
    last_active = steady_clock::now();
    served = 0;
    closing = false;
    http = RequestContext{};
    http.owner = this; // will be set again in init(), but keep consistent
    req = std::make_shared<Request>();
    res = std::make_shared<Response>();
    rx_len = 0;
    tx_count = 0;
    recv_inflight.store(false, std::memory_order_relaxed);
    send_inflight.store(false, std::memory_order_relaxed);
    headers_ready = false;
  }

  void init(SOCKET sock)
  {
    s = sock;
    rx_len = 0;
    served = 0;
    closing = false;
    headers_ready = false;
    recv_inflight.store(false, std::memory_order_relaxed);
    send_inflight.store(false, std::memory_order_relaxed);
    std::memset(&rctx, 0, sizeof(rctx));
    std::memset(&wctx, 0, sizeof(wctx));
    rctx.op = Op::Recv;
    rctx.s = s;
    wctx.op = Op::Send;
    wctx.s = s;
    http.owner = this;
    http.reset_parser();
    last_active = steady_clock::now();
  }

  void mark_activity() { last_active = steady_clock::now(); }

  void queue_response(bool keep)
  {
    tx_count = 0;
    const char* h = keep ? kHdrKeep : kHdrClose;
    tx[tx_count++] = WSABUF{(ULONG) std::strlen(h), const_cast<char*>(h)};
    tx[tx_count++] = WSABUF{(ULONG) (sizeof(kBody) - 1), const_cast<char*>(kBody)};
  }

  // Reset state to accept the next request on this keep-alive connection
  void reset_for_next_request()
  {
    headers_ready = false;
    http.reset_parser(); // <— critical fix
    rx_len = 0;
  }
};

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

  ConnHandle allocate(SOCKET s)
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
          slot.client.init(s);
          return ConnHandle{i, slot.generation.load(std::memory_order_relaxed)};
        }
      }
      ::Sleep(1);
    }
  }

  PerClientStorage* try_get(ConnHandle h)
  {
    auto& slot = slots_[h.index];
    if (!slot.in_use.load(std::memory_order_acquire))
      return nullptr;
    if (slot.generation.load(std::memory_order_acquire) != h.generation)
      return nullptr;
    return &slot.client;
  }

  void close_and_recycle(ConnHandle h)
  {
    auto& slot = slots_[h.index];
    if (slot.in_use.exchange(false, std::memory_order_acq_rel))
    {
      if (slot.client.s != INVALID_SOCKET)
      {
        ::closesocket(slot.client.s);
        slot.client.s = INVALID_SOCKET;
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

// ===================== WinSock wrapper =================================
struct WinSockWrapper
{
  static inline LPFN_ACCEPTEX AcceptExPtr = nullptr;

  WinSockWrapper()
  {
    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
      die("WSAStartup failed");
  }
  ~WinSockWrapper() { WSACleanup(); }

  [[noreturn]] static void die(const char* msg)
  {
    std::fprintf(stderr, "%s (WSA=%d)\n", msg, WSAGetLastError());
    std::exit(1);
  }

  static void load_extensions(SOCKET s)
  {
    DWORD bytes = 0;
    GUID g1 = WSAID_ACCEPTEX;
    if (WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER, &g1, sizeof(g1), &AcceptExPtr, sizeof(AcceptExPtr), &bytes,
                 nullptr, nullptr) == SOCKET_ERROR)
      die("WSAIoctl(WSAID_ACCEPTEX) failed");
  }

  static SOCKET make_listen_socket(uint16_t port)
  {
    SOCKET s = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (s == INVALID_SOCKET)
      die("WSASocket listen");
    int on = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*) &on, sizeof(on));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = INADDR_ANY;
    a.sin_port = htons(port);
    if (bind(s, (sockaddr*) &a, sizeof(a)) == SOCKET_ERROR)
      die("bind");
    if (listen(s, SOMAXCONN) == SOCKET_ERROR)
      die("listen");
    return s;
  }
} wsa_;

// ===================== Accept context ==================================
struct AcceptCtx
{
  IoHeader hdr; // op=Accept
  SOCKET acceptSock{INVALID_SOCKET};
  char addrbuf[ADDR_LEN * 2];
};

// ===================== Worker ==========================================
class Worker
{
public:
  Worker(HANDLE iocp, SOCKET listen, ConnTable& table, uint16_t accepts_per_worker, uint16_t keepalive_limit,
         seconds idle_timeout)
    : iocp_(iocp)
    , listen_(listen)
    , table_(table)
    , accepts_per_worker_(accepts_per_worker)
    , keepalive_limit_(keepalive_limit)
    , idle_timeout_(idle_timeout)
  {}

  void operator()() { run(); }

private:
  HANDLE iocp_;
  SOCKET listen_;
  ConnTable& table_;
  uint16_t accepts_per_worker_;
  uint16_t keepalive_limit_;
  seconds idle_timeout_;
  std::vector<std::unique_ptr<AcceptCtx>> accept_pool_;

  void post_accept(AcceptCtx* ac)
  {
    if (ac->acceptSock != INVALID_SOCKET)
    {
      ac->acceptSock = INVALID_SOCKET;
    }
    ZeroMemory(&ac->hdr.ol, sizeof(ac->hdr.ol));
    ac->hdr.op = Op::Accept;
    ac->hdr.s = listen_;
    ac->acceptSock = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
    if (ac->acceptSock == INVALID_SOCKET)
      WinSockWrapper::die("WSASocket acceptSock");
    DWORD bytes = 0;
    BOOL ok =
      WinSockWrapper::AcceptExPtr(listen_, ac->acceptSock, ac->addrbuf, 0, ADDR_LEN, ADDR_LEN, &bytes, &ac->hdr.ol);
    if (!ok && WSAGetLastError() != ERROR_IO_PENDING)
      WinSockWrapper::die("AcceptEx");
  }

  static inline void start_recv(PerClientStorage* c)
  {
    bool expected = false;
    if (!c->recv_inflight.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
      return;
    ZeroMemory(&c->rctx.ol, sizeof(c->rctx.ol));
    WSABUF b{(ULONG) (RX_CAP - c->rx_len), c->rx + c->rx_len};
    DWORD flags = 0, recvd = 0;
    int rc = WSARecv(c->s, &b, 1, &recvd, &flags, &c->rctx.ol, nullptr);
    if (rc == 0)
    {
      // DO NOT process inline; completion will still arrive on IOCP
      return;
    }
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      c->recv_inflight.store(false, std::memory_order_release);
      ::closesocket(c->s);
      c->closing = true;
    }
  }

  static inline void start_send(PerClientStorage* c)
  {
    bool expected = false;
    if (!c->send_inflight.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
      return;
    ZeroMemory(&c->wctx.ol, sizeof(c->wctx.ol));
    DWORD sent = 0;
    int rc = WSASend(c->s, c->tx, c->tx_count, &sent, 0, &c->wctx.ol, nullptr);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      c->send_inflight.store(false, std::memory_order_release);
      ::closesocket(c->s);
      c->closing = true;
    }
  }

  void run()
  {
    // Stable pool (no reallocation after posting)
    accept_pool_.reserve(accepts_per_worker_);
    for (uint16_t i = 0; i < accepts_per_worker_; ++i)
    {
      accept_pool_.emplace_back(std::make_unique<AcceptCtx>());
      post_accept(accept_pool_.back().get());
    }

    for (;;)
    {
      DWORD bytes = 0;
      ULONG_PTR key = 0;
      OVERLAPPED* pol = nullptr;
      BOOL ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &pol, 1000);

      if (!pol)
      {
        // 1s tick: sweep idle
        table_.sweep_idle(idle_timeout_, [&](ConnHandle h) {
          if (auto* c = table_.try_get(h))
          {
            c->closing = true;
            table_.close_and_recycle(h);
          }
        });
        continue;
      }

      IoHeader* hdr = CONTAINING_RECORD(pol, IoHeader, ol);
      switch (hdr->op)
      {
      case Op::Accept: {
        auto* ac = CONTAINING_RECORD(hdr, AcceptCtx, hdr);
        SOCKET s = ac->acceptSock;

        if (!ok)
        {
          if (s != INVALID_SOCKET)
            ::closesocket(s);
          post_accept(ac);
          break;
        }

        if (setsockopt(s, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*) &listen_, sizeof(listen_)) == SOCKET_ERROR)
        {
          ::closesocket(s);
          post_accept(ac);
          break;
        }

        ConnHandle h = table_.allocate(s);
        if (!CreateIoCompletionPort((HANDLE) s, iocp_, pack_key(h), 0))
        {
          table_.close_and_recycle(h);
          post_accept(ac);
          break;
        }

        BOOL nd = TRUE;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*) &nd, sizeof(nd));

        if (auto* c = table_.try_get(h))
        {
          c->headers_ready = false;
          start_recv(c);
        }

        post_accept(ac);
      }
      break;

      case Op::Recv: {
        ConnHandle h = unpack_key(key);
        auto* c = table_.try_get(h);
        if (!c)
          break;

        c->recv_inflight.store(false, std::memory_order_release);
        if (!ok || bytes == 0)
        {
          table_.close_and_recycle(h);
          break;
        }
        if (bytes > RX_CAP - c->rx_len)
        {
          table_.close_and_recycle(h);
          break;
        }

        c->rx_len += bytes;
        c->mark_activity();

        // streaming parser: only sets headers_ready flag
        c->http.on_data(std::string_view(c->rx, c->rx_len));

        if (c->headers_ready)
        {
          bool keep = (c->served < (keepalive_limit_ - 1));
          c->queue_response(keep);
          start_send(c); // single WSASend per reply
        }
        else
        {
          if (!c->closing && c->rx_len < RX_CAP)
            start_recv(c);
        }
      }
      break;

      case Op::Send: {
        ConnHandle h = unpack_key(key);
        auto* c = table_.try_get(h);
        if (!c)
          break;

        c->send_inflight.store(false, std::memory_order_release);
        if (!ok)
        {
          table_.close_and_recycle(h);
          break;
        }

        c->served++;
        if (c->served < keepalive_limit_ && !c->closing)
        {
          c->reset_for_next_request(); // <— critical fix
          start_recv(c);
        }
        else
        {
          table_.close_and_recycle(h);
        }
      }
      break;
      }
    }
  }
};

// ===== RequestContext implementation ==================================
void RequestContext::on_data(std::string_view chunk)
{
  // Minimal demo: mark headers_ready when \r\n\r\n is seen.
  if (!headers_complete && has_double_crlf(chunk.data(), chunk.size()))
  {
    headers_complete = true;
    owner->headers_ready = true;
  }
}

void RequestContext::produce_hello(bool keep_alive)
{
  owner->res->protocol = "HTTP/1.1";
  owner->res->code = "200 OK";
  owner->res->headers.clear();
  owner->res->headers["Content-Type"] = "text/plain";
  owner->res->headers["Content-Length"] = "14";
  owner->res->headers["Connection"] = keep_alive ? "keep-alive" : "close";
  owner->queue_response(keep_alive);
}

// ===================== Server scaffolding =============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = (uint16_t) std::thread::hardware_concurrency();
  uint16_t pending_accepts_per_worker = 32;
  uint16_t max_keepalive_requests = 100;
  uint32_t table_capacity = 1u << 15; // 32768 slots
  uint16_t idle_seconds = 100;
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
    listen_ = WinSockWrapper::make_listen_socket(cfg_.port);
    WinSockWrapper::load_extensions(listen_);

    iocp_ = CreateIoCompletionPort((HANDLE) listen_, nullptr, 0, 0);
    if (!iocp_)
      WinSockWrapper::die("CreateIoCompletionPort(listener)");

    if (cfg_.threads == 0)
      cfg_.threads = 1;
    workers_.reserve(cfg_.threads);
    threads_.reserve(cfg_.threads);
    for (uint16_t i = 0; i < cfg_.threads; i++)
    {
      workers_.emplace_back(iocp_, listen_, table_, cfg_.pending_accepts_per_worker, cfg_.max_keepalive_requests,
                            seconds(cfg_.idle_seconds));
      threads_.emplace_back(std::ref(workers_.back()));
    }

    for (auto& t : threads_)
      t.join();
  }

private:
  ServerConfig cfg_;
  SOCKET listen_{INVALID_SOCKET};
  HANDLE iocp_{nullptr};
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
