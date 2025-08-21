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

#ifndef CONTAINING_RECORD
#  include <cstddef>
#  define CONTAINING_RECORD(ptr, type, member)                                                                         \
    (reinterpret_cast<type*>(reinterpret_cast<char*>(ptr) - offsetof(type, member)))
#endif

// ===================== Tunables =====================================
static constexpr DWORD ADDR_LEN = sizeof(SOCKADDR_STORAGE) + 16;
static constexpr int RX_CAP = 8192;
static constexpr int MAX_WSABUF = 8;
static constexpr int DEFAULT_PORT = 8080;
// crank accepts to cushion 2k+ concurrent connects
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
  Kick,
  Process,
  Respond
};

struct IoHeader
{
  OVERLAPPED ol{}; // must be first
  Op op{};
  SOCKET s{INVALID_SOCKET};
};

// ===================== Stable handle (index,generation) ===============
struct ConnHandle
{
  uint32_t index{0}, generation{0};
};
static_assert(sizeof(ULONG_PTR) >= 8, "Build x64 so IOCP key can pack handle.");

static inline ULONG_PTR pack_key(ConnHandle h)
{
  return (ULONG_PTR) ((uint64_t(h.generation) << 32) | uint64_t(h.index));
}
static inline ConnHandle unpack_key(ULONG_PTR k)
{
  return ConnHandle{uint32_t(k & 0xFFFFFFFFu), uint32_t(k >> 32)};
}

// ===================== WinSock wrapper ================================
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

// ===================== Buffer wrapper =================================
struct OwnedBuf
{
  WSABUF wb{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false)
  {
    return {WSABUF{(ULONG) n, const_cast<char*>(p)}, eor, {}};
  }
  static OwnedBuf copy(std::string_view sv, bool eor = false)
  {
    auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
    return {WSABUF{(ULONG) buf->size(), buf->data()}, eor, buf};
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
  SOCKET s{INVALID_SOCKET};
  IoHeader rctx{}, wctx{}, kick{}, proc{}, resp{};

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
  bool kick_pending{false};
  bool send_inflight{false};
  int inflight_count{0};
  bool inflight_eor{false};
  WSABUF tx[MAX_WSABUF];

  // cross-refs
  HANDLE iocp{nullptr};
  ConnHandle self{};

  void clear()
  {
    s = INVALID_SOCKET;
    std::memset(&rctx, 0, sizeof(rctx));
    std::memset(&wctx, 0, sizeof(wctx));
    std::memset(&kick, 0, sizeof(kick));
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
      kick_pending = false;
      send_inflight = false;
      inflight_count = 0;
      inflight_eor = false;
    }

    iocp = nullptr;
    self = {};
  }

  void init(SOCKET sock, HANDLE port, ConnHandle h)
  {
    clear();
    s = sock;
    iocp = port;
    self = h;

    ZeroMemory(&rctx.ol, sizeof rctx.ol);
    rctx.op = Op::Recv;
    rctx.s = s;
    ZeroMemory(&wctx.ol, sizeof wctx.ol);
    wctx.op = Op::Send;
    wctx.s = s;
    ZeroMemory(&kick.ol, sizeof kick.ol);
    kick.op = Op::Kick;
    kick.s = s;
    ZeroMemory(&proc.ol, sizeof proc.ol);
    proc.op = Op::Process;
    proc.s = s;
    ZeroMemory(&resp.ol, sizeof resp.ol);
    resp.op = Op::Respond;
    resp.s = s;

    last_active = steady_clock::now();
  }

  void mark_activity() { last_active = steady_clock::now(); }

  // ==== TX publishing (thread-safe) ====
  void tx_enqueue_literal(const char* p, size_t n, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::literal(p, n, eor));
    if (!kick_pending)
    {
      kick_pending = true;
      PostQueuedCompletionStatus(iocp, 0, pack_key(self), &kick.ol);
    }
  }
  void tx_enqueue_copy(std::string_view sv, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::copy(sv, eor));
    if (!kick_pending)
    {
      kick_pending = true;
      PostQueuedCompletionStatus(iocp, 0, pack_key(self), &kick.ol);
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
      PostQueuedCompletionStatus(iocp, 0, pack_key(self), &resp.ol);
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

  ConnHandle allocate(SOCKET)
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
      ::Sleep(1);
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

      if (c.s != INVALID_SOCKET)
      {
        ::closesocket(c.s); // cancels pending I/O; completions still arrive
        c.s = INVALID_SOCKET;
      }

      // Leave txq intact if a send might still be in flight; it will be cleared at next init().
      {
        std::lock_guard lk(c.tx_mtx);
        c.kick_pending = false;
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
  IoHeader hdr; // op=Accept
  SOCKET acceptSock{INVALID_SOCKET};
  char addrbuf[ADDR_LEN * 2];
};

// ===================== Helpers ========================================
static inline void start_recv(PerClientStorage* c)
{
  auto hold = std::make_shared<std::vector<char>>(RX_CAP);

  ZeroMemory(&c->rctx.ol, sizeof(c->rctx.ol));
  c->rctx.op = Op::Recv;
  c->rctx.s = c->s;

  {
    std::lock_guard lk(c->rx_mtx);
    c->rx_hold = hold;
  }

  WSABUF b{(ULONG) hold->size(), hold->data()};
  DWORD flags = 0, recvd = 0;
  int rc = WSARecv(c->s, &b, 1, &recvd, &flags, &c->rctx.ol, nullptr);
  if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
  {
    ::closesocket(c->s);
    c->closing = true;
  }
}

// ===================== Worker =========================================
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

  void maybe_start_send_worker(PerClientStorage* c)
  {
    int tx_count = 0;
    bool eor = false;

    {
      std::lock_guard lk(c->tx_mtx);
      c->kick_pending = false;
      if (c->send_inflight || c->txq.empty())
        return;

      for (auto it = c->txq.begin(); it != c->txq.end() && tx_count < MAX_WSABUF; ++it, ++tx_count)
      {
        c->tx[tx_count] = it->wb;
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

    ZeroMemory(&c->wctx.ol, sizeof(c->wctx.ol));
    c->wctx.op = Op::Send;
    c->wctx.s = c->s;

    DWORD sent = 0;
    int rc = WSASend(c->s, c->tx, tx_count, &sent, 0, &c->wctx.ol, nullptr);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      std::lock_guard lk(c->tx_mtx);
      c->send_inflight = false;
      ::closesocket(c->s);
      c->closing = true;
    }
  }

  void run()
  {
    // Accept pool
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
      ConnHandle h = unpack_key(key);

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

        ConnHandle nh = table_.allocate(s);
        if (!CreateIoCompletionPort((HANDLE) s, iocp_, pack_key(nh), 0))
        {
          table_.close_and_recycle(nh);
          post_accept(ac);
          break;
        }
        BOOL nd = TRUE;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*) &nd, sizeof(nd));

        if (auto* c = table_.try_get(nh))
        {
          c->init(s, iocp_, nh);
          c->mark_activity();
          start_recv(c);
        }
        post_accept(ac);
      }
      break;

      case Op::Recv: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        if (!ok || bytes == 0)
        {
          table_.close_and_recycle(h);
          break;
        }
        c->mark_activity();

        std::shared_ptr<std::vector<char>> hold;
        {
          std::lock_guard lk(c->rx_mtx);
          hold = c->rx_hold;
          c->rx_hold.reset();
          if (hold)
            c->rxq.emplace_back(OwnedBuf{WSABUF{(ULONG) bytes, hold->data()}, false, hold});
          if (!c->parse_inflight)
          {
            c->parse_inflight = true;
            PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->proc.ol);
          }
        }

        if (!c->closing)
          start_recv(c);
      }
      break;

      case Op::Process: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

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
            c->http.on_segment(seg.wb.buf, seg.wb.len);
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
          PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->proc.ol);
      }
      break;

      case Op::Respond: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

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
            PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->resp.ol);
          }
        }
      }
      break;

      case Op::Kick: {
        auto* c = table_.try_get(h);
        if (!c)
          break;
        maybe_start_send_worker(c);
      }
      break;

      case Op::Send: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        if (!ok)
        {
          table_.close_and_recycle(h);
          break;
        }

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
            if (!c->kick_pending)
            {
              c->kick_pending = true;
              PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->kick.ol);
            }
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
      break;
      } // switch
    } // loop
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
    listen_ = WinSockWrapper::make_listen_socket(cfg_.port);
    WinSockWrapper::load_extensions(listen_);

    iocp_ = CreateIoCompletionPort((HANDLE) listen_, nullptr, 0, 0);
    if (!iocp_)
      WinSockWrapper::die("CreateIoCompletionPort(listener)");

    if (cfg_.threads == 0)
      cfg_.threads = 1;
    workers_.reserve(cfg_.threads);
    threads_.reserve(cfg_.threads);
    for (uint16_t i = 0; i < cfg_.threads; ++i)
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
