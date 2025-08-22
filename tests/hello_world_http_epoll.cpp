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

    ZeroMemory(&rctx.ol, sizeof(rctx.ol));
    rctx.op = Op::Recv;
    rctx.socket_fd = socket_fd;
    ZeroMemory(&wctx.ol, sizeof wctx.ol);
    wctx.op = Op::Send;
    wctx.socket_fd = socket_fd;
    ZeroMemory(&proc.ol, sizeof proc.ol);
    proc.op = Op::Process;
    proc.socket_fd = socket_fd;
    ZeroMemory(&resp.ol, sizeof resp.ol);
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