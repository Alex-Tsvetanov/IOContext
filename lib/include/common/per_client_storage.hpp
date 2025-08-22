#ifndef PER_CLIENT_STORAGE_H
#define PER_CLIENT_STORAGE_H

#include "request.hpp"
#include "response.hpp"
#include "request_context.hpp"
#include <mutex>
#include <deque>
#include <chrono>
using steady_clock = std::chrono::steady_clock;

#ifdef _WIN32
#  define _WIN32_WINNT 0x0600
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#  include <winsock2.h>
#  include <mswsock.h>
#  include <ws2tcpip.h>
#  pragma comment(lib, "Ws2_32.lib")
#  pragma comment(lib, "Mswsock.lib")

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
    truct PerClientStorage
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

      void mark_activity()
      {
        last_active = steady_clock::now();
      }

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
#else
#  include <sys/epoll.h>
#  include <sys/eventfd.h>
#  include <sys/socket.h>
#  include <sys/uio.h>
#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <netinet/tcp.h>
#  include <fcntl.h>
#  include <unistd.h>
#  include <errno.h>
#  include <signal.h>

using SOCKET = int;
inline constexpr SOCKET INVALID_SOCKET = -1;

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
  SOCKET epollfd{INVALID_SOCKET};
  SOCKET notifier{INVALID_SOCKET};
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

    epollfd = INVALID_SOCKET;
    notifier = INVALID_SOCKET;
    self = {};
  }

  void init(SOCKET sock, SOCKET _epollfd, SOCKET _notifier, ConnHandle h)
  {
    clear();
    s = sock;
    epollfd = _epollfd;
    notifier = _notifier;
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
#endif

#endif // PER_CLIENT_STORAGE_H