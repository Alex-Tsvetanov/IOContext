// Windows IOCP-specific client storage
#include "http.hpp"
#include "request_context.hpp"
#include "connection_handle.hpp"
#include "iocp/iocp_types.hpp"
#include <deque>
#include <mutex>
#include <memory>

#ifdef _WIN32

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

  void clear();
  void init(SOCKET sock, HANDLE port, ConnHandle h);
  void mark_activity();

  // ==== TX publishing (thread-safe) ====
  void tx_enqueue_literal(const char* p, size_t n, bool eor = false);
  void tx_enqueue_copy(std::string_view sv, bool eor = false);

  // ==== Respond scheduling (thread-safe) ====
  void enqueue_request_for_response(std::shared_ptr<Request> r);
};

#endif