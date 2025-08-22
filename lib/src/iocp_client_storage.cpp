// Windows IOCP client storage implementation
#include "iocp/iocp_client_storage.hpp"
#include "iocp/iocp_types.hpp"
#include <cstring>

#ifdef _WIN32

void PerClientStorage::clear()
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

void PerClientStorage::init(SOCKET sock, HANDLE port, ConnHandle h)
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

void PerClientStorage::mark_activity()
{
  last_active = steady_clock::now();
}

void PerClientStorage::tx_enqueue_literal(const char* p, size_t n, bool eor)
{
  std::lock_guard lk(tx_mtx);
  txq.emplace_back(OwnedBuf::literal(p, n, eor));
  if (!kick_pending)
  {
    kick_pending = true;
    PostQueuedCompletionStatus(iocp, 0, pack_key(self), &kick.ol);
  }
}

void PerClientStorage::tx_enqueue_copy(std::string_view sv, bool eor)
{
  std::lock_guard lk(tx_mtx);
  txq.emplace_back(OwnedBuf::copy(sv, eor));
  if (!kick_pending)
  {
    kick_pending = true;
    PostQueuedCompletionStatus(iocp, 0, pack_key(self), &kick.ol);
  }
}

void PerClientStorage::enqueue_request_for_response(std::shared_ptr<Request> r)
{
  std::lock_guard lr(resp_mtx);
  respq.emplace_back(std::move(r));
  if (!respond_inflight)
  {
    respond_inflight = true;
    PostQueuedCompletionStatus(iocp, 0, pack_key(self), &resp.ol);
  }
}

#endif