// Windows IOCP helper functions
#include "iocp/iocp_helpers.hpp"
#include "iocp/iocp_client_storage.hpp"

#ifdef _WIN32

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

#endif