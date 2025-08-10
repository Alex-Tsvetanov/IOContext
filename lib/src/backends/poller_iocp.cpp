#include "xhttp/poller.hpp"
#if defined(XHTTP_PLATFORM_WINDOWS)
  #define NOMINMAX
  #include <windows.h>
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #include <vector>
  #include <unordered_map>
  #include <cstring>

namespace xhttp
{

  struct ConnCtx
  {
    xhttp_socket_t fd{INVALID_SOCKET};
    WSABUF wbuf{};
    std::vector<char> buf;
    OVERLAPPED ol{};
    DWORD flags{0};
    ConnCtx()
      : buf(8192)
    {
      wbuf.buf = buf.data();
      wbuf.len = (ULONG) buf.size();
      std::memset(&ol, 0, sizeof(ol));
    }
  };

  class IocpPoller: public IPoller
  {
    HANDLE iocp_{nullptr};
    std::vector<PollEvent> out_;
    std::unordered_map<xhttp_socket_t, ConnCtx> ctx_;

  public:
    bool init() override
    {
      iocp_ = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
      return iocp_ != nullptr;
    }
    bool add(xhttp_socket_t fd, uint32_t, void* user) override
    {
      HANDLE h = CreateIoCompletionPort((HANDLE) fd, iocp_, (ULONG_PTR) user, 0);
      if (!h)
        return false;
      auto& c = ctx_[fd];
      c.fd = fd;
      c.flags = 0;
      std::memset(&c.ol, 0, sizeof(c.ol));
      DWORD recvd = 0;
      int rc = WSARecv(fd, &c.wbuf, 1, &recvd, &c.flags, &c.ol, NULL);
      if (rc == SOCKET_ERROR)
      {
        int e = WSAGetLastError();
        if (e != WSA_IO_PENDING)
          return false;
      }
      return true;
    }
    bool mod(xhttp_socket_t, uint32_t, void*) override { return true; } // interest toggling not needed
    void del(xhttp_socket_t fd) override { ctx_.erase(fd); }

    int wait(int timeout_ms) override
    {
      out_.clear();
      ULONG nremove = 64;
      std::vector<OVERLAPPED_ENTRY> entries(nremove);
      DWORD ms = (timeout_ms < 0) ? INFINITE : (DWORD) timeout_ms;
      ULONG n = 0;
      BOOL ok = GetQueuedCompletionStatusEx(iocp_, entries.data(), nremove, &n, ms, FALSE);
      if (!ok && GetLastError() != WAIT_TIMEOUT)
        return -1;
      for (ULONG i = 0; i < n; i++)
      {
        auto& e = entries[i];
        // Find context by overlapped pointer
        ConnCtx* found = nullptr;
        xhttp_socket_t fd = INVALID_SOCKET;
        for (auto& kv : ctx_)
        {
          if (&kv.second.ol == e.lpOverlapped)
          {
            found = &kv.second;
            fd = kv.first;
            break;
          }
        }
        PollEvent pe{};
        pe.fd = fd;
        pe.user = (void*) e.lpCompletionKey;
        pe.bytes = e.dwNumberOfBytesTransferred;
        pe.error_code = ok ? 0 : (int) GetLastError();
        if (!ok || pe.bytes == 0)
        {
          pe.events = XHTTP_EV_ERR | XHTTP_EV_RDHUP;
        }
        else
        {
          pe.events = XHTTP_EV_READ;
          pe.data = found ? found->buf.data() : nullptr;
          // repost receive
          if (found)
          {
            found->flags = 0;
            std::memset(&found->ol, 0, sizeof(found->ol));
            DWORD recvd = 0;
            int rc = WSARecv(found->fd, &found->wbuf, 1, &recvd, &found->flags, &found->ol, NULL);
            if (rc == SOCKET_ERROR)
            {
              int e2 = WSAGetLastError();
              if (e2 != WSA_IO_PENDING)
              { /* will surface later */
              }
            }
          }
        }
        out_.push_back(pe);
      }
      return (int) out_.size();
    }
    std::span<const PollEvent> events() const override { return out_; }
  };

  IPoller* make_poller()
  {
    return new IocpPoller();
  }

} // namespace xhttp
#endif
