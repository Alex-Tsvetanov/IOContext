#pragma once
#include <cstdint>
#include <span>
#include <vector>
#include "xhttp/net.hpp"

namespace xhttp
{

  enum : uint32_t
  {
    XHTTP_EV_READ = 0x01,
    XHTTP_EV_WRITE = 0x02,
    XHTTP_EV_RDHUP = 0x04,
    XHTTP_EV_ERR = 0x08
  };

  struct PollEvent
  {
    xhttp_socket_t fd;
    uint32_t events;
    void* user;           // back-pointer (Connection* or nullptr for listener)
    std::size_t bytes{0}; // used by completion APIs
    int error_code{0};    // platform error (diagnostics)
    char* data{nullptr};  // optional (IOCP: points to bytes received)
  };

  class IPoller
  {
  public:
    virtual ~IPoller() = default;
    virtual bool init() = 0;
    virtual bool add(xhttp_socket_t fd, uint32_t events, void* user) = 0;
    virtual bool mod(xhttp_socket_t fd, uint32_t events, void* user) = 0;
    virtual void del(xhttp_socket_t fd) = 0;
    virtual int wait(int timeout_ms) = 0;
    virtual std::span<const PollEvent> events() const = 0;
  };

  IPoller* make_poller(); // factory implemented per-OS

} // namespace xhttp
