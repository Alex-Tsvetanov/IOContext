#pragma once
#include <string>
#include "xhttp/net.hpp"
#include "xhttp/buffer.hpp"

namespace xhttp
{

  enum class ConnState
  {
    Reading,
    Writing,
    Idle,
    Closing
  };

  struct Connection
  {
    xhttp_socket_t fd{XHTTP_INVALID_SOCKET};
    ConnState state{ConnState::Reading};
    Buffer in{8192};
    OutQueue out;
    bool keep_alive{true};
    int requests_made{0};
    std::string header_acc; // accumulate until \r\n\r\n
    uint64_t idle_timer_id{0};
    void* platform{nullptr}; // reserved (for IOCP-specific storage if needed)
  };

} // namespace xhttp
