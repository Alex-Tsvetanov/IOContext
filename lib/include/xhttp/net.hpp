#pragma once
#include <cstdint>
#include <string>

#if defined(_WIN32)
  #define NOMINMAX
  #include <winsock2.h>
  #include <ws2tcpip.h>
  using xhttp_socket_t = SOCKET;
  inline constexpr xhttp_socket_t XHTTP_INVALID_SOCKET = INVALID_SOCKET;
#else
  #include <sys/types.h>
  #include <sys/socket.h>
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <netdb.h>
  #include <fcntl.h>
  #include <unistd.h>
  using xhttp_socket_t = int;
  inline constexpr xhttp_socket_t XHTTP_INVALID_SOCKET = -1;
#endif

namespace xhttp {

struct Endpoint { std::string address{"0.0.0.0"}; uint16_t port{8080}; };

bool net_init();
void net_cleanup();

bool set_nonblocking(xhttp_socket_t s);
bool set_reuseaddr(xhttp_socket_t s);
bool set_reuseport(xhttp_socket_t s);
bool set_nodelay(xhttp_socket_t s, bool on);

void closesock(xhttp_socket_t s);
int  last_socket_error();

} // namespace xhttp
