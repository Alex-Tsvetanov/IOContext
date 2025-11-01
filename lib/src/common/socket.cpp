#include "common/socket.hpp"
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

int set_nonblock(fd_t fd) noexcept
{
  int f = fcntl(fd, F_GETFL, 0);
  if (f < 0)
  {
    return -1;
  }
  return fcntl(fd, F_SETFL, f | O_NONBLOCK);
}
void set_tcp_opts(fd_t fd) noexcept
{
  int nd = 1;
  (void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nd, sizeof(nd));
#ifdef TCP_QUICKACK
  int qa = 1;
  (void) setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &qa, sizeof(qa));
#endif
  int sndbuf = 256 * 1024;
  (void) setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
}

fd_t make_listener(uint16_t port, bool reuseport) noexcept
{
  fd_t s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s < 0)
  {
    std::perror("socket");
    std::exit(1);
  }
  int on = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (reuseport)
  {
    setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
  }

  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = INADDR_ANY;
  a.sin_port = htons(port);
  if (bind(s, (sockaddr*) &a, sizeof(a)) < 0)
  {
    std::perror("bind");
    std::exit(1);
  }
  if (listen(s, BACKLOG) < 0)
  {
    std::perror("listen");
    std::exit(1);
  }
  set_nonblock(s);
  return s;
}
