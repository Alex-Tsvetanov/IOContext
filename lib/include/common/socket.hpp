#ifndef SOCKET_H
#define SOCKET_H

#include "../common/default_config.hpp"
#include <liburing.h>
#include <liburing/io_uring.h>

#ifdef WIN32
#define _WIN32_WINNT 0x0600
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

using fd_t = SOCKET;
// struct BatchedSendData
//{
//   LPWSABUF lpBuffers;
//   DWORD dwBufferCount;
// };
#else
#include <bits/types/struct_iovec.h>
#include <sys/socket.h>
#include <netinet/in.h>

using fd_t = int;
// struct BatchedSendData
//{
//   struct iovec iov[MAX_IOV];
//   struct msghdr msg{};
// };
fd_t make_listener(uint16_t port, bool reuseport) noexcept;
int set_nonblock(fd_t fd) noexcept;
void set_tcp_opts(fd_t fd) noexcept;
io_uring_sqe* get_sqe_or_submit(io_uring& ring) noexcept;
#endif

#endif // SOCKET_H