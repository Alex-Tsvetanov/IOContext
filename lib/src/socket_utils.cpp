#include "xhttp/net.hpp"
#include <cstring>

namespace xhttp {

bool net_init() {
#if defined(_WIN32)
    WSADATA wsa{};
    return WSAStartup(MAKEWORD(2,2), &wsa) == 0;
#else
    return true;
#endif
}

void net_cleanup() {
#if defined(_WIN32)
    WSACleanup();
#endif
}

bool set_nonblocking(xhttp_socket_t s) {
#if defined(_WIN32)
    u_long v = 1;
    return ioctlsocket(s, FIONBIO, &v) == 0;
#else
    int flags = fcntl(s, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(s, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

bool set_reuseaddr(xhttp_socket_t s) {
    int v = 1;
#if defined(_WIN32)
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (const char*)&v, sizeof(v)) == 0;
#else
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &v, sizeof(v)) == 0;
#endif
}

bool set_reuseport(xhttp_socket_t s) {
#if defined(_WIN32)
    return set_reuseaddr(s); // Windows lacks Linux-style SO_REUSEPORT
#else
    int v = 1;
    return setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &v, sizeof(v)) == 0;
#endif
}

bool set_nodelay(xhttp_socket_t s, bool on) {
    int v = on ? 1 : 0;
#if defined(_WIN32)
    return setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char*)&v, sizeof(v)) == 0;
#else
    return setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &v, sizeof(v)) == 0;
#endif
}

void closesock(xhttp_socket_t s) {
#if defined(_WIN32)
    if (s != INVALID_SOCKET) ::closesocket(s);
#else
    if (s >= 0) ::close(s);
#endif
}

int last_socket_error() {
#if defined(_WIN32)
    return WSAGetLastError();
#else
    return errno;
#endif
}

} // namespace xhttp
