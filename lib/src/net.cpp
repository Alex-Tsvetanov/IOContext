#include "xhttp/net.hpp"
#include <cstring>
#include <cerrno>

#if defined(_WIN32)
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "Ws2_32.lib")
#else
    #include <sys/types.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <netinet/in.h>
    #include <netdb.h>
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace xhttp
{
    bool net_init()
    {
#if defined(_WIN32)
        WSADATA wsaData;
        return WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
#else
        return true; // No initialization needed on Unix-like systems
#endif
    }

    void net_cleanup()
    {
#if defined(_WIN32)
        WSACleanup();
#endif
    }

    bool set_nonblocking(xhttp_socket_t s)
    {
#if defined(_WIN32)
        u_long mode = 1;
        return ioctlsocket(s, FIONBIO, &mode) == 0;
#else
        int flags = fcntl(s, F_GETFL, 0);
        if (flags == -1) return false;
        return fcntl(s, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
    }

    bool set_reuseaddr(xhttp_socket_t s)
    {
        int opt = 1;
        return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0;
    }

    bool set_reuseport(xhttp_socket_t s)
    {
#if defined(_WIN32)
        return true; // Not available on Windows
#else
        int opt = 1;
        return setsockopt(s, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == 0;
#endif
    }

    bool set_nodelay(xhttp_socket_t s, bool on)
    {
        int opt = on ? 1 : 0;
        return setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0;
    }

    void closesock(xhttp_socket_t s)
    {
#if defined(_WIN32)
        closesocket(s);
#else
        close(s);
#endif
    }

    int last_socket_error()
    {
#if defined(_WIN32)
        return WSAGetLastError();
#else
        return errno;
#endif
    }
}