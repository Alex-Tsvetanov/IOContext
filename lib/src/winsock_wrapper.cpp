// Windows socket wrapper implementation
#include "iocp/winsock_wrapper.hpp"

#ifdef _WIN32

WinSockWrapper::WinSockWrapper()
{
  WSADATA wsa{};
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
    die("WSAStartup failed");
}

WinSockWrapper::~WinSockWrapper()
{
  WSACleanup();
}

[[noreturn]] void WinSockWrapper::die(const char* msg)
{
  std::fprintf(stderr, "%s (WSA=%d)\n", msg, WSAGetLastError());
  std::exit(1);
}

void WinSockWrapper::load_extensions(SOCKET s)
{
  DWORD bytes = 0;
  GUID g1 = WSAID_ACCEPTEX;
  if (WSAIoctl(s, SIO_GET_EXTENSION_FUNCTION_POINTER, &g1, sizeof(g1), &AcceptExPtr, sizeof(AcceptExPtr), &bytes,
               nullptr, nullptr) == SOCKET_ERROR)
    die("WSAIoctl(WSAID_ACCEPTEX) failed");
}

SOCKET WinSockWrapper::make_listen_socket(uint16_t port)
{
  SOCKET s = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
  if (s == INVALID_SOCKET)
    die("WSASocket listen");
  int on = 1;
  setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*) &on, sizeof(on));
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = INADDR_ANY;
  a.sin_port = htons(port);
  if (bind(s, (sockaddr*) &a, sizeof(a)) == SOCKET_ERROR)
    die("bind");
  if (listen(s, SOMAXCONN) == SOCKET_ERROR)
    die("listen");
  return s;
}

WinSockWrapper wsa_;

#endif