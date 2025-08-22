// Windows IOCP helper structures and functions
#include "iocp/iocp_types.hpp"

#ifdef _WIN32

// Accept context for IOCP
struct AcceptCtx
{
  IoHeader hdr; // op=Accept
  SOCKET acceptSock{INVALID_SOCKET};
  char addrbuf[ADDR_LEN * 2];
};

// Helper functions
static inline void start_recv(PerClientStorage* c);

// Constants
static constexpr int RX_CAP = 8192;
static constexpr int PROC_MAX_SEGMENTS = 16;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(250);

// Demo payload
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

#endif