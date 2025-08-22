// Windows IOCP-specific types and constants
#include <cstdint>

#ifdef _WIN32
#define _WIN32_WINNT 0x0600
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

// Platform-specific constants
static constexpr DWORD ADDR_LEN = sizeof(SOCKADDR_STORAGE) + 16;
static constexpr int MAX_WSABUF = 8;

// Platform-specific key packing implementations
static inline ULONG_PTR pack_key(ConnHandle h)
{
  return (ULONG_PTR) ((uint64_t(h.generation) << 32) | uint64_t(h.index));
}
static inline ConnHandle unpack_key(ULONG_PTR k)
{
  return ConnHandle{uint32_t(k & 0xFFFFFFFFu), uint32_t(k >> 32)};
}

// IOCP operation types
enum class Op : uint8_t
{
  Accept,
  Recv,
  Send,
  Kick,
  Process,
  Respond
};

// IOCP-specific IO header
struct IoHeader
{
  OVERLAPPED ol{}; // must be first
  Op op{};
  SOCKET s{INVALID_SOCKET};
};

// Platform-specific buffer implementation
struct OwnedBuf
{
  WSABUF wb{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false)
  {
    return {WSABUF{(ULONG) n, const_cast<char*>(p)}, eor, {}};
  }
  static OwnedBuf copy(std::string_view sv, bool eor = false)
  {
    auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
    return {WSABUF{(ULONG) buf->size(), buf->data()}, eor, buf};
  }
};

#endif