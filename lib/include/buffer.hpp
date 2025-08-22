// Buffer wrapper - platform independent
#include <memory>
#include <string_view>

// Forward declaration for platform-specific buffer types
#ifdef _WIN32
struct WSABUF;
using PlatformBuffer = WSABUF;
#else
struct iovec;
using PlatformBuffer = iovec;
#endif

struct OwnedBuf
{
  PlatformBuffer wb{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false);
  static OwnedBuf copy(std::string_view sv, bool eor = false);
};