// Stable handle (index,generation) - platform independent
#include <cstdint>

struct ConnHandle
{
  uint32_t index{0}, generation{0};
};

// Platform-specific key packing/unpacking functions
#ifdef _WIN32
#include <cstddef>
static_assert(sizeof(ULONG_PTR) >= 8, "Build x64 so IOCP key can pack handle.");

static inline ULONG_PTR pack_key(ConnHandle h);
static inline ConnHandle unpack_key(ULONG_PTR k);
#else
// For EPOLL, we might use different key types
static inline uintptr_t pack_key(ConnHandle h);
static inline ConnHandle unpack_key(uintptr_t k);
#endif