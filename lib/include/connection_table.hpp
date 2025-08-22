// Connection table - platform independent template
#include <atomic>
#include <vector>
#include <chrono>
#include <functional>
#include "connection_handle.hpp"

using namespace std::chrono;

// Forward declaration for platform-specific client storage
struct PerClientStorage;

struct ConnSlot
{
  std::atomic<uint32_t> generation{1};
  std::atomic<bool> in_use{false};
  PerClientStorage client{};
};

class ConnTable
{
public:
  explicit ConnTable(size_t cap)
    : slots_(cap)
  {}

  virtual ~ConnTable() = default;

  // Platform-specific allocation
  virtual ConnHandle allocate() = 0;

  // Platform-independent methods
  PerClientStorage* try_get(ConnHandle h)
  {
    if (h.index >= slots_.size())
      return nullptr;
    auto& slot = slots_[h.index];
    if (!slot.in_use.load(std::memory_order_acquire))
      return nullptr;
    if (slot.generation.load(std::memory_order_acquire) != h.generation)
      return nullptr;
    return &slot.client;
  }

  virtual void close_and_recycle(ConnHandle h) = 0;

  template <class Fn> void sweep_idle(seconds idle, Fn&& on_close)
  {
    auto now = steady_clock::now();
    for (uint32_t i = 0; i < slots_.size(); ++i)
    {
      auto& slot = slots_[i];
      if (!slot.in_use.load(std::memory_order_acquire))
        continue;
      auto* c = &slot.client;
      if (now - c->last_active > idle)
      {
        on_close(ConnHandle{i, slot.generation.load(std::memory_order_relaxed)});
      }
    }
  }

protected:
  std::vector<ConnSlot> slots_;
};