#pragma once
#include <atomic>
#include <cstddef>
#include <optional>
#include <type_traits>

template <typename T, std::size_t Capacity> class MPMCQueue
{
public:
  static_assert(Capacity && ((Capacity & (Capacity - 1)) == 0), "Capacity must be power of two");

  bool try_push(const T& item);
  bool try_push(T&& item);

  std::optional<T> try_pop();

private:
  struct Slot
  {
    std::atomic<std::size_t> sequence;
    std::aligned_storage_t<sizeof(T), alignof(T)> storage;
  };

  static constexpr std::size_t MASK = Capacity - 1;

  alignas(64) Slot buffer_[Capacity];

  alignas(64) std::atomic<std::size_t> head_{0};
  alignas(64) std::atomic<std::size_t> tail_{0};
};

#include <new>

template <typename T, std::size_t Capacity> bool MPMCQueue<T, Capacity>::try_push(const T& item)
{
  std::size_t pos = tail_.load(std::memory_order_relaxed);
  for (;;)
  {
    Slot& slot = buffer_[pos & MASK];
    std::size_t seq = slot.sequence.load(std::memory_order_acquire);
    intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
    if (diff == 0)
    {
      if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
      {
        new (&slot.storage) T(item);
        slot.sequence.store(pos + 1, std::memory_order_release);
        return true;
      }
    }
    else if (diff < 0)
    {
      return false; // Full
    }
    else
    {
      pos = tail_.load(std::memory_order_relaxed);
    }
  }
}

template <typename T, std::size_t Capacity> bool MPMCQueue<T, Capacity>::try_push(T&& item)
{
  std::size_t pos = tail_.load(std::memory_order_relaxed);
  for (;;)
  {
    Slot& slot = buffer_[pos & MASK];
    std::size_t seq = slot.sequence.load(std::memory_order_acquire);
    intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
    if (diff == 0)
    {
      if (tail_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
      {
        new (&slot.storage) T(std::move(item));
        slot.sequence.store(pos + 1, std::memory_order_release);
        return true;
      }
    }
    else if (diff < 0)
    {
      return false;
    }
    else
    {
      pos = tail_.load(std::memory_order_relaxed);
    }
  }
}

template <typename T, std::size_t Capacity> std::optional<T> MPMCQueue<T, Capacity>::try_pop()
{
  std::size_t pos = head_.load(std::memory_order_relaxed);
  for (;;)
  {
    Slot& slot = buffer_[pos & MASK];
    std::size_t seq = slot.sequence.load(std::memory_order_acquire);
    intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
    if (diff == 0)
    {
      if (head_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
      {
        T* val_ptr = reinterpret_cast<T*>(&slot.storage);
        T val = std::move(*val_ptr);
        val_ptr->~T();
        slot.sequence.store(pos + Capacity, std::memory_order_release);
        return val;
      }
    }
    else if (diff < 0)
    {
      return std::nullopt; // Empty
    }
    else
    {
      pos = head_.load(std::memory_order_relaxed);
    }
  }
}
