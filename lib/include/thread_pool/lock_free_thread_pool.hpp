#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include <functional>
#include <cstddef>
#include <optional>
#include <algorithm>
#include <type_traits>

#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h> // for _mm_pause (x86 only)
#endif

#include "mpmc_queue.hpp"

template <std::size_t ThreadCount, std::size_t QueueSize = 1024> class LockFreeThreadPool
{
public:
  using Task = std::function<void()>;

  LockFreeThreadPool();
  ~LockFreeThreadPool();

  bool try_post(Task task);

private:
  void worker_loop();

  std::atomic<bool> running_{true};
  MPMCQueue<Task, QueueSize> queue_;
  std::vector<std::thread> workers_;
};

template <std::size_t ThreadCount, std::size_t QueueSize>
LockFreeThreadPool<ThreadCount, QueueSize>::LockFreeThreadPool()
{
  workers_.reserve(ThreadCount);
  for (std::size_t i = 0; i < ThreadCount; ++i)
  {
    workers_.emplace_back([this] { worker_loop(); });
  }
}

template <std::size_t ThreadCount, std::size_t QueueSize>
LockFreeThreadPool<ThreadCount, QueueSize>::~LockFreeThreadPool()
{
  running_.store(false, std::memory_order_release);
  for (auto& t : workers_)
  {
    if (t.joinable())
      t.join();
  }
}

template <std::size_t ThreadCount, std::size_t QueueSize>
bool LockFreeThreadPool<ThreadCount, QueueSize>::try_post(Task task)
{
  return queue_.try_push(std::move(task));
}

template <std::size_t ThreadCount, std::size_t QueueSize> void LockFreeThreadPool<ThreadCount, QueueSize>::worker_loop()
{
  std::size_t spin = 1;
  while (running_.load(std::memory_order_acquire))
  {
    if (auto task = queue_.try_pop())
    {
      spin = 1;
      (*task)();
    }
    else
    {
      for (std::size_t i = 0; i < spin; ++i)
      {
#if defined(__x86_64__) || defined(_M_X64)
        _mm_pause();
#else
        std::this_thread::yield();
#endif
      }
      spin = std::min<size_t>(spin * 2, 1024ul);
    }
  }

  // Drain remaining tasks
  while (auto task = queue_.try_pop())
  {
    (*task)();
  }
}
