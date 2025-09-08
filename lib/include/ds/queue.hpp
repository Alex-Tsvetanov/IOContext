#ifndef QUEUE_H
#define QUEUE_H

#include <queue>
#include <memory>
#include <mutex>

template <typename T> class TSQueue
{
  std::queue<std::shared_ptr<T>> queue_;
  std::atomic_flag has_elements = ATOMIC_FLAG_INIT;
  std::mutex mutex_;

public:
  TSQueue() = default;

  void clear()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.clear();
    has_elements.clear();
  }

  void push(const std::shared_ptr<T>& value)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(value);
    has_elements.test_and_set();
  }

  std::shared_ptr<T> pop()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty())
    {
      return nullptr;
    }
    auto value = queue_.front();
    queue_.pop();
    if (queue_.empty())
    {
      has_elements.clear();
    }
    return value;
  }

  bool empty() const { return !has_elements.test(); }
};

#endif // QUEUE_H