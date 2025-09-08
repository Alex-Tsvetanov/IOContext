#ifndef VECTOR_H
#define VECTOR_H

#include <vector>
#include <memory>
#include <mutex>

template <typename T> class TSVector
{
  std::vector<std::shared_ptr<T>> vec_;
  std::mutex mutex_;

public:
  TSVector() = default;
  TSVector(const TSVector&) = delete;
  TSVector& operator=(const TSVector&) = delete;
  TSVector(TSVector&& other) noexcept
  {
    std::unique_lock<std::mutex> lock1(mutex_, std::defer_lock);
    std::unique_lock<std::mutex> lock2(other.mutex_, std::defer_lock);
    std::lock(lock1, lock2);
    vec_ = std::move(other.vec_);
  }
  TSVector& operator=(TSVector&& other) noexcept
  {
    if (this != &other)
    {
      std::unique_lock<std::mutex> lock1(mutex_, std::defer_lock);
      std::unique_lock<std::mutex> lock2(other.mutex_, std::defer_lock);
      std::lock(lock1, lock2);
      vec_ = std::move(other.vec_);
    }
    return *this;
  }

  void clear()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    vec_.clear();
  }

  void push(const T& value)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    vec_.push_back(std::make_shared<T>(value));
  }

  void lock() { mutex_.lock(); }

  void unlock() { mutex_.unlock(); }
};

#endif // VECTOR_H