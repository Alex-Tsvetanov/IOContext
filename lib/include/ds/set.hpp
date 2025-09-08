#ifndef SET_H
#define SET_H

#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_set>

template <typename T> class TSSet
{
  std::unordered_set<std::shared_ptr<T>> set_;
  std::atomic_flag has_elements = ATOMIC_FLAG_INIT;
  std::mutex mutex_;

public:
  using element = std::shared_ptr<T>;

  TSSet() = default;

  TSSet(const TSSet&) = delete;
  TSSet& operator=(const TSSet&) = delete;
  TSSet(TSSet&& other) noexcept { this->operator=(std::move(other)); }
  TSSet& operator=(TSSet&& other) noexcept
  {
    if (this != &other)
    {
      std::unique_lock<std::mutex> lock1(mutex_, std::defer_lock);
      std::unique_lock<std::mutex> lock2(other.mutex_, std::defer_lock);
      std::lock(lock1, lock2); // Lock both mutexes without deadlock risk
      set_ = std::move(other.set_);
      if (set_.empty())
      {
        has_elements.clear();
      }
      other.has_elements.clear();
    }
    return *this;
  }

  void clear()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : set_)
    {
      ts_std::cout << "Clearing element from TSSet<" << typeid(T).name() << ">: " << item << " " << item.use_count()
                   << std::endl;
    }
    set_.clear();
    has_elements.clear();
  }

  void add(std::shared_ptr<T> value)
  {
#ifdef DEBUG
    ts_std::cout << "Adding element to TSSet<" << typeid(T).name() << ">: " << value << " " << value.use_count()
                 << std::endl;
#endif // DEBUG
    std::lock_guard<std::mutex> lock(mutex_);
    auto status = set_.insert(value);
#ifdef DEBUG
    if (!status.second)
    {
      ts_std::cout << "Element already exists in TSSet: " << value << std::endl;
      throw std::runtime_error("Element already exists in TSSet");
    }
#endif // DEBUG
    has_elements.test_and_set();
  }

  bool contains(std::shared_ptr<T> value)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return set_.contains(value);
  }

  void remove(std::shared_ptr<T> value)
  {
#ifdef DEBUG
    ts_std::cout << "Removing element from TSSet<" << typeid(T).name() << ">: " << value << " " << value.use_count()
                 << std::endl;
#endif
    std::lock_guard<std::mutex> lock(mutex_);
    const size_t deleted = set_.erase(value);
#ifdef DEBUG
    if (deleted == 0)
    {
      throw std::runtime_error("Element not found in TSSet");
    }
#endif
    if (set_.empty())
    {
      has_elements.clear();
    }
#ifdef DEBUG
    ts_std::cout << "Elements remaining: " << typeid(T).name() << " " << set_.size() << std::endl;
#endif
  }

  size_t size()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return set_.size();
  }

#ifdef DEBUG
  void print()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ts_std::cout << "TSSet<" << typeid(T).name() << "> contents:\n";
    for (const auto& item : set_)
    {
      ts_std::cout << "  " << item << " " << item.use_count() << "\n";
    }
    ts_std::cout << std::endl;
  }
#endif

  void lock() { mutex_.lock(); }

  void unlock() { mutex_.unlock(); }

  const std::unordered_set<std::shared_ptr<T>>& unsafe_get_set() const { return set_; }

  bool empty() const { return !has_elements.test(std::memory_order_relaxed); }
};

#endif // SET_H