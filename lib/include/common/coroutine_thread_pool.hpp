#pragma once

#include <coroutine>
#include <condition_variable>
#include <atomic>
#include <deque>
#include <exception>
#include <list>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>
#include <variant>

// Forward-declare
struct ThreadPool;

//--------------------------------------------------------------
// Task: a void-returning coroutine that runs on a ThreadPool
//--------------------------------------------------------------
struct Task {
  struct promise_type {
    void* udata;
    std::weak_ptr<ThreadPool> pool; // set by ThreadPool::spawn()

    Task get_return_object() noexcept;
    std::suspend_always initial_suspend() noexcept { return {}; } // start suspended
    std::suspend_always final_suspend() noexcept { return {}; }   // worker will destroy
    void return_void() noexcept {}
    void unhandled_exception() { std::terminate(); }

    // Enable `co_yield std::monostate{};` to reschedule on the pool
    std::suspend_always yield_value(std::monostate) noexcept;
  };

  using handle_t = std::coroutine_handle<promise_type>;

  Task() noexcept : h_(nullptr) {}
  explicit Task(handle_t h) noexcept : h_(h) {}

  Task(Task&& other) noexcept : h_(std::exchange(other.h_, nullptr)) {}
  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      h_ = std::exchange(other.h_, nullptr);
    }
    return *this;
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task() { if (h_) h_.destroy(); }

  handle_t handle() const noexcept { return h_; }

private:
  handle_t h_;
};

inline Task Task::promise_type::get_return_object() noexcept {
  return Task{ Task::handle_t::from_promise(*this) };
}

//--------------------------------------------------------------
// ThreadPool: schedules coroutine handles
//--------------------------------------------------------------
struct ThreadPool : public std::enable_shared_from_this<ThreadPool> {
  using coro_handle = std::coroutine_handle<Task::promise_type>;

  explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency())
  : stop_(false), pending_(0) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (std::size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this]{ worker_loop(); });
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool() {
    // graceful shutdown
    {
      std::unique_lock lk(m_);
      stop_ = true;
      cv_.notify_all();
    }
    for (auto& t : workers_) t.join();

    // Drain any left-over (shouldn’t happen if wait_idle() was used)
    while (true) {
      coro_handle h;
      {
        std::lock_guard lk(m_);
        if (q_.empty()) break;
        h = q_.front(); q_.pop_front();
      }
      if (h) {
        if (!h.done()) { h.resume(); }
        if (h && h.done()) h.destroy();
      }
    }
  }

  // Spawn a new Task on this pool.
  void spawn(Task&& t) {
    auto h = t.handle();
    t = Task{}; // release ownership; pool will own/destroy
    if (!h) return;

    {
      std::lock_guard lk(m_);
      ++pending_;
      // Annotate promise with this pool so co_yield can re-enqueue
      h.promise().pool = this->weak_from_this();
      q_.push_back(h);
    }
    cv_.notify_one();
  }

  // Wait until all spawned tasks have completed and the queue is empty.
  void wait_idle() {
    std::unique_lock lk(m_);
    cv_idle_.wait(lk, [this]{
      return pending_ == 0 && q_.empty();
    });
  }

private:
  friend struct Task::promise_type;

  void enqueue_(coro_handle h) noexcept {
    std::lock_guard lk(m_);
    q_.push_back(h);
    cv_.notify_one();
  }

  void worker_loop() {
    for (;;) {
      coro_handle h;
      {
        std::unique_lock lk(m_);
        cv_.wait(lk, [this]{ return stop_ || !q_.empty(); });
        if (stop_ && q_.empty()) break;
        h = q_.front(); q_.pop_front();
      }

      if (h) {
        h.resume();

        if (h.done()) {
          h.destroy();
          std::lock_guard lk(m_);
          if (--pending_ == 0 && q_.empty()) {
            cv_idle_.notify_all();
          }
        }
        // else: coroutine suspended (likely via co_yield); it already re-enqueued itself
      }
    }
  }

  // Shared state
  mutable std::mutex m_;
  std::condition_variable cv_;
  std::condition_variable cv_idle_;
  std::list<coro_handle> q_;
  std::vector<std::thread> workers_;

  bool stop_;
  std::atomic<std::size_t> pending_;
};
