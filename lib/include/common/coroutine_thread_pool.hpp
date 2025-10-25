#pragma once
#include <coroutine>
#include <condition_variable>
#include <atomic>
#include <deque>
#include <exception>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

// Forward-declare
struct ThreadPool;

//--------------------------------------------------------------
// Task: a void-returning coroutine that runs on a ThreadPool
//--------------------------------------------------------------
struct Task
{
  struct promise_type
  {
    // ThreadPool* pool = nullptr;              // set by spawn()
    Task get_return_object() noexcept;
    std::suspend_always initial_suspend() noexcept { return {}; } // start suspended
    std::suspend_always final_suspend() noexcept { return {}; }   // worker will destroy
    void return_void() noexcept {}
    void unhandled_exception() { std::terminate(); }
  };

  using handle_t = std::coroutine_handle<promise_type>;

  Task() noexcept
    : h_(nullptr)
  {}
  explicit Task(handle_t h) noexcept
    : h_(h)
  {}

  Task(Task&& other) noexcept
    : h_(std::exchange(other.h_, nullptr))
  {}
  Task& operator=(Task&& other) noexcept
  {
    if (this != &other)
    {
      // if (h_) h_.destroy();
      h_ = std::exchange(other.h_, nullptr);
    }
    return *this;
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  ~Task()
  {
    if (h_)
      h_.destroy();
  }

  handle_t handle() const noexcept { return h_; }

private:
  handle_t h_;
};

inline Task Task::promise_type::get_return_object() noexcept
{
  return Task{Task::handle_t::from_promise(*this)};
}

//--------------------------------------------------------------
// ThreadPool: schedules coroutine handles
//--------------------------------------------------------------
struct ThreadPool
{
  using coro_handle = std::coroutine_handle<Task::promise_type>;

  explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency())
    : stop_(false)
    , drained_(false)
    , pending_(0)
  {
    if (threads == 0)
      threads = 1;
    workers_.reserve(threads);
    for (std::size_t i = 0; i < threads; ++i)
    {
      workers_.emplace_back([this] { worker_loop(); });
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool()
  {
    // graceful shutdown
    {
      std::unique_lock lk(m_);
      stop_ = true;
      cv_.notify_all();
    }
    for (auto& t : workers_)
      t.join();

    // Drain any left-over (shouldn’t happen if wait_idle() was used)
    while (true)
    {
      coro_handle h;
      {
        std::lock_guard lk(m_);
        if (q_.empty())
          break;
        h = q_.front();
        q_.pop_front();
      }
      if (h)
      {
        // If something remained, destroy safely
        if (!h.done())
        {
          h.resume();
        }
        if (h && h.done())
          h.destroy();
      }
    }
  }

  // Spawn a new Task on this pool.
  // This enqueues the coroutine handle without resuming yet.
  void spawn(Task&& t)
  {
    auto h = t.handle();
    t = Task{}; // release ownership; pool will own/destroy
    if (!h)
      return;

    {
      std::lock_guard lk(m_);
      ++pending_;
      // annotate promise with this pool, so yield can re-enqueue
      if (h.address())
      {
        [[maybe_unused]] auto& prom = h.promise();
        // prom.pool = this;
      }
      q_.push_back(h);
    }
    cv_.notify_one();
  }

  // Awaitable that re-enqueues the current coroutine once, then suspends.
  // Usage inside a Task: co_await pool.yield_once();
  struct YieldOnce
  {
    ThreadPool& pool;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<Task::promise_type> h) const noexcept
    {
      // Put back into the pool queue
      pool.enqueue_(h);
    }
    void await_resume() const noexcept {}
  };

  [[nodiscard]] YieldOnce yield_once() noexcept { return YieldOnce{*this}; }

  // Wait until all spawned tasks have completed and the queue is empty.
  void wait_idle()
  {
    std::unique_lock lk(m_);
    cv_idle_.wait(lk, [this] { return pending_ == 0 && q_.empty(); });
  }

private:
  friend struct Task::promise_type;

  void enqueue_(coro_handle h) noexcept
  {
    // NOTE: await_suspend can be called concurrently; guard push with mutex.
    std::lock_guard lk(m_);
    q_.push_back(h);
    cv_.notify_one();
  }

  void worker_loop()
  {
    for (;;)
    {
      coro_handle h;
      {
        std::unique_lock lk(m_);
        cv_.wait(lk, [this] { return stop_ || !q_.empty(); });
        if (stop_ && q_.empty())
          break;
        h = q_.front();
        q_.pop_front();
      }

      if (h)
      {
        h.resume();

        if (h.done())
        {
          // destroy & mark completion
          [[maybe_unused]] auto& promise = static_cast<Task::promise_type&>(h.promise());
          h.destroy();

          // pending_ corresponds to “live tasks”
          // Only decrement on true completion (final_suspend observed).
          std::lock_guard lk(m_);
          if (--pending_ == 0 && q_.empty())
          {
            cv_idle_.notify_all();
          }
        }
        // else: coroutine suspended; it re-enqueued itself via yield_once()
      }
    }
  }

  // Shared state
  mutable std::mutex m_;
  std::condition_variable cv_;
  std::condition_variable cv_idle_;
  std::deque<coro_handle> q_;
  std::vector<std::thread> workers_;

  bool stop_;
  [[maybe_unused]] bool drained_;
  std::atomic<std::size_t> pending_;
};
