#include <coroutine>
#include <condition_variable>
#include <atomic>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

struct Task;

class ThreadPool
{
public:
  explicit ThreadPool(std::size_t threads = std::thread::hardware_concurrency());
  void spawn(Task&& t);
  void wait_idle();

private:
  friend struct Task;
  struct ActualThreadPool;
  std::shared_ptr<ActualThreadPool> actual_pool_;
};

//--------------------------------------------------------------
// Task: a void-returning coroutine that runs on a ThreadPool
//--------------------------------------------------------------
struct Task
{
  struct promise_type
  {
    std::weak_ptr<ThreadPool::ActualThreadPool> pool; // set by ThreadPool::spawn
    Task get_return_object() noexcept;
    std::suspend_always initial_suspend() noexcept { return {}; } // start suspended
    std::suspend_always final_suspend() noexcept { return {}; }   // worker will destroy
    std::suspend_always yield_value(int) noexcept;
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

struct GetCurrentHandle
{
  std::coroutine_handle<Task::promise_type> handle_;

  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<Task::promise_type> h) noexcept
  {
    handle_ = h;
    return false; // resume immediately, don't actually suspend
  }
  std::coroutine_handle<Task::promise_type> await_resume() const noexcept { return handle_; }
};

[[nodiscard]] static inline GetCurrentHandle current_handle() noexcept
{
  return GetCurrentHandle{};
}

inline Task Task::promise_type::get_return_object() noexcept
{
  return Task{Task::handle_t::from_promise(*this)};
}

//--------------------------------------------------------------
// ThreadPool: schedules coroutine handles
//--------------------------------------------------------------
struct ThreadPool::ActualThreadPool: public std::enable_shared_from_this<ThreadPool::ActualThreadPool>
{
  using coro_handle = std::coroutine_handle<Task::promise_type>;

  explicit ActualThreadPool(std::size_t threads = std::thread::hardware_concurrency())
    : stop_(false)
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

  ActualThreadPool(const ActualThreadPool&) = delete;
  ActualThreadPool& operator=(const ActualThreadPool&) = delete;

  ~ActualThreadPool()
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
        auto& prom = h.promise();
        prom.pool = this->weak_from_this();
      }
      q_.push_back(h);
    }
    cv_.notify_one();
  }

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
  std::atomic<std::size_t> pending_;
};

std::suspend_always Task::promise_type::yield_value(int) noexcept
{
  // co_yield: just yield back to the pool once
  if (auto sp = pool.lock())
  {
    sp->enqueue_(std::coroutine_handle<promise_type>::from_promise(*this));
  }
  return {};
}

ThreadPool::ThreadPool(std::size_t threads)
  : actual_pool_(std::make_shared<ActualThreadPool>(threads))
{}

void ThreadPool::spawn(Task&& t)
{
  actual_pool_->spawn(std::move(t));
}

void ThreadPool::wait_idle()
{
  actual_pool_->wait_idle();
}

//--------------------------------------------------------------
// Example usage (put this in your .cpp to test)
//--------------------------------------------------------------
#include <iostream>
#include <sstream>

Task stepper(int id, int steps, int work_ms)
{
  // On spawn(), we start suspended and only run when a worker resumes us.
  for (int i = 0; i < steps; ++i)
  {
    auto handle = co_await current_handle();
    void* addr = handle.address();
    // "Do work"
    std::this_thread::sleep_for(std::chrono::milliseconds(work_ms));
    std::stringstream ss;
    ss << "[task " << id << "] step " << (i + 1) << "/" << steps << " coroutine address: " << addr << "\n";
    std::cout << ss.str();
    // Yield back to the pool so other work can run; we’ll be re-scheduled later
    co_yield {};
  }
}

int main()
{
  ThreadPool pool(4);

  // Spawn a bunch of coroutine tasks
  for (int i = 0; i < 80; ++i)
  {
    pool.spawn(stepper(i, /*steps=*/5, /*work_ms=*/30));
  }

  // Block until all tasks finish
  pool.wait_idle();

  std::cout << "All tasks done.\n";
}
