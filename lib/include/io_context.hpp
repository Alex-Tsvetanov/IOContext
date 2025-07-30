#pragma once
#include <functional>
#include <queue>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <vector>
#include <thread>
#include <memory>
#include <chrono>

#include "pollers/iocp_poller.hpp" // For Windows first; epoll/kqueue come next

namespace io
{
  class MyIOContext
  {
  public:
    MyIOContext();
    ~MyIOContext();

    void post(std::function<void()> task);
    bool register_handle(SOCKET fd); // IOCP only
    void run();                      // single-thread
    void run_in_threads(size_t n);   // optional MT
    void stop();

  private:
    std::unique_ptr<IocpPoller> poller_;
    std::mutex queue_mutex_;
    std::queue<std::function<void()>> work_queue_;
    std::atomic<bool> stop_flag_ = false;

    void run_tasks(); // handles only the `post()` queue
  };
} // namespace io
