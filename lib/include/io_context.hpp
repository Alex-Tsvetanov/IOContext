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

#if defined(_WIN32)
  #include "pollers/iocp_poller.hpp"
#else
  #include "pollers/epoll_poller.hpp"
#endif

namespace io
{
#if defined(_WIN32)
  class MyIOContext
  {
  public:
    MyIOContext();
    ~MyIOContext();

    void post(std::function<void()> task);
    bool register_handle(SOCKET fd);
    void run();
    void run_in_threads(size_t n);
    void stop();

  private:
    std::unique_ptr<IocpPoller> poller_;
    std::mutex queue_mutex_;
    std::queue<std::function<void()>> work_queue_;
    std::atomic<bool> stop_flag_ = false;

    void run_tasks();
  };
#else
  class MyIOContext
  {
  public:
    MyIOContext();
    ~MyIOContext();

    void post(std::function<void()> task);
    bool register_handle(PlatformSocket fd, EpollEvent* event, uint32_t events = EPOLLIN | EPOLLET);
    void run();
    void run_in_threads(size_t n);
    void stop();

  private:
    std::unique_ptr<EpollPoller> poller_;
    std::mutex queue_mutex_;
    std::queue<std::function<void()>> work_queue_;
    std::atomic<bool> stop_flag_ = false;

    void run_tasks();
  };
#endif
} // namespace io
