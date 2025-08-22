#pragma once

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <functional>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mutex>

namespace io
{
  struct EpollEventData
  {
    int fd;
    void* user_data;
    uint32_t events;
  };

  class EpollPoller
  {
  public:
    EpollPoller();
    ~EpollPoller();

    EpollPoller(const EpollPoller&) = delete;
    EpollPoller& operator=(const EpollPoller&) = delete;
    EpollPoller(EpollPoller&&) = default;
    EpollPoller& operator=(EpollPoller&&) = default;

    bool add_fd(int fd, uint32_t events, void* user_data);
    bool modify_fd(int fd, uint32_t events, void* user_data);
    bool remove_fd(int fd);

    // Post a custom event (similar to PostQueuedCompletionStatus in IOCP)
    void post_event(void* user_data);

    // Wait for events with timeout (milliseconds)
    std::vector<EpollEventData> wait_events(int timeout_ms = -1);

  private:
    int epoll_fd_;
    int event_fd_;
    std::mutex mutex_;
    std::vector<struct epoll_event> events_buffer_;
  };
}