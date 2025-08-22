#include "../include/pollers/epoll_poller.hpp"
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <stdexcept>

namespace io
{
  EpollPoller::EpollPoller()
    : epoll_fd_(-1)
    , event_fd_(-1)
    , events_buffer_(1024)
  {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
      throw std::runtime_error("Failed to create epoll instance: " + std::string(strerror(errno)));
    }

    event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (event_fd_ == -1) {
      close(epoll_fd_);
      throw std::runtime_error("Failed to create eventfd: " + std::string(strerror(errno)));
    }

    // Add eventfd to epoll for internal notifications
    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = nullptr; // nullptr indicates this is our internal eventfd
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev) == -1) {
      close(event_fd_);
      close(epoll_fd_);
      throw std::runtime_error("Failed to add eventfd to epoll: " + std::string(strerror(errno)));
    }
  }

  EpollPoller::~EpollPoller()
  {
    if (event_fd_ != -1) {
      close(event_fd_);
    }
    if (epoll_fd_ != -1) {
      close(epoll_fd_);
    }
  }

  bool EpollPoller::add_fd(int fd, uint32_t events, void* user_data)
  {
    std::lock_guard<std::mutex> lock(mutex_);

    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = user_data;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
      return false;
    }

    return true;
  }

  bool EpollPoller::modify_fd(int fd, uint32_t events, void* user_data)
  {
    std::lock_guard<std::mutex> lock(mutex_);

    struct epoll_event ev;
    ev.events = events;
    ev.data.ptr = user_data;

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == -1) {
      return false;
    }

    return true;
  }

  bool EpollPoller::remove_fd(int fd)
  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == -1) {
      return false;
    }

    return true;
  }

  void EpollPoller::post_event(void* user_data)
  {
    // Write to eventfd to wake up the epoll_wait
    uint64_t value = 1;
    if (write(event_fd_, &value, sizeof(value)) == -1) {
      // In a real implementation, you might want to handle this error
    }
  }

  std::vector<EpollEventData> EpollPoller::wait_events(int timeout_ms)
  {
    std::vector<EpollEventData> result;

    int nfds = epoll_wait(epoll_fd_, events_buffer_.data(), events_buffer_.size(), timeout_ms);
    if (nfds == -1) {
      return result;
    }

    for (int i = 0; i < nfds; ++i) {
      const auto& event = events_buffer_[i];

      // Check if this is our internal eventfd
      if (event.data.ptr == nullptr) {
        // Consume the eventfd notification
        uint64_t value;
        read(event_fd_, &value, sizeof(value));
        continue;
      }

      result.push_back({
        .fd = -1, // We don't track the original fd easily, but it's in the event
        .user_data = event.data.ptr,
        .events = event.events
      });
    }

    return result;
  }
}