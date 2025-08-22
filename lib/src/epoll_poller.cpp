#include "pollers/epoll_poller.hpp"
#include <unistd.h>
#include <cstring>
#include <iostream>

namespace io
{
    EpollPoller::EpollPoller() : ready_events_(MAX_EVENTS)
    {
    }

    EpollPoller::~EpollPoller()
    {
        close();
    }

    bool EpollPoller::init()
    {
        epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ == -1) {
            std::cerr << "Failed to create epoll: " << strerror(errno) << std::endl;
            return false;
        }

        event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (event_fd_ == -1) {
            std::cerr << "Failed to create eventfd: " << strerror(errno) << std::endl;
            ::close(epoll_fd_);
            epoll_fd_ = -1;
            return false;
        }

        // Add eventfd to epoll for custom events
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = event_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev) == -1) {
            std::cerr << "Failed to add eventfd to epoll: " << strerror(errno) << std::endl;
            ::close(event_fd_);
            ::close(epoll_fd_);
            event_fd_ = epoll_fd_ = -1;
            return false;
        }

        return true;
    }

    bool EpollPoller::add_fd(PlatformSocket fd, EpollEvent* event, uint32_t events)
    {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
            std::cerr << "Failed to add fd to epoll: " << strerror(errno) << std::endl;
            return false;
        }

        events_[fd] = std::unique_ptr<EpollEvent>(event);
        return true;
    }

    bool EpollPoller::modify_fd(PlatformSocket fd, uint32_t events)
    {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;

        if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == -1) {
            std::cerr << "Failed to modify fd in epoll: " << strerror(errno) << std::endl;
            return false;
        }

        return true;
    }

    bool EpollPoller::remove_fd(PlatformSocket fd)
    {
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == -1) {
            std::cerr << "Failed to remove fd from epoll: " << strerror(errno) << std::endl;
            return false;
        }

        events_.erase(fd);
        return true;
    }

    bool EpollPoller::post_custom_event(EpollEvent* event)
    {
        uint64_t value = 1;
        if (write(event_fd_, &value, sizeof(value)) != sizeof(value)) {
            std::cerr << "Failed to write to eventfd: " << strerror(errno) << std::endl;
            return false;
        }

        // Store the custom event temporarily
        events_[event_fd_] = std::unique_ptr<EpollEvent>(event);
        return true;
    }

    void EpollPoller::run_loop(std::atomic<bool>& stop_flag, std::function<void()> pump_tasks)
    {
        const int timeout_ms = 1000; // 1 second timeout

        while (!stop_flag.load(std::memory_order_acquire)) {
            int num_events = epoll_wait(epoll_fd_, ready_events_.data(), MAX_EVENTS, timeout_ms);

            if (num_events == -1) {
                if (errno == EINTR) continue;
                std::cerr << "epoll_wait failed: " << strerror(errno) << std::endl;
                break;
            }

            for (int i = 0; i < num_events; ++i) {
                const epoll_event& ev = ready_events_[i];
                PlatformSocket fd = ev.data.fd;

                if (fd == event_fd_) {
                    // Handle custom event
                    uint64_t value;
                    read(event_fd_, &value, sizeof(value));

                    auto it = events_.find(event_fd_);
                    if (it != events_.end() && it->second) {
                        it->second->handler(it->second.get(), ev.events);
                    }
                    continue;
                }

                auto it = events_.find(fd);
                if (it != events_.end() && it->second) {
                    it->second->handler(it->second.get(), ev.events);
                }
            }

            // Pump any posted tasks
            if (pump_tasks) {
                pump_tasks();
            }
        }
    }

    void EpollPoller::close()
    {
        if (event_fd_ != -1) {
            ::close(event_fd_);
            event_fd_ = -1;
        }

        if (epoll_fd_ != -1) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }

        events_.clear();
    }
}