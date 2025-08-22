#pragma once

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <chrono>

namespace io
{
    using PlatformSocket = int;

    enum class EventType {
        Accept,
        Read,
        Write,
        Close,
        Process,
        Respond,
        Custom
    };

    struct EpollEvent {
        EventType type;
        PlatformSocket socket;
        std::function<void(EpollEvent*, uint32_t)> handler;
        void* user_data{nullptr};

        EpollEvent(EventType t, PlatformSocket s, std::function<void(EpollEvent*, uint32_t)> h)
            : type(t), socket(s), handler(std::move(h)) {}
    };

    class EpollPoller {
    public:
        EpollPoller();
        ~EpollPoller();

        bool init();
        bool add_fd(PlatformSocket fd, EpollEvent* event, uint32_t events = EPOLLIN | EPOLLET);
        bool modify_fd(PlatformSocket fd, uint32_t events);
        bool remove_fd(PlatformSocket fd);
        bool post_custom_event(EpollEvent* event);
        void run_loop(std::atomic<bool>& stop_flag, std::function<void()> pump_tasks);
        void close();

    private:
        int epoll_fd_{-1};
        int event_fd_{-1};
        std::unordered_map<PlatformSocket, std::unique_ptr<EpollEvent>> events_;
        std::vector<epoll_event> ready_events_;
        static constexpr size_t MAX_EVENTS = 1024;
    };
}