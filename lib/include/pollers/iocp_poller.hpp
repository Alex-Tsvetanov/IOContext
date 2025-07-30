// iocp_poller.h
#pragma once

#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <iostream>
#include <string>
#include <memory>
#include <functional>
#include <chrono>
#include <thread>

namespace io
{
    using PlatformSocket = SOCKET;

    // Overlapped I/O operation holder
    struct OverlappedOp {
        OVERLAPPED ol{};
        std::function<void(DWORD)> handler;

        OverlappedOp() {
            ZeroMemory(&ol, sizeof(ol));
        }

        static OverlappedOp* from(OVERLAPPED* ptr) {
            return reinterpret_cast<OverlappedOp*>(ptr);
        }
    };

    class IocpPoller 
    {
    public:
        IocpPoller();
        ~IocpPoller();

        bool init();
        bool register_fd(SOCKET fd);
        bool post_empty();
        void run_loop(std::atomic<bool>& stop_flag, std::function<void()> pump_tasks);
        void close();

    private:
        HANDLE iocp_ = nullptr;
    };

} // namespace io
