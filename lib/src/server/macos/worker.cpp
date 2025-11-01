#include <cstddef>
#include <cstring>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <fcntl.h>
#include "common/socket.hpp"
#include "common/workflow.hpp"
#include "server/worker.hpp"
#include "server/server.hpp"
#include "common/processing_machine.hpp"
#include <coroutine>
#include <exception>

enum class SuspensionReason
{
    READ,
    FIND_HANDLER,
    GENERATE_RESPONSE,
    WRITE,
    ERROR,
    NONE
};

struct RequestCoroutine
{
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    struct promise_type
    {
        SuspensionReason reason{SuspensionReason::NONE};
        std::exception_ptr exception_;

        RequestCoroutine get_return_object() { return RequestCoroutine(handle_type::from_promise(*this)); }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() { exception_ = std::current_exception(); }
        std::suspend_always yield_value(SuspensionReason from)
        {
            reason = from;
            return {};
        }
        void return_void() {}
    };

    handle_type h_;
    RequestCoroutine(handle_type h) : h_(h) {}
    ~RequestCoroutine() { if (h_) h_.destroy(); }

    explicit operator bool() { return !h_.done(); }
    SuspensionReason operator()()
    {
        h_();
        if (h_.promise().exception_)
            std::rethrow_exception(h_.promise().exception_);
        return h_.promise().reason;
    }
};

struct CoroutineState {
    ProcessingMachine pm;
    char buffer[1024];
    std::vector<std::string> response_parts; // Store strings to ensure data validity
    std::vector<iovec> iov;
    msghdr msg{};
};

RequestCoroutine handle_request(fd_t client_fd, Worker* worker, std::shared_ptr<CoroutineState> state) {
    while (true) { // Keep-alive loop
        co_yield SuspensionReason::READ;

        ssize_t bytes_read = recv(client_fd, state->buffer, sizeof(state->buffer), 0);
        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                perror("recv");
            }
            co_yield SuspensionReason::ERROR;
            co_return;
        }

        ssize_t bytes_parsed = 0;
        while (bytes_parsed < bytes_read) {
            bytes_parsed += state->pm.on_segment(state->buffer, bytes_read);
            if (state->pm.state == ProcessingState::completed_state || state->pm.state == ProcessingState::error_state) {
                std::function<void(const Request&, Response&)> request_handler;

                co_yield SuspensionReason::FIND_HANDLER;
                if (state->pm.state == ProcessingState::error_state) {
                    auto it = worker->server()->get_routes().find("500");
                    if (it != worker->server()->get_routes().end())
                        request_handler = it->second;
                } else {
                    auto it = worker->server()->get_routes().find(state->pm.req.path);
                    if (it != worker->server()->get_routes().end())
                        request_handler = it->second;
                    else {
                        auto it = worker->server()->get_routes().find("404");
                        if (it != worker->server()->get_routes().end())
                            request_handler = it->second;
                    }
                }

                if (!request_handler) {
                    co_yield SuspensionReason::ERROR;
                    co_return;
                }

                co_yield SuspensionReason::GENERATE_RESPONSE;
                request_handler(state->pm.req, state->pm.res);

                // Prepare response
                state->iov.clear();
                state->response_parts.clear();

                state->response_parts.emplace_back("HTTP/1.1 " + state->pm.res.code + "\r\n");
                state->iov.push_back({(void*)state->response_parts.back().data(), state->response_parts.back().size()});

                state->response_parts.emplace_back("Content-Length: " + std::to_string(state->pm.res.body.size()) + "\r\n");
                state->iov.push_back({(void*)state->response_parts.back().data(), state->response_parts.back().size()});

                for (const auto& h : state->pm.res.headers) {
                    state->response_parts.emplace_back(h.first + ": " + h.second + "\r\n");
                    state->iov.push_back({(void*)state->response_parts.back().data(), state->response_parts.back().size()});
                }

                state->response_parts.emplace_back("\r\n");
                state->iov.push_back({(void*)state->response_parts.back().data(), state->response_parts.back().size()});

                state->response_parts.emplace_back(state->pm.res.body);
                state->iov.push_back({(void*)state->response_parts.back().data(), state->response_parts.back().size()});

                state->msg.msg_iov = state->iov.data();
                state->msg.msg_iovlen = state->iov.size();

                co_yield SuspensionReason::WRITE;
                ssize_t bytes_sent = sendmsg(client_fd, &state->msg, 0);
                if (bytes_sent < 0) {
                    perror("sendmsg");
                    co_yield SuspensionReason::ERROR;
                    co_return;
                }

                state->pm.reset_parser();
            }
        }
    }
}

Worker::Worker(Server* owner_, fd_t listen_fd, const WorkerConfig& cfg)
    : listen_fd_(listen_fd), cfg_(cfg), owner(owner_)
{
    kqueue_fd_ = kqueue();
    if (kqueue_fd_ == -1)
        throw std::runtime_error("Failed to create kqueue");
}

void Worker::run()
{
    std::unordered_map<fd_t, RequestCoroutine> coroutines;
    std::unordered_map<fd_t, SuspensionReason> suspension_reasons;
    std::unordered_map<fd_t, std::shared_ptr<CoroutineState>> coroutine_states;

    struct kevent change[2];
    struct kevent events[128];

    EV_SET(&change[0], listen_fd_, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kqueue_fd_, change, 1, nullptr, 0, nullptr) == -1)
        throw std::runtime_error("Failed to register listen_fd with kqueue");

    while (true)
    {
        int nev = kevent(kqueue_fd_, nullptr, 0, events, 128, nullptr);
        if (nev < 0)
        {
            perror("kevent");
            break;
        }

        for (int i = 0; i < nev; i++)
        {
            fd_t fd = static_cast<fd_t>(events[i].ident);

            if (fd == listen_fd_)
            {
                fd_t client_fd = accept(listen_fd_, nullptr, nullptr);
                if (client_fd != -1)
                {
                    fcntl(client_fd, F_SETFL, O_NONBLOCK);
                    EV_SET(&change[0], client_fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
                    EV_SET(&change[1], client_fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
                    kevent(kqueue_fd_, change, 2, nullptr, 0, nullptr);
                    auto state = std::make_shared<CoroutineState>();
                    coroutine_states[client_fd] = state;
                    coroutines.emplace(client_fd, handle_request(client_fd, this, state));
                    suspension_reasons[client_fd] = SuspensionReason::READ;
                }
            }
            else
            {
                auto it = coroutines.find(fd);
                if (it == coroutines.end())
                {
                    close(fd);
                    continue;
                }

                auto& coroutine = it->second;
                auto& reason = suspension_reasons[fd];

                if (events[i].flags & EV_EOF || events[i].flags & EV_ERROR)
                {
                    close(fd);
                    coroutines.erase(it);
                    suspension_reasons.erase(fd);
                    coroutine_states.erase(fd);
                    continue;
                }

                if ((reason == SuspensionReason::READ && events[i].filter == EVFILT_READ) ||
                    (reason == SuspensionReason::WRITE && events[i].filter == EVFILT_WRITE))
                {
                    reason = coroutine();
                }

                if (reason == SuspensionReason::ERROR)
                {
                    close(fd);
                    coroutines.erase(it);
                    suspension_reasons.erase(fd);
                    coroutine_states.erase(fd);
                }
            }
        }
    }
}

Worker::~Worker()
{
    if (kqueue_fd_ != -1)
        close(kqueue_fd_);
}
