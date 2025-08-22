#include "io_context.hpp"

#if defined(_WIN32)
    // Windows IOCP implementation would go here
    #include "pollers/iocp_poller.hpp"

    namespace io
    {
        MyIOContext::MyIOContext() : poller_(std::make_unique<IocpPoller>())
        {
            if (!poller_->init()) {
                throw std::runtime_error("Failed to initialize IOCP poller");
            }
        }

        MyIOContext::~MyIOContext()
        {
            stop();
        }

        void MyIOContext::post(std::function<void()> task)
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            work_queue_.push(std::move(task));
        }

        bool MyIOContext::register_handle(SOCKET fd)
        {
            return poller_->register_fd(fd);
        }

        void MyIOContext::run()
        {
            run_tasks();
        }

        void MyIOContext::run_in_threads(size_t n)
        {
            // Windows implementation
        }

        void MyIOContext::stop()
        {
            stop_flag_.store(true, std::memory_order_release);
            poller_->close();
        }

        void MyIOContext::run_tasks()
        {
            while (!stop_flag_.load(std::memory_order_acquire)) {
                std::function<void()> task;
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    if (!work_queue_.empty()) {
                        task = std::move(work_queue_.front());
                        work_queue_.pop();
                    }
                }

                if (task) {
                    task();
                }

                // Run poller with timeout
                poller_->run_loop(stop_flag_, [this]() { run_tasks(); });
            }
        }
    }
#else
    // Linux epoll implementation
    #include "pollers/epoll_poller.hpp"
    #include <vector>
    #include <thread>

    namespace io
    {
        MyIOContext::MyIOContext() : poller_(std::make_unique<EpollPoller>())
        {
            if (!poller_->init()) {
                throw std::runtime_error("Failed to initialize epoll poller");
            }
        }

        MyIOContext::~MyIOContext()
        {
            stop();
        }

        void MyIOContext::post(std::function<void()> task)
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            work_queue_.push(std::move(task));
        }

        bool MyIOContext::register_handle(PlatformSocket fd, EpollEvent* event, uint32_t events)
        {
            return poller_->add_fd(fd, event, events);
        }

        void MyIOContext::run()
        {
            run_tasks();
        }

        void MyIOContext::run_in_threads(size_t n)
        {
            std::vector<std::thread> threads;
            threads.reserve(n);

            for (size_t i = 0; i < n; ++i) {
                threads.emplace_back([this]() {
                    run_tasks();
                });
            }

            for (auto& t : threads) {
                if (t.joinable()) {
                    t.join();
                }
            }
        }

        void MyIOContext::stop()
        {
            stop_flag_.store(true, std::memory_order_release);
            poller_->close();
        }

        void MyIOContext::run_tasks()
        {
            auto pump_tasks = [this]() {
                std::function<void()> task;
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    if (!work_queue_.empty()) {
                        task = std::move(work_queue_.front());
                        work_queue_.pop();
                    }
                }

                if (task) {
                    task();
                }
            };

            poller_->run_loop(stop_flag_, pump_tasks);
        }
    }
#endif