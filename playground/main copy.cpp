#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/event.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std;

// ------------------------------- config --------------------------------
constexpr uint16_t PORT = 8080;
constexpr int BACKLOG = 65535;                 // clamped by kern.ipc.somaxconn
constexpr size_t READ_BUF = 4096;
constexpr int MAX_WORKERS = 0;                 // 0 = use HW concurrency
constexpr int MAX_KEEPALIVE_REQ = 100;         // per-connection
constexpr int REACTOR_MAX_EVENTS = 4096;       // kqueue drain batch
// -----------------------------------------------------------------------

static int make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void raise_nofile(rlim_t want = 65536) {
    rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rl.rlim_cur = std::min<rlim_t>(want, rl.rlim_max);
        setrlimit(RLIMIT_NOFILE, &rl);
    }
}

// ============================= ThreadPool ==============================
class ThreadPool {
public:
    explicit ThreadPool(int n = 0) : stop_(false) {
        if (n <= 0) n = std::thread::hardware_concurrency();
        if (n <= 0) n = 4;
        workers_.reserve(n);
        for (int i = 0; i < n; ++i) workers_.emplace_back([this]{ run(); });
    }
    ~ThreadPool() { stop(); }

    void schedule(std::coroutine_handle<> h) {
        { std::lock_guard lock(m_); q_.push(h); }
        cv_.notify_one();
    }
    void stop() {
        bool expected = false;
        if (!stop_.compare_exchange_strong(expected, true)) return;
        cv_.notify_all();
        for (auto &t : workers_) if (t.joinable()) t.join();
    }

private:
    void run() {
        for (;;) {
            std::coroutine_handle<> h;
            {
                std::unique_lock lk(m_);
                cv_.wait(lk, [this]{ return stop_ || !q_.empty(); });
                if (stop_ && q_.empty()) break;
                h = q_.front(); q_.pop();
            }
            if (h) h.resume();
        }
    }

    vector<thread> workers_;
    queue<std::coroutine_handle<>> q_;
    mutex m_;
    condition_variable cv_;
    atomic<bool> stop_;
};

// ================================ Reactor ==============================
class Reactor {
public:
    explicit Reactor(ThreadPool& pool) : pool_(pool) {
        kq_ = kqueue();
        if (kq_ == -1) { perror("kqueue"); std::exit(1); }
        reactor_thread_ = std::thread([this]{ loop(); });
    }
    ~Reactor() {
        stop_ = true;
        struct kevent kev; EV_SET(&kev, 1, EVFILT_TIMER, EV_ADD | EV_ONESHOT, 0, 1, nullptr);
        kevent(kq_, &kev, 1, nullptr, 0, nullptr);
        if (reactor_thread_.joinable()) reactor_thread_.join();
        close(kq_);
    }
    int kq() const { return kq_; }

    struct IOAwaiter {
        Reactor& r; int fd; int filter; // EVFILT_READ or EVFILT_WRITE
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            struct kevent kev;
            EV_SET(&kev, fd, filter, EV_ADD | EV_ONESHOT | EV_CLEAR, 0, 0, h.address());
            if (kevent(r.kq_, &kev, 1, nullptr, 0, nullptr) == -1) {
                r.pool_.schedule(h); // let coroutine handle error path
            }
        }
        void await_resume() const noexcept {}
    };

    IOAwaiter readable(int fd) { return IOAwaiter{*this, fd, EVFILT_READ}; }
    IOAwaiter writable(int fd) { return IOAwaiter{*this, fd, EVFILT_WRITE}; }

private:
    void loop() {
        std::vector<struct kevent> events(REACTOR_MAX_EVENTS);
        while (!stop_) {
            int nfds = kevent(kq_, nullptr, 0, events.data(), (int)events.size(), nullptr);
            if (nfds < 0) { if (errno == EINTR) continue; perror("kevent wait"); continue; }
            for (int i = 0; i < nfds; ++i) {
                auto &e = events[i];
                if (!e.udata) continue; // timer wakeups, etc.
                auto h = std::coroutine_handle<>::from_address(e.udata);
                pool_.schedule(h);
            }
        }
    }

    ThreadPool& pool_;
    int kq_{-1};
    std::thread reactor_thread_;
    std::atomic<bool> stop_{false};
};

// ======================= Detached coroutine type ======================
struct Detached {
    struct promise_type {
        Detached get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        struct SelfDestroy {
            bool await_ready() noexcept { return false; }
            void await_suspend(std::coroutine_handle<promise_type> h) noexcept { h.destroy(); }
            void await_resume() noexcept {}
        };
        SelfDestroy final_suspend() noexcept { return {}; }
        void unhandled_exception() { std::terminate(); }
        void return_void() {}
    };
};

// ============================= HTTP bits ===============================
static inline bool find_double_crlf(const char* b, size_t n) {
    for (size_t i = 0; i + 3 < n; ++i)
        if (b[i]=='\r' && b[i+1]=='\n' && b[i+2]=='\r' && b[i+3]=='\n') return true;
    return false;
}

static constexpr std::string_view RESP_HDR_KEEP =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 14\r\n"
    "Connection: keep-alive\r\n"
    "\r\n";
static constexpr std::string_view RESP_HDR_CLOSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 14\r\n"
    "Connection: close\r\n"
    "\r\n";
static constexpr std::string_view RESP_BODY = "Hello, World!\n"; // 14 bytes

// ================================ Server ===============================
struct Server {
    ThreadPool pool;
    Reactor reactor;
    int listen_v4{-1};
    int listen_v6{-1};

    explicit Server(int workers = MAX_WORKERS) : pool(workers), reactor(pool) {}
    ~Server() {
        if (listen_v4 != -1) close(listen_v4);
        if (listen_v6 != -1) close(listen_v6);
        pool.stop();
    }

    bool bind_and_listen(uint16_t port) {
        listen_v4 = create_listener_v4(port);
        listen_v6 = create_listener_v6(port); // -1 if v6 unavailable
        if (listen_v4 == -1 && listen_v6 == -1) return false;
        if (listen_v4 != -1) accept_loop(listen_v4, "IPv4");
        if (listen_v6 != -1) accept_loop(listen_v6, "IPv6");
        return true;
    }

    // ------------------------- socket helpers --------------------------
    static int create_listener_v4(uint16_t port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { perror("socket v4"); return -1; }
        int yes = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
        if (make_nonblocking(fd) == -1) { perror("nonblock v4"); close(fd); return -1; }
        sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_addr.s_addr=htonl(INADDR_ANY); addr.sin_port=htons(port);
        if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind v4"); close(fd); return -1; }
        if (listen(fd, BACKLOG) < 0) { perror("listen v4"); close(fd); return -1; }
        return fd;
    }
    static int create_listener_v6(uint16_t port) {
        int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
        if (fd < 0) return -1;
        int yes = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
#ifdef IPV6_V6ONLY
        int v6only = 0; setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)); // prefer dual stack
#endif
        if (make_nonblocking(fd) == -1) { perror("nonblock v6"); close(fd); return -1; }
        sockaddr_in6 a6{}; a6.sin6_family=AF_INET6; a6.sin6_addr=in6addr_any; a6.sin6_port=htons(port);
        if (bind(fd, (sockaddr*)&a6, sizeof(a6)) < 0) { perror("bind v6"); close(fd); return -1; }
        if (listen(fd, BACKLOG) < 0) { perror("listen v6"); close(fd); return -1; }
        return fd;
    }

    // --------------------------- coroutines ----------------------------
    Detached accept_loop(int lfd, const char* tag) {
        std::cout << "Accept loop started (" << tag << ", fd=" << lfd << ")\n";
        for (;;) {
            co_await reactor.readable(lfd);
            for (;;) {
                sockaddr_storage ss{}; socklen_t slen = sizeof(ss);
                int cfd = ::accept(lfd, (sockaddr*)&ss, &slen);
                if (cfd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    perror("accept"); break;
                }
                if (make_nonblocking(cfd) == -1) { ::close(cfd); continue; }
#ifdef SO_NOSIGPIPE
                {int one = 1; setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));}
#endif
                {int one = 1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));}
                connection(cfd);
            }
        }
    }

    Detached connection(int cfd) {
        std::array<char, READ_BUF> buf{};

        for (int served = 0; served < MAX_KEEPALIVE_REQ; ++served) {
            size_t len = 0;

            // --- read headers (super naive) ---
            for (;;) {
                ssize_t n = ::recv(cfd, buf.data() + len, READ_BUF - len, 0);
                if (n > 0) {
                    len += (size_t)n;
                    if (find_double_crlf(buf.data(), len) || len == READ_BUF) break;
                    continue;
                }
                if (n == 0) { ::close(cfd); co_return; }
                if (errno == EAGAIN || errno == EWOULDBLOCK) { co_await reactor.readable(cfd); continue; }
                ::close(cfd); co_return; // other error
            }

            // --- write response via writev (no per-request string build) ---
            const auto& hdr = (served + 1 < MAX_KEEPALIVE_REQ) ? RESP_HDR_KEEP : RESP_HDR_CLOSE;
            iovec iov[2]{
                { const_cast<char*>(hdr.data()),      static_cast<unsigned>(hdr.size()) },
                { const_cast<char*>(RESP_BODY.data()), static_cast<unsigned>(RESP_BODY.size()) }
            };
            size_t total = hdr.size() + RESP_BODY.size();
            size_t sent  = 0;
            while (sent < total) {
                ssize_t n = ::writev(cfd, iov, 2);
                if (n > 0) {
                    sent += (size_t)n;
                    size_t left = (size_t)n;
                    for (int k = 0; k < 2 && left; ++k) {
                        size_t take = std::min(left, (size_t)iov[k].iov_len);
                        iov[k].iov_base = static_cast<char*>(iov[k].iov_base) + take;
                        iov[k].iov_len  -= take;
                        left -= take;
                    }
                    continue;
                }
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { co_await reactor.writable(cfd); continue; }
                ::close(cfd); co_return; // send error
            }

            if (served + 1 >= MAX_KEEPALIVE_REQ) { ::close(cfd); co_return; }
            // else loop for next request on same TCP connection
        }
        ::close(cfd);
        co_return;
    }
};

// ================================ main =================================
int main() {
    raise_nofile(65536);

    // quick knob: set workers via env (e.g., WORKERS=16)
    int workers = MAX_WORKERS;
    if (const char* w = std::getenv("WORKERS")) {
        int v = std::atoi(w);
        if (v > 0) workers = v;
    }

    Server srv(workers);
    if (!srv.bind_and_listen(PORT)) {
        std::cerr << "Failed to bind on port " << PORT << "\n";
        return 1;
    }

    std::cout << "Listening on http://0.0.0.0:" << PORT
              << " (kqueue + coroutines + thread pool)\n";

    std::this_thread::sleep_for(std::chrono::hours(24 * 365));
    return 0;
}
