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
#include <functional>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace std;

// ------------------------------- config --------------------------------
constexpr uint16_t PORT = 8080;
constexpr int BACKLOG = 65535;                 // clamped by kern.ipc.somaxconn
constexpr size_t READ_BUF = 4096;
constexpr int MAX_WORKERS = 0;                 // 0 = use HW concurrency
constexpr int MAX_EVENTS = 4096;               // kevent drain batch
constexpr int MAX_KEEPALIVE_REQ = 100;         // per-connection
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
    explicit ThreadPool(int n = 0) {
        if (n <= 0) n = std::thread::hardware_concurrency();
        if (n <= 0) n = 4;
        for (int i = 0; i < n; ++i) {
            workers_.emplace_back([this]{ worker(); });
        }
    }
    ~ThreadPool() { stop(); }

    template<class F>
    void schedule(F&& f) {
        {
            std::lock_guard lk(m_);
            q_.emplace(std::forward<F>(f));
        }
        cv_.notify_one();
    }

    void stop() {
        {
            std::lock_guard lk(m_);
            stopping_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) if (t.joinable()) t.join();
        workers_.clear();
    }

private:
    void worker() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock lk(m_);
                cv_.wait(lk, [this]{ return stopping_ || !q_.empty(); });
                if (stopping_ && q_.empty()) break;
                job = std::move(q_.front()); q_.pop();
            }
            job();
        }
    }

    vector<std::thread> workers_;
    queue<std::function<void()>> q_;
    mutex m_;
    condition_variable cv_;
    bool stopping_{false};
};

// ============================= HTTP helpers ============================
static inline bool has_double_crlf(const char* b, size_t n) {
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

// Forward declare
struct Server;

// =============================== Connection ============================
struct Connection {
    int fd{-1};
    std::array<char, READ_BUF> rbuf{};
    size_t rlen{0};

    // write state (iovecs mutated as we write)
    iovec iov[2]{};
    size_t total_to_send{0};
    size_t sent{0};

    int served{0};
    std::atomic_flag inflight = ATOMIC_FLAG_INIT; // prevent concurrent processing

    explicit Connection(int cfd) : fd(cfd) {}
};

// ================================ Server ===============================
struct Server {
    explicit Server(int workers)
        : pool_(workers) {
        kq_ = kqueue();
        if (kq_ == -1) { perror("kqueue"); std::exit(1); }
        reactor_thread_ = std::thread([this]{ reactor_loop(); });
    }

    ~Server() {
        stopping_ = true;
        // wake kevent
        struct kevent kev; EV_SET(&kev, 1, EVFILT_TIMER, EV_ADD | EV_ONESHOT, 0, 1, nullptr);
        kevent(kq_, &kev, 1, nullptr, 0, nullptr);

        if (reactor_thread_.joinable()) reactor_thread_.join();

        // Close all remaining conns
        for (auto* c : conns_) {
            if (c->fd != -1) ::close(c->fd);
            delete c;
        }
        conns_.clear();

        if (listen_v4_ != -1) ::close(listen_v4_);
        if (listen_v6_ != -1) ::close(listen_v6_);
        if (kq_ != -1) ::close(kq_);
        pool_.stop();
    }

    bool bind_and_listen(uint16_t port) {
        listen_v4_ = create_listener_v4(port);
        listen_v6_ = create_listener_v6(port); // -1 if v6 unavailable
        if (listen_v4_ == -1 && listen_v6_ == -1) return false;

        if (listen_v4_ != -1) add_read_oneshot(listen_v4_, nullptr);
        if (listen_v6_ != -1) add_read_oneshot(listen_v6_, nullptr);

        return true;
    }

    // --------------- kqueue registration helpers (one-shot) -------------
    void add_read_oneshot(int fd, void* udata) {
        struct kevent kev;
        EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ONESHOT | EV_CLEAR, 0, 0, udata);
        if (kevent(kq_, &kev, 1, nullptr, 0, nullptr) == -1) {
            // perror("kevent add read");
        }
    }
    void add_write_oneshot(int fd, void* udata) {
        struct kevent kev;
        EV_SET(&kev, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT | EV_CLEAR, 0, 0, udata);
        if (kevent(kq_, &kev, 1, nullptr, 0, nullptr) == -1) {
            // perror("kevent add write");
        }
    }
    void del_all_filters(int fd, void* udata) {
        struct kevent kev[2];
        EV_SET(&kev[0], fd, EVFILT_READ,  EV_DELETE, 0, 0, udata);
        EV_SET(&kev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, udata);
        kevent(kq_, kev, 2, nullptr, 0, nullptr);
    }

    // ---------------------------- acceptors -----------------------------
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
        if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind v4"); close(fd); return -1; }
        if (::listen(fd, BACKLOG) < 0) { perror("listen v4"); close(fd); return -1; }
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
        if (::bind(fd, (sockaddr*)&a6, sizeof(a6)) < 0) { perror("bind v6"); close(fd); return -1; }
        if (::listen(fd, BACKLOG) < 0) { perror("listen v6"); close(fd); return -1; }
        return fd;
    }

    // --------------------------- reactor loop ---------------------------
    void reactor_loop() {
        std::cout << "Reactor started. Listening...\n";
        std::vector<struct kevent> events(MAX_EVENTS);

        while (!stopping_) {
            int nfds = kevent(kq_, nullptr, 0, events.data(), (int)events.size(), nullptr);
            if (nfds < 0) {
                if (errno == EINTR) continue;
                perror("kevent wait");
                continue;
            }

            for (int i = 0; i < nfds; ++i) {
                auto &e = events[i];

                // Listening sockets have udata == nullptr
                if (e.udata == nullptr) {
                    if (e.filter == EVFILT_READ) {
                        int lfd = (int)e.ident;
                        handle_accept_ready(lfd);
                        // re-arm accept
                        add_read_oneshot(lfd, nullptr);
                    }
                    continue;
                }

                // Connection events
                auto* conn = static_cast<Connection*>(e.udata);
                if (!conn || conn->fd == -1) continue;

                if (e.flags & (EV_ERROR | EV_EOF)) {
                    close_conn(conn);
                    continue;
                }

                // prevent duplicate concurrent processing
                if (conn->inflight.test_and_set(std::memory_order_acquire)) {
                    // another worker is already processing this connection
                    continue;
                }

                // Decide what to do based on filter; schedule work
                if (e.filter == EVFILT_READ) {
                    pool_.schedule([this, conn] {
                        process_read(conn);
                        conn->inflight.clear(std::memory_order_release);
                    });
                } else if (e.filter == EVFILT_WRITE) {
                    pool_.schedule([this, conn] {
                        process_write(conn);
                        conn->inflight.clear(std::memory_order_release);
                    });
                }
            }
        }
    }

    // ----------------------------- accept -------------------------------
    void handle_accept_ready(int lfd) {
        for (;;) {
            sockaddr_storage ss{}; socklen_t slen = sizeof(ss);
            int cfd = ::accept(lfd, (sockaddr*)&ss, &slen);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                perror("accept");
                break;
            }
            if (make_nonblocking(cfd) == -1) { ::close(cfd); continue; }
#ifdef SO_NOSIGPIPE
            {int one = 1; setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));}
#endif
            {int one = 1; setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));}

            auto* conn = new Connection(cfd);
            {
                std::lock_guard lk(conns_m_);
                conns_.insert(conn);
            }

            // start by asking for read
            add_read_oneshot(cfd, conn);
        }
    }

    // -------------------------- read / write ----------------------------
    void process_read(Connection* c) {
        if (c->fd == -1) return;
        // read until headers done or EAGAIN
        for (;;) {
            ssize_t n = ::recv(c->fd, c->rbuf.data() + c->rlen, READ_BUF - c->rlen, 0);
            if (n > 0) {
                c->rlen += (size_t)n;
                if (has_double_crlf(c->rbuf.data(), c->rlen) || c->rlen == READ_BUF) break;
                continue;
            }
            if (n == 0) { close_conn(c); return; }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // need more data later; re-arm read and return
                add_read_oneshot(c->fd, c);
                return;
            }
            // other error
            close_conn(c);
            return;
        }

        // build write iovecs (header + body)
        const auto& hdr = (c->served + 1 < MAX_KEEPALIVE_REQ) ? RESP_HDR_KEEP : RESP_HDR_CLOSE;
        c->iov[0].iov_base = const_cast<char*>(hdr.data());
        c->iov[0].iov_len  = static_cast<unsigned>(hdr.size());
        c->iov[1].iov_base = const_cast<char*>(RESP_BODY.data());
        c->iov[1].iov_len  = static_cast<unsigned>(RESP_BODY.size());
        c->total_to_send   = hdr.size() + RESP_BODY.size();
        c->sent            = 0;

        // try immediate write
        if (!try_writev(c)) {
            // need EVFILT_WRITE notification
            add_write_oneshot(c->fd, c);
        }
    }

    void process_write(Connection* c) {
        if (c->fd == -1) return;
        if (!try_writev(c)) {
            // still not done, re-arm write
            add_write_oneshot(c->fd, c);
        }
    }

    bool try_writev(Connection* c) {
        // write until done or EAGAIN/error
        for (;;) {
            ssize_t n = ::writev(c->fd, c->iov, 2);
            if (n > 0) {
                c->sent += (size_t)n;
                size_t left = (size_t)n;
                for (int k = 0; k < 2 && left; ++k) {
                    size_t take = std::min(left, (size_t)c->iov[k].iov_len);
                    c->iov[k].iov_base = static_cast<char*>(c->iov[k].iov_base) + take;
                    c->iov[k].iov_len  -= take;
                    left -= take;
                }
                if (c->sent >= c->total_to_send) {
                    // finished response
                    c->served++;
                    c->rlen = 0; // reset read buffer for next request

                    if (c->served < MAX_KEEPALIVE_REQ) {
                        // go back to reading next request
                        add_read_oneshot(c->fd, c);
                    } else {
                        close_conn(c);
                    }
                    return true; // complete
                }
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return false; // need write readiness
            }
            // error
            close_conn(c);
            return true; // treat as complete (closed)
        }
    }

    void close_conn(Connection* c) {
        int fd = c->fd;
        if (fd != -1) {
            del_all_filters(fd, c);
            ::close(fd);
            c->fd = -1;
        }
        std::lock_guard lk(conns_m_);
        auto it = conns_.find(c);
        if (it != conns_.end()) {
            conns_.erase(it);
            delete c;
        }
    }

    // ------------------------------- state ------------------------------
    ThreadPool pool_;
    int kq_{-1};
    int listen_v4_{-1};
    int listen_v6_{-1};
    std::thread reactor_thread_;
    std::atomic<bool> stopping_{false};

    // connection lifetime tracking
    std::unordered_set<Connection*> conns_;
    std::mutex conns_m_;
};

// ================================= main ================================
int main() {
    raise_nofile(65536);

    int workers = MAX_WORKERS;
    if (const char* env = std::getenv("WORKERS")) {
        int v = std::atoi(env);
        if (v > 0) workers = v;
    }
    Server srv(workers);
    if (!srv.bind_and_listen(PORT)) {
        std::cerr << "Failed to bind on port " << PORT << "\n";
        return 1;
    }

    std::cout << "Listening on http://0.0.0.0:" << PORT
              << " (kqueue + thread pool, no coroutines)\n";

    // Keep process alive
    std::this_thread::sleep_for(std::chrono::hours(24 * 365));
    return 0;
}
