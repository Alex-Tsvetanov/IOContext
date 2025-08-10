#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono;

constexpr int PORT = 8080;
constexpr int READ_BUF_SIZE = 4096;
constexpr int WRITE_BUF_SIZE = 4096;
constexpr int KEEPALIVE_TIMEOUT_MS = 5000;
constexpr int MAX_KEEPALIVE_REQUESTS = 100;

enum class ConnState {
    ReadingHeaders,
    Writing,
    Closing
};

struct Connection {
    int fd{-1};
    ConnState state{ConnState::ReadingHeaders};

    alignas(64) char read_buf[READ_BUF_SIZE];
    size_t read_len = 0;

    alignas(64) char write_buf[WRITE_BUF_SIZE];
    size_t write_len = 0;
    size_t write_offset = 0;

    int requests_served = 0;
    steady_clock::time_point last_active{steady_clock::now()};

    void prepare_response() {
        const char body[] = "Hello, World!\n";

        std::string header = "HTTP/1.1 200 OK\r\n"
                             "Content-Type: text/plain\r\n"
                             "Content-Length: 14\r\n";

        bool keep_alive = (requests_served < MAX_KEEPALIVE_REQUESTS);
        header += keep_alive ? "Connection: keep-alive\r\n\r\n"
                             : "Connection: close\r\n\r\n";

        size_t header_len = header.size();
        memcpy(write_buf, header.data(), header_len);
        memcpy(write_buf + header_len, body, sizeof(body) - 1);

        write_len = header_len + sizeof(body) - 1;
        write_offset = 0;
        state = ConnState::Writing;
    }

    void reset_for_next_request() {
        read_len = 0;
        write_len = 0;
        write_offset = 0;
        state = ConnState::ReadingHeaders;
        last_active = steady_clock::now();
        ++requests_served;
    }

    void close_conn() {
        if (fd != -1) {
            ::close(fd);
            fd = -1;
        }
        state = ConnState::Closing;
    }

    bool handle_read() {
        last_active = steady_clock::now();
        while (true) {
            ssize_t n = ::recv(fd, read_buf + read_len,
                               sizeof(read_buf) - read_len, 0);
            if (n > 0) {
                read_len += n;
                const char* end = (const char*)memmem(read_buf, read_len, "\r\n\r\n", 4);
                if (end) {
                    prepare_response();
                    return true; // ready to write
                }
            } else if (n == 0) {
                close_conn();
                return false;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                close_conn();
                return false;
            }
        }
        return false;
    }

    bool handle_write() {
        while (write_offset < write_len) {
            ssize_t n = ::send(fd, write_buf + write_offset,
                               write_len - write_offset, 0);
            if (n > 0) {
                write_offset += n;
            } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return true; // not finished
            } else {
                close_conn();
                return false;
            }
        }

        // finished writing
        if (requests_served + 1 < MAX_KEEPALIVE_REQUESTS) {
            reset_for_next_request();
            return true; // keep alive
        } else {
            close_conn();
            return false;
        }
    }
};

static int make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    return fd;
}

struct Worker {
    int listen_fd;
    std::vector<pollfd> fds;
    std::vector<Connection*> conns;
    std::vector<std::unique_ptr<Connection>> owned_conns;

    Worker(int lfd) : listen_fd(lfd) {
        fds.reserve(1024);
        conns.reserve(1024);
        fds.push_back({listen_fd, POLLIN, 0});
        conns.push_back(nullptr);
    }

    void add_conn(std::unique_ptr<Connection> conn) {
        fds.push_back({conn->fd, POLLIN, 0});
        conns.push_back(conn.get());
        owned_conns.push_back(std::move(conn));
    }

    void remove_slot(size_t idx) {
        size_t last = fds.size() - 1;
        if (idx != last) {
            fds[idx] = fds[last];
            conns[idx] = conns[last];
        }
        fds.pop_back();
        conns.pop_back();
    }

    void run() {
        while (true) {
            int nfds = ::poll(fds.data(), fds.size(), 1000);
            auto now = steady_clock::now();

            if (nfds <= 0) continue;

            for (size_t i = 0; i < fds.size();) {
                auto& pfd = fds[i];
                auto* conn = conns[i];

                if (pfd.revents == 0) {
                    ++i;
                    continue;
                }

                if (conn == nullptr) {
                    // Listening socket
                    while (true) {
                        sockaddr_in caddr{};
                        socklen_t clen = sizeof(caddr);
                        int cfd = ::accept(listen_fd, (sockaddr*)&caddr, &clen);
                        if (cfd < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            perror("accept");
                            break;
                        }
                        make_nonblocking(cfd);

                        auto conn_ptr = std::make_unique<Connection>();
                        conn_ptr->fd = cfd;
                        conn_ptr->state = ConnState::ReadingHeaders;
                        conn_ptr->last_active = now;
                        add_conn(std::move(conn_ptr));
                    }
                    ++i;
                    continue;
                }

                bool alive = true;
                if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
                    conn->close_conn();
                    alive = false;
                } else {
                    if (pfd.revents & POLLIN) {
                        bool ready_to_write = conn->handle_read();
                        if (conn->fd == -1) alive = false;
                        else if (ready_to_write) pfd.events |= POLLOUT;
                    }
                    if (alive && (pfd.revents & POLLOUT)) {
                        alive = conn->handle_write();
                        pfd.events = alive ? POLLIN | POLLOUT : 0;
                    }
                }

                if (!alive || conn->fd == -1) {
                    remove_slot(i); // compact
                } else {
                    ++i;
                }
            }

            // Sweep idle connections
            for (size_t i = 1; i < fds.size();) {
                auto* c = conns[i];
                if (!c || c->fd == -1) {
                    remove_slot(i);
                    continue;
                }
                if (duration_cast<milliseconds>(now - c->last_active).count() >
                    KEEPALIVE_TIMEOUT_MS) {
                    c->close_conn();
                    remove_slot(i);
                    continue;
                }
                ++i;
            }
        }
    }
};

int main() {
    int cores = std::thread::hardware_concurrency();
    if (cores <= 0) cores = 4;

    std::vector<std::thread> workers;
    workers.reserve(cores);

    for (int i = 0; i < cores; ++i) {
        int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int yes = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
        make_nonblocking(listen_fd);

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(PORT);

        if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            perror("bind");
            return 1;
        }
        if (listen(listen_fd, SOMAXCONN) < 0) {
            perror("listen");
            return 1;
        }

        workers.emplace_back([listen_fd] {
            Worker w(listen_fd);
            w.run();
        });
    }

    for (auto& t : workers) t.join();
}
