#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono;

constexpr int PORT = 8080;
constexpr int MAX_EVENTS = 1024;
constexpr int READ_BUF_SIZE = 4096;
constexpr int WRITE_BUF_SIZE = 4096;

// Keep-alive configuration
constexpr int KEEPALIVE_TIMEOUT_MS = 5000;
constexpr int MAX_KEEPALIVE_REQUESTS = 100;

struct Connection;
struct Worker;

static int make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    return fd;
}

enum class ConnState
{
    ReadingHeaders,
    Writing,
    Closing
};

struct Connection
{
    int fd{-1};
    ConnState state{ConnState::ReadingHeaders};

    alignas(64) char read_buf[READ_BUF_SIZE];
    size_t read_len = 0;

    alignas(64) char write_buf[WRITE_BUF_SIZE];
    size_t write_len = 0;
    size_t write_offset = 0;

    int requests_served = 0;
    steady_clock::time_point last_active{steady_clock::now()};

    void handle_read(Worker& w);
    void handle_write(Worker& w);
    void close_conn(Worker& w);
    void prepare_response();
    void reset_for_next_request(Worker& w);
    void enable_write(Worker& w);
    void disable_write(Worker& w);
};

struct HeapEntry
{
    steady_clock::time_point expiry;
    Connection* conn;
    bool operator>(const HeapEntry& other) const
    {
        return expiry > other.expiry;
    }
};

struct Worker
{
    int kq;
    int listen_fd;
    std::vector<struct kevent> events;
    std::vector<std::unique_ptr<Connection>> conns;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> timeout_heap;

    // Thread-local batch for kevent changes
    std::vector<struct kevent> pending_changes;

    Worker(int lfd)
        : listen_fd(lfd)
        , events(MAX_EVENTS)
    {
        kq = kqueue();
        if (kq < 0)
        {
            perror("kqueue");
            exit(1);
        }

        // Register listen_fd for read
        struct kevent ev;
        EV_SET(&ev, listen_fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }

    void flush_changes()
    {
        if (!pending_changes.empty())
        {
            kevent(kq, pending_changes.data(), pending_changes.size(), nullptr, 0, nullptr);
            pending_changes.clear();
        }
    }

    void add_connection(int fd)
    {
        auto conn = std::make_unique<Connection>();
        conn->fd = fd;
        conn->last_active = steady_clock::now();
        conn->state = ConnState::ReadingHeaders;

        // Register for read only initially (always enabled)
        struct kevent kev;
        EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, conn.get());
        pending_changes.push_back(kev);

        // Initially disable write
        EV_SET(&kev, fd, EVFILT_WRITE, EV_ADD | EV_DISABLE, 0, 0, conn.get());
        pending_changes.push_back(kev);

        // Insert into timeout heap
        timeout_heap.push({conn->last_active + milliseconds(KEEPALIVE_TIMEOUT_MS), conn.get()});

        conns.push_back(std::move(conn));
    }

    void check_timeouts()
    {
        auto now = steady_clock::now();
        while (!timeout_heap.empty())
        {
            auto top = timeout_heap.top();
            if (duration_cast<milliseconds>(top.expiry - now).count() > 0)
                break;

            timeout_heap.pop();
            Connection* c = top.conn;
            if (c->fd != -1)
            {
                c->close_conn(*this);
            }
        }
    }

    void run()
    {
        while (true)
        {
            flush_changes(); // push pending kevent changes

            int nfds = kevent(kq, nullptr, 0, events.data(), MAX_EVENTS, nullptr);

            //auto now = steady_clock::now();

            for (int i = 0; i < nfds; ++i)
            {
                auto& e = events[i];

                if (e.ident == (uintptr_t)listen_fd)
                {
                    // Accept loop
                    while (true)
                    {
                        sockaddr_in caddr{};
                        socklen_t clen = sizeof(caddr);
                        int cfd = ::accept(listen_fd, (sockaddr*)&caddr, &clen);
                        if (cfd < 0)
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                                break;
                            perror("accept");
                            break;
                        }
                        make_nonblocking(cfd);

                        // Optional: enable TCP_NODELAY
                        int yes = 1;
                        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

                        add_connection(cfd);
                    }
                }
                else
                {
                    auto* conn = static_cast<Connection*>(e.udata);
                    if (!conn || conn->fd == -1)
                        continue;

                    if (e.flags & (EV_ERROR | EV_EOF))
                    {
                        conn->close_conn(*this);
                        continue;
                    }
                    if (e.filter == EVFILT_READ)
                        conn->handle_read(*this);
                    if (conn->fd != -1 && e.filter == EVFILT_WRITE)
                        conn->handle_write(*this);
                }
            }

            check_timeouts();
        }
    }
};

void Connection::enable_write(Worker& w)
{
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_WRITE, EV_ENABLE, 0, 0, this);
    w.pending_changes.push_back(kev);
}

void Connection::disable_write(Worker& w)
{
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_WRITE, EV_DISABLE, 0, 0, this);
    w.pending_changes.push_back(kev);
}

void Connection::close_conn(Worker& w)
{
    struct kevent kev[2];
    EV_SET(&kev[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&kev[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    w.pending_changes.insert(w.pending_changes.end(), kev, kev + 2);

    ::close(fd);
    fd = -1;
}

void Connection::prepare_response()
{
    const char body[] = "Hello, World!\n";

    std::string header = "HTTP/1.1 200 OK\r\n"
                         "Content-Type: text/plain\r\n"
                         "Content-Length: 14\r\n";

    bool keep_alive = (requests_served < MAX_KEEPALIVE_REQUESTS);
    if (keep_alive)
        header += "Connection: keep-alive\r\n\r\n";
    else
        header += "Connection: close\r\n\r\n";

    size_t header_len = header.size();
    memcpy(write_buf, header.data(), header_len);
    memcpy(write_buf + header_len, body, sizeof(body) - 1);

    write_len = header_len + sizeof(body) - 1;
    write_offset = 0;
    state = ConnState::Writing;
}

void Connection::reset_for_next_request(Worker& w)
{
    read_len = 0;
    write_len = 0;
    write_offset = 0;
    state = ConnState::ReadingHeaders;
    last_active = steady_clock::now();
    ++requests_served;

    // Only disable write, keep read enabled
    disable_write(w);
}

void Connection::handle_read(Worker& w)
{
    last_active = steady_clock::now();

    while (true)
    {
        ssize_t n = ::recv(fd, read_buf + read_len, sizeof(read_buf) - read_len, 0);

        if (n > 0)
        {
            read_len += n;
            const char* end = (const char*)memmem(read_buf, read_len, "\r\n\r\n", 4);
            if (end)
            {
                prepare_response();
                enable_write(w); // enable write
                return;
            }
        }
        else if (n == 0)
        {
            state = ConnState::Closing;
            close_conn(w);
            return;
        }
        else if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            break;
        }
        else
        {
            state = ConnState::Closing;
            close_conn(w);
            return;
        }
    }
}

void Connection::handle_write(Worker& w)
{
    while (write_offset < write_len)
    {
        ssize_t n = ::send(fd, write_buf + write_offset, write_len - write_offset, 0);

        if (n > 0)
        {
            write_offset += n;
        }
        else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return;
        }
        else
        {
            state = ConnState::Closing;
            close_conn(w);
            return;
        }
    }

    if (requests_served + 1 < MAX_KEEPALIVE_REQUESTS)
    {
        reset_for_next_request(w);
    }
    else
    {
        state = ConnState::Closing;
        close_conn(w);
    }
}

int main()
{
    int cores = std::thread::hardware_concurrency();
    if (cores <= 0)
        cores = 4;

    std::vector<std::thread> workers;
    workers.reserve(cores);

    for (int i = 0; i < cores; ++i)
    {
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

        if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
        {
            perror("bind");
            return 1;
        }
        if (listen(listen_fd, SOMAXCONN) < 0)
        {
            perror("listen");
            return 1;
        }

        workers.emplace_back([listen_fd] {
            Worker w(listen_fd);
            w.run();
        });
    }

    for (auto& t : workers)
        t.join();
}
