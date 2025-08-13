#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>

using namespace std::chrono;

constexpr int PORT = 8080;
constexpr int MAX_EVENTS = 1024;
constexpr int READ_BUF_SIZE = 4096;
constexpr int WRITE_BUF_SIZE = 4096;

// Keep-alive configuration
constexpr int KEEPALIVE_TIMEOUT_MS = 5000;
constexpr int MAX_KEEPALIVE_REQUESTS = 100;

enum class ConnState
{
  ReadingHeaders,
  Writing,
  Closing
};

static int make_nonblocking(int fd)
{
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

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

  // kqueue handle is per-worker; pass it into ops:
  void handle_read(int kq);
  void handle_write(int kq);
  void close_conn(int kq);
  void prepare_response();
  void reset_for_next_request();

  // helpers for kqueue interest toggling
  void add_read_interest(int kq);
  void add_write_interest(int kq);
  void del_write_interest(int kq);
};

static inline void kevent_ctl(int kq, int fd, short filter, uint16_t flags, void* udata)
{
  struct kevent kev;
  EV_SET(&kev, fd, filter, flags, 0, 0, udata);
  if (kevent(kq, &kev, 1, nullptr, 0, nullptr) == -1)
  {
    // Non-fatal for add/delete races, but useful while developing:
    // perror("kevent change");
  }
}

void Connection::add_read_interest(int kq)
{
  kevent_ctl(kq, fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, this);
}

void Connection::add_write_interest(int kq)
{
  kevent_ctl(kq, fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, this);
}

void Connection::del_write_interest(int kq)
{
  kevent_ctl(kq, fd, EVFILT_WRITE, EV_DELETE, this);
}

void Connection::close_conn(int kq)
{
  // Remove both filters to stop future events
  kevent_ctl(kq, fd, EVFILT_READ, EV_DELETE, this);
  kevent_ctl(kq, fd, EVFILT_WRITE, EV_DELETE, this);
  ::close(fd);
  fd = -1;
}

void Connection::prepare_response()
{
  // Minimal HTTP/1.1 OK response with keep-alive support
  static constexpr char body[] = "Hello, World!\n";
  // Note: 14 bytes length (including \n)
  std::string header = "HTTP/1.1 200 OK\r\n"
                       "Content-Type: text/plain\r\n"
                       "Content-Length: 14\r\n";

  const bool keep_alive = (requests_served < MAX_KEEPALIVE_REQUESTS);
  if (keep_alive)
    header += "Connection: keep-alive\r\n\r\n";
  else
    header += "Connection: close\r\n\r\n";

  const size_t header_len = header.size();
  if (header_len + (sizeof(body) - 1) > sizeof(write_buf))
  {
    // Shouldn't happen in this demo; close if it does.
    state = ConnState::Closing;
    return;
  }

  std::memcpy(write_buf, header.data(), header_len);
  std::memcpy(write_buf + header_len, body, sizeof(body) - 1);

  write_len = header_len + sizeof(body) - 1;
  write_offset = 0;
  state = ConnState::Writing;
}

void Connection::reset_for_next_request()
{
  read_len = 0;
  write_len = 0;
  write_offset = 0;
  state = ConnState::ReadingHeaders;
  last_active = steady_clock::now();
  ++requests_served;
}

// Portable search for "\r\n\r\n" within a buffer.
static inline bool has_end_of_headers(const char* buf, size_t len)
{
  static const char pattern[] = "\r\n\r\n";
  const char* b = buf;
  const char* e = buf + len;
  auto it = std::search(b, e, std::begin(pattern), std::end(pattern) - 1);
  return it != e;
}

void Connection::handle_read(int kq)
{
  last_active = steady_clock::now();

  while (true)
  {
    ssize_t n = ::recv(fd, read_buf + read_len, sizeof(read_buf) - read_len, 0);

    if (n > 0)
    {
      read_len += static_cast<size_t>(n);
      // Naive parse: check for \r\n\r\n
      if (has_end_of_headers(read_buf, read_len))
      {
        prepare_response();
        if (state == ConnState::Writing)
          add_write_interest(kq); // enable write notifications only when needed
        return;
      }
      // If buffer fills without end-of-headers, you could 400 here. We keep reading.
      if (read_len == sizeof(read_buf))
      {
        // Simple safety: respond anyway.
        prepare_response();
        if (state == ConnState::Writing)
          add_write_interest(kq);
        return;
      }
    }
    else if (n == 0)
    {
      state = ConnState::Closing;
      close_conn(kq);
      return;
    }
    else
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      state = ConnState::Closing;
      close_conn(kq);
      return;
    }
  }
}

void Connection::handle_write(int kq)
{
  while (write_offset < write_len)
  {
    ssize_t n = ::send(fd, write_buf + write_offset, write_len - write_offset, 0);
    if (n > 0)
    {
      write_offset += static_cast<size_t>(n);
    }
    else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
    {
      // Need further write readiness notifications
      add_write_interest(kq);
      return;
    }
    else
    {
      state = ConnState::Closing;
      close_conn(kq);
      return;
    }
  }

  // Finished writing everything
  del_write_interest(kq); // avoid busy write notifications when we have nothing to send

  if (requests_served + 1 < MAX_KEEPALIVE_REQUESTS)
  {
    reset_for_next_request();
    // Keep read interest active; kqueue read filter remains installed.
  }
  else
  {
    state = ConnState::Closing;
    close_conn(kq);
  }
}

struct Worker
{
  int kq{-1};
  int listen_fd{-1};
  std::vector<struct kevent> events;
  std::vector<std::unique_ptr<Connection>> conns;

  Worker(int lfd)
    : listen_fd(lfd)
    , events(MAX_EVENTS)
  {
    kq = kqueue();
    if (kq == -1)
    {
      perror("kqueue");
      std::exit(1);
    }

    // Register the listening socket for read events (edge-like with EV_CLEAR)
    struct kevent kev;
    EV_SET(&kev, listen_fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
    if (kevent(kq, &kev, 1, nullptr, 0, nullptr) == -1)
    {
      perror("kevent add listen");
      std::exit(1);
    }
  }

  void run()
  {
    while (true)
    {
      timespec ts;
      ts.tv_sec = 1;
      ts.tv_nsec = 0;

      int nfds = kevent(kq, nullptr, 0, events.data(), static_cast<int>(events.size()), &ts);

      const auto now = steady_clock::now();

      for (int i = 0; i < nfds; ++i)
      {
        const auto& e = events[i];

        // Errors on the event itself
        if (e.flags & EV_ERROR)
        {
          if (e.udata)
          {
            auto* conn = static_cast<Connection*>(e.udata);
            if (conn->fd != -1)
              conn->close_conn(kq);
          }
          // For listen socket EV_ERROR, we just continue; errors would be logged via perror in accept path.
          continue;
        }

        // Listening socket readiness (udata == nullptr)
        if (e.udata == nullptr && e.filter == EVFILT_READ && static_cast<int>(e.ident) == listen_fd)
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

            // Make client non-blocking
            if (make_nonblocking(cfd) == -1)
            {
              ::close(cfd);
              continue;
            }

            // Avoid SIGPIPE on send() for this socket
            int one = 1;
            setsockopt(cfd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));

            auto conn = std::make_unique<Connection>();
            conn->fd = cfd;
            conn->state = ConnState::ReadingHeaders;
            conn->last_active = now;

            // Register for read events on this connection
            struct kevent kev_rd;
            EV_SET(&kev_rd, cfd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, conn.get());
            if (kevent(kq, &kev_rd, 1, nullptr, 0, nullptr) == -1)
            {
              perror("kevent add conn read");
              ::close(cfd);
              continue;
            }

            conns.push_back(std::move(conn));
          }
        }
        else
        {
          auto* conn = static_cast<Connection*>(e.udata);
          if (!conn || conn->fd == -1) continue;

          // Remote closed write end (EOF), or hangup
          if (e.flags & EV_EOF)
          {
            conn->close_conn(kq);
            continue;
          }

          if (e.filter == EVFILT_READ)
          {
            conn->handle_read(kq);
          }
          else if (e.filter == EVFILT_WRITE)
          {
            conn->handle_write(kq);
          }
        }
      }

      // Sweep idle connections
      for (auto& c : conns)
      {
        if (c->fd != -1 &&
            duration_cast<milliseconds>(now - c->last_active).count() > KEEPALIVE_TIMEOUT_MS)
        {
          c->close_conn(kq);
        }
      }
    }
  }
};

int main()
{
  int cores = std::thread::hardware_concurrency();
  if (cores <= 0)
    cores = 4;

  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cores));

  for (int i = 0; i < cores; ++i)
  {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
      perror("socket");
      return 1;
    }
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    if (make_nonblocking(listen_fd) == -1)
    {
      perror("nonblock listen");
      return 1;
    }

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

  return 0;
}
