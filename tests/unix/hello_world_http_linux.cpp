#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
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

constexpr int PORT = 8082;
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

  void handle_read(int epfd);
  void handle_write(int epfd);
  void close_conn(int epfd);
  void prepare_response();
  void reset_for_next_request();
  void modify_epoll(int epfd, uint32_t events);
};

static int make_nonblocking(int fd)
{
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  return fd;
}

void Connection::modify_epoll(int epfd, uint32_t events)
{
  epoll_event ev{};
  ev.events = events | EPOLLET;
  ev.data.ptr = this;
  epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

void Connection::close_conn(int epfd)
{
  epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
  ::close(fd);
  fd = -1;
}

void Connection::prepare_response()
{
  // Minimal HTTP/1.1 OK response with keep-alive support
  const char body[] = "Hello, World!\n";

  std::string header = "HTTP/1.1 200 OK\r\n"
                       "Content-Type: text/plain\r\n"
                       "Content-Length: 14\r\n";

  // Determine if we keep-alive
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

void Connection::reset_for_next_request()
{
  read_len = 0;
  write_len = 0;
  write_offset = 0;
  state = ConnState::ReadingHeaders;
  last_active = steady_clock::now();
  ++requests_served;
}

void Connection::handle_read(int epfd)
{
  last_active = steady_clock::now();

  while (true)
  {
    ssize_t n = ::recv(fd, read_buf + read_len, sizeof(read_buf) - read_len, 0);

    if (n > 0)
    {
      read_len += n;
      // Naive parse: check for \r\n\r\n
      const char* end = (const char*) memmem(read_buf, read_len, "\r\n\r\n", 4);
      if (end)
      {
        prepare_response();
        modify_epoll(epfd, EPOLLIN | EPOLLOUT);
        return;
      }
    }
    else if (n == 0)
    {
      state = ConnState::Closing;
      close_conn(epfd);
      return;
    }
    else if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
      break;
    }
    else
    {
      state = ConnState::Closing;
      close_conn(epfd);
      return;
    }
  }
}

void Connection::handle_write(int epfd)
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
      close_conn(epfd);
      return;
    }
  }

  // Finished writing
  if (requests_served + 1 < MAX_KEEPALIVE_REQUESTS)
  {
    reset_for_next_request();
    modify_epoll(epfd, EPOLLIN); // Back to reading
  }
  else
  {
    state = ConnState::Closing;
    close_conn(epfd);
  }
}

struct Worker
{
  int epfd;
  int listen_fd;
  std::vector<epoll_event> events;
  std::vector<std::unique_ptr<Connection>> conns;

  Worker(int lfd)
    : listen_fd(lfd)
    , events(MAX_EVENTS)
  {
    epfd = epoll_create1(0);

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = nullptr;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);
  }

  void run()
  {
    while (true)
    {
      int nfds = epoll_wait(epfd, events.data(), MAX_EVENTS, 1000);

      auto now = steady_clock::now();

      for (int i = 0; i < nfds; ++i)
      {
        epoll_event& e = events[i];

        if (!e.data.ptr)
        {
          // Accept loop
          while (true)
          {
            sockaddr_in caddr{};
            socklen_t clen = sizeof(caddr);
            int cfd = ::accept4(listen_fd, (sockaddr*) &caddr, &clen, SOCK_NONBLOCK);
            if (cfd < 0)
            {
              if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
              perror("accept");
              break;
            }

            auto conn = std::make_unique<Connection>();
            conn->fd = cfd;
            conn->state = ConnState::ReadingHeaders;
            conn->last_active = now;

            epoll_event cev{};
            cev.events = EPOLLIN | EPOLLET;
            cev.data.ptr = conn.get();
            epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);

            conns.push_back(std::move(conn));
          }
        }
        else
        {
          auto* conn = static_cast<Connection*>(e.data.ptr);
          if (conn->fd == -1)
            continue;

          if ((e.events & EPOLLERR) || (e.events & EPOLLHUP))
          {
            conn->close_conn(epfd);
            continue;
          }
          if (e.events & EPOLLIN)
            conn->handle_read(epfd);
          if (conn->fd != -1 && e.events & EPOLLOUT)
            conn->handle_write(epfd);
        }
      }

      // Sweep idle connections
      for (auto& c : conns)
      {
        if (c->fd != -1 && duration_cast<milliseconds>(now - c->last_active).count() > KEEPALIVE_TIMEOUT_MS)
        {
          c->close_conn(epfd);
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
  workers.reserve(cores);

  for (int i = 0; i < cores; ++i)
  {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    make_nonblocking(listen_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, (sockaddr*) &addr, sizeof(addr)) < 0)
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
