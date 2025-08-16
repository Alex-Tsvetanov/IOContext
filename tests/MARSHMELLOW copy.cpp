// Cross-platform HTTP/1.1 hello server with internal multi-threading
// Backends: Linux(epoll) with SO_REUSEPORT workers,
//           macOS(kqueue) with SO_REUSEPORT workers,
//           Windows(IOCP) worker pool.
// C++20, TCP sockets only, low-level C socket APIs.

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <mutex>

using namespace std;
using namespace std::chrono;

// -------------------------------
// Common configuration
// -------------------------------
static constexpr int PORT = 8080;
static constexpr int BACKLOG = 512;
static constexpr int READ_BUF_SIZE = 8192;
static constexpr int WRITE_BUF_SIZE = 4096;
static constexpr int MAX_EVENTS = 1024;
static constexpr int KEEPALIVE_TIMEOUTMS = 5000;
static constexpr int MAX_KEEPALIVE_REQS = 100;

// Response payload
static constexpr char kBody[] = "Hello, World!\n";
static constexpr size_t kBodyLen = sizeof(kBody) - 1;

static constexpr char kHdrKeepAlive[] = "HTTP/1.1 200 OK\r\n"
                                        "Server: cp-http/0.2\r\n"
                                        "Content-Type: text/plain\r\n"
                                        "Connection: keep-alive\r\n"
                                        "Content-Length: ";
static constexpr char kHdrClose[] = "HTTP/1.1 200 OK\r\n"
                                    "Server: cp-http/0.2\r\n"
                                    "Content-Type: text/plain\r\n"
                                    "Connection: close\r\n"
                                    "Content-Length: ";
static constexpr char kCRLF[] = "\r\n";

// -------------------------------
// Platform split
// -------------------------------
#if defined(_WIN32)
  // -------- Windows / IOCP --------
  #define WIN32_LEAN_AND_MEAN
  #include <WinSock2.h>
  #include <Windows.h>
  #include <MSWSock.h>
  #pragma comment(lib, "Ws2_32.lib")
  #pragma comment(lib, "Mswsock.lib")
using PlatformSocket = SOCKET;
static constexpr PlatformSocket kInvalidSock = INVALID_SOCKET;
static inline bool is_wouldblock(int err)
{
  return err == WSAEWOULDBLOCK;
}

#else
  // -------- POSIX (Linux/macOS) ---
  #include <arpa/inet.h>
  #include <fcntl.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <unistd.h>
  #include <unistd.h>
using PlatformSocket = int;
static constexpr PlatformSocket kInvalidSock = -1;
  #include <cerrno>
static inline bool is_wouldblock(int err)
{
  return err == EAGAIN || err == EWOULDBLOCK;
}
#endif

#if defined(__APPLE__)
  #include <sys/event.h> // kqueue
#elif defined(__linux__)
  #include <sys/epoll.h> // epoll
  #include <signal.h>
#endif

// -------------------------------
// Common helpers
// -------------------------------
static inline int set_nonblocking(PlatformSocket fd)
{
#if defined(_WIN32)
  u_long on = 1;
  return ioctlsocket(fd, FIONBIO, &on) == 0 ? 0 : -1;
#else
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
}

static inline void set_tcp_nodelay(PlatformSocket fd)
{
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*) &one, sizeof(one));
}

static inline void set_reuse(PlatformSocket fd)
{
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*) &one, sizeof(one));
#if defined(__APPLE__) || defined(__linux__)
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (char*) &one, sizeof(one));
#endif
}

#if defined(__APPLE__)
static inline void set_no_sigpipe(PlatformSocket fd)
{
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
}
#endif

static inline bool has_double_crlf(const char* buf, size_t len)
{
  if (len < 4)
    return false;
  for (size_t i = 3; i < len; ++i)
  {
    if (buf[i - 3] == '\r' && buf[i - 2] == '\n' && buf[i - 1] == '\r' && buf[i] == '\n')
      return true;
  }
  return false;
}

// -------------------------------
// Shared connection core
// -------------------------------
enum class ConnState
{
  ReadingHeaders,
  Writing,
  Closing
};

struct Connection
{
  PlatformSocket fd{kInvalidSock};
  ConnState state{ConnState::ReadingHeaders};

  alignas(64) char read_buf[READ_BUF_SIZE];
  size_t read_len{0};

  alignas(64) char write_buf[WRITE_BUF_SIZE];
  size_t write_len{0};
  size_t write_off{0};

  int served{0};
  steady_clock::time_point last_active{steady_clock::now()};

  void reset_for_next()
  {
    read_len = 0;
    write_len = 0;
    write_off = 0;
    state = ConnState::ReadingHeaders;
    last_active = steady_clock::now();
  }

  void prepare_response(bool keepalive)
  {
    char* p = write_buf;
    size_t cap = WRITE_BUF_SIZE;

    auto append = [&](const char* s, size_t n) {
      if (n > cap)
        n = cap;
      memcpy(p, s, n);
      p += n;
      cap -= n;
    };
    auto append_num = [&](size_t v) {
      char tmp[32];
      int n = snprintf(tmp, sizeof(tmp), "%zu", v);
      append(tmp, (size_t) n);
    };

    if (keepalive && served + 1 < MAX_KEEPALIVE_REQS)
      append(kHdrKeepAlive, sizeof(kHdrKeepAlive) - 1);
    else
      append(kHdrClose, sizeof(kHdrClose) - 1);
    append_num(kBodyLen);
    append(kCRLF, 2);
    append(kCRLF, 2);
    append(kBody, kBodyLen);

    write_len = WRITE_BUF_SIZE - cap;
    write_off = 0;
    state = ConnState::Writing;
  }

  bool is_idle_timeout() const
  {
    return duration_cast<milliseconds>(steady_clock::now() - last_active).count() > KEEPALIVE_TIMEOUTMS;
  }
};

// ====================================================================
// POSIX: Multi-threaded servers (internal worker pool via SO_REUSEPORT)
// ====================================================================
#if !defined(_WIN32)

static PlatformSocket create_listen_socket(uint16_t port)
{
  PlatformSocket s = ::socket(AF_INET, SOCK_STREAM, 0);
  if (s == kInvalidSock)
    return kInvalidSock;
  set_reuse(s);
  set_nonblocking(s);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(s, (sockaddr*) &addr, sizeof(addr)) < 0)
  {
    ::close(s);
    return kInvalidSock;
  }
  if (::listen(s, BACKLOG) < 0)
  {
    ::close(s);
    return kInvalidSock;
  }
  return s;
}

static void configure_client_socket(PlatformSocket cfd)
{
  set_nonblocking(cfd);
  set_tcp_nodelay(cfd);
  #if defined(__APPLE__)
  set_no_sigpipe(cfd);
  #endif
}

#endif // !WIN32

// -------------------------------------
// Linux: EpollServer (multi-threaded)
// -------------------------------------
#if defined(__linux__)
struct EpollServer
{
  struct Worker
  {
    int id{-1};
    PlatformSocket lfd{kInvalidSock};
    int epfd{-1};
    vector<unique_ptr<Connection>> conns;
    thread thr;

    bool init(uint16_t port, int worker_id)
    {
  #if defined(SIGPIPE)
      signal(SIGPIPE, SIG_IGN); // still pass MSG_NOSIGNAL on send()
  #endif
      id = worker_id;
      lfd = create_listen_socket(port);
      if (lfd == kInvalidSock)
      {
        perror("listen socket");
        return false;
      }
      epfd = epoll_create1(0);
      if (epfd < 0)
      {
        perror("epoll_create1");
        return false;
      }
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLET;
      ev.data.fd = lfd;
      if (epoll_ctl(epfd, EPOLL_CTL_ADD, lfd, &ev) < 0)
      {
        perror("epoll add listen");
        return false;
      }
      return true;
    }

    void add_client(PlatformSocket cfd)
    {
      auto c = make_unique<Connection>();
      c->fd = cfd;
      configure_client_socket(cfd);
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLET;
      ev.data.fd = cfd;
      epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &ev);
      conns.emplace_back(std::move(c));
    }

    Connection* find_conn(PlatformSocket fd)
    {
      for (auto& c : conns)
        if (c->fd == fd)
          return c.get();
      return nullptr;
    }

    void close_conn(Connection* c)
    {
      if (!c || c->fd == kInvalidSock)
        return;
      epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, nullptr);
      ::close(c->fd);
      c->fd = kInvalidSock;
    }

    void accept_loop()
    {
      while (true)
      {
        sockaddr_in inaddr{};
        socklen_t inlen = sizeof(inaddr);
        PlatformSocket cfd = accept4(lfd, (sockaddr*) &inaddr, &inlen, SOCK_NONBLOCK);
        if (cfd == kInvalidSock)
        {
          int err = errno;
          if (is_wouldblock(err))
            break;
          if (err == EINTR)
            continue;
          perror("accept");
          break;
        }
        set_tcp_nodelay(cfd);
        add_client(cfd);
      }
    }

    void drive_read(Connection* c)
    {
      while (true)
      {
        ssize_t n = ::recv(c->fd, c->read_buf + c->read_len, READ_BUF_SIZE - c->read_len, 0);
        if (n > 0)
        {
          c->read_len += (size_t) n;
          c->last_active = steady_clock::now();
          if (has_double_crlf(c->read_buf, c->read_len))
          {
            bool keepalive = true;
            c->prepare_response(keepalive);
            while (c->write_off < c->write_len)
            {
              ssize_t wn = ::send(c->fd, c->write_buf + c->write_off, c->write_len - c->write_off,
  #ifdef MSG_NOSIGNAL
                                  MSG_NOSIGNAL
  #else
                                  0
  #endif
              );
              if (wn > 0)
              {
                c->write_off += (size_t) wn;
                c->last_active = steady_clock::now();
              }
              else
              {
                if (wn < 0 && is_wouldblock(errno))
                {
                  epoll_event ev{};
                  ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                  ev.data.fd = c->fd;
                  epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
                  return;
                }
                else
                {
                  close_conn(c);
                  return;
                }
              }
            }
            c->served++;
            if (c->served < MAX_KEEPALIVE_REQS)
            {
              c->reset_for_next();
              epoll_event ev{};
              ev.events = EPOLLIN | EPOLLET;
              ev.data.fd = c->fd;
              epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
            }
            else
            {
              close_conn(c);
            }
            return;
          }
          if (c->read_len == READ_BUF_SIZE)
          {
            close_conn(c);
            return;
          }
          continue; // keep draining
        }
        else if (n == 0)
        {
          close_conn(c);
          return;
        }
        else
        {
          if (is_wouldblock(errno))
            return;
          close_conn(c);
          return;
        }
      }
    }

    void drive_write(Connection* c)
    {
      while (c->write_off < c->write_len)
      {
        ssize_t wn = ::send(c->fd, c->write_buf + c->write_off, c->write_len - c->write_off,
  #ifdef MSG_NOSIGNAL
                            MSG_NOSIGNAL
  #else
                            0
  #endif
        );
        if (wn > 0)
        {
          c->write_off += (size_t) wn;
          c->last_active = steady_clock::now();
        }
        else
        {
          if (wn < 0 && is_wouldblock(errno))
            return;
          close_conn(c);
          return;
        }
      }
      c->served++;
      if (c->served < MAX_KEEPALIVE_REQS)
      {
        c->reset_for_next();
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = c->fd;
        epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ev);
      }
      else
      {
        close_conn(c);
      }
    }

    void loop()
    {
      vector<epoll_event> events(MAX_EVENTS);
      while (true)
      {
        int n = epoll_wait(epfd, events.data(), (int) events.size(), 1000);
        if (n < 0)
        {
          if (errno == EINTR)
            continue;
          perror("epoll_wait");
          break;
        }
        for (int i = 0; i < n; ++i)
        {
          auto& ev = events[i];
          if (ev.data.fd == lfd)
          {
            accept_loop();
            continue;
          }
          Connection* c = find_conn(ev.data.fd);
          if (!c)
          {
            ::close(ev.data.fd);
            continue;
          }
          if (ev.events & (EPOLLERR | EPOLLHUP))
          {
            close_conn(c);
            continue;
          }
          if (ev.events & EPOLLIN)
            drive_read(c);
          if (c->fd != kInvalidSock && (ev.events & EPOLLOUT))
            drive_write(c);
        }
        // Idle sweep + compaction
        for (auto& c : conns)
        {
          if (c->fd != kInvalidSock && c->is_idle_timeout())
            close_conn(c.get());
        }
        conns.erase(remove_if(conns.begin(), conns.end(), [](auto& c) { return c->fd == kInvalidSock; }), conns.end());
      }
    }
  }; // Worker

  vector<unique_ptr<Worker>> workers;

  bool init(uint16_t port, unsigned threads = thread::hardware_concurrency())
  {
    if (threads == 0)
      threads = 1;
    workers.reserve(threads);
    for (unsigned i = 0; i < threads; ++i)
    {
      auto w = make_unique<Worker>();
      if (!w->init(port, (int) i))
        return false;
      workers.emplace_back(std::move(w));
    }
    return true;
  }

  void run()
  {
    for (auto& w : workers)
      w->thr = thread([w = w.get()] { w->loop(); });
    std::cout << "epoll HTTP (" << workers.size() << " threads) on 0.0.0.0:" << PORT << "\n";
    for (auto& w : workers)
      w->thr.join();
  }
};
#endif // __linux__

// -------------------------------------
// macOS: KqueueServer (multi-threaded)
// -------------------------------------
#if defined(__APPLE__)
struct KqueueServer
{
  struct Worker
  {
    int id{-1};
    PlatformSocket lfd{kInvalidSock};
    int kq{-1};
    vector<unique_ptr<Connection>> conns;
    thread thr;

    bool init(uint16_t port, int worker_id)
    {
      id = worker_id;
      lfd = create_listen_socket(port);
      if (lfd == kInvalidSock)
      {
        perror("listen socket");
        return false;
      }
      kq = kqueue();
      if (kq < 0)
      {
        perror("kqueue");
        return false;
      }
      struct kevent ev{};
      EV_SET(&ev, lfd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
      if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0)
      {
        perror("kevent add listen");
        return false;
      }
      return true;
    }

    void add_client(PlatformSocket cfd)
    {
      auto c = make_unique<Connection>();
      c->fd = cfd;
      configure_client_socket(cfd);
      struct kevent ev{};
      EV_SET(&ev, cfd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, nullptr);
      kevent(kq, &ev, 1, nullptr, 0, nullptr);
      conns.emplace_back(std::move(c));
    }

    Connection* find_conn(PlatformSocket fd)
    {
      for (auto& c : conns)
        if (c->fd == fd)
          return c.get();
      return nullptr;
    }

    void close_conn(Connection* c)
    {
      if (!c || c->fd == kInvalidSock)
        return;
      struct kevent ev[2];
      EV_SET(&ev[0], c->fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
      EV_SET(&ev[1], c->fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
      kevent(kq, ev, 2, nullptr, 0, nullptr);
      ::close(c->fd);
      c->fd = kInvalidSock;
    }

    void accept_loop()
    {
      while (true)
      {
        sockaddr_in inaddr{};
        socklen_t inlen = sizeof(inaddr);
        PlatformSocket cfd = ::accept(lfd, (sockaddr*) &inaddr, &inlen);
        if (cfd == kInvalidSock)
        {
          int err = errno;
          if (is_wouldblock(err))
            break;
          if (err == EINTR)
            continue;
          perror("accept");
          break;
        }
        configure_client_socket(cfd);
        add_client(cfd);
      }
    }

    void arm_write(PlatformSocket fd, bool on)
    {
      struct kevent ev{};
      EV_SET(&ev, fd, EVFILT_WRITE, (on ? (EV_ADD | EV_ENABLE | EV_CLEAR) : EV_DELETE), 0, 0, nullptr);
      kevent(kq, &ev, 1, nullptr, 0, nullptr);
    }

    void drive_read(Connection* c)
    {
      while (true)
      {
        ssize_t n = ::recv(c->fd, c->read_buf + c->read_len, READ_BUF_SIZE - c->read_len, 0);
        if (n > 0)
        {
          c->read_len += (size_t) n;
          c->last_active = steady_clock::now();
          if (has_double_crlf(c->read_buf, c->read_len))
          {
            bool keepalive = true;
            c->prepare_response(keepalive);
            while (c->write_off < c->write_len)
            {
              ssize_t wn = ::send(c->fd, c->write_buf + c->write_off, c->write_len - c->write_off, 0);
              if (wn > 0)
              {
                c->write_off += (size_t) wn;
                c->last_active = steady_clock::now();
              }
              else
              {
                if (wn < 0 && is_wouldblock(errno))
                {
                  arm_write(c->fd, true);
                  return;
                }
                close_conn(c);
                return;
              }
            }
            c->served++;
            if (c->served < MAX_KEEPALIVE_REQS)
            {
              c->reset_for_next();
              arm_write(c->fd, false);
            }
            else
            {
              close_conn(c);
            }
            return;
          }
          if (c->read_len == READ_BUF_SIZE)
          {
            close_conn(c);
            return;
          }
        }
        else if (n == 0)
        {
          close_conn(c);
          return;
        }
        else
        {
          if (is_wouldblock(errno))
            return;
          close_conn(c);
          return;
        }
      }
    }

    void drive_write(Connection* c)
    {
      while (c->write_off < c->write_len)
      {
        ssize_t wn = ::send(c->fd, c->write_buf + c->write_off, c->write_len - c->write_off, 0);
        if (wn > 0)
        {
          c->write_off += (size_t) wn;
          c->last_active = steady_clock::now();
        }
        else
        {
          if (wn < 0 && is_wouldblock(errno))
            return;
          close_conn(c);
          return;
        }
      }
      c->served++;
      if (c->served < MAX_KEEPALIVE_REQS)
      {
        c->reset_for_next();
        arm_write(c->fd, false);
      }
      else
      {
        close_conn(c);
      }
    }

    void loop()
    {
      vector<struct kevent> evs(MAX_EVENTS);
      while (true)
      {
        timespec ts{1, 0}; // 1s timeout for idle sweep
        int n = kevent(kq, nullptr, 0, evs.data(), (int) evs.size(), &ts);
        if (n < 0)
        {
          if (errno == EINTR)
            continue;
          perror("kevent");
          break;
        }
        for (int i = 0; i < n; ++i)
        {
          auto& ev = evs[i];
          if ((int) ev.ident == lfd && ev.filter == EVFILT_READ)
          {
            accept_loop();
            continue;
          }
          Connection* c = find_conn((int) ev.ident);
          if (!c)
          {
            ::close((int) ev.ident);
            continue;
          }
          if (ev.flags & (EV_EOF | EV_ERROR))
          {
            close_conn(c);
            continue;
          }
          if (ev.filter == EVFILT_READ)
            drive_read(c);
          if (c->fd != kInvalidSock && ev.filter == EVFILT_WRITE)
            drive_write(c);
        }
        for (auto& c : conns)
        {
          if (c->fd != kInvalidSock && c->is_idle_timeout())
            close_conn(c.get());
        }
        conns.erase(remove_if(conns.begin(), conns.end(), [](auto& c) { return c->fd == kInvalidSock; }), conns.end());
      }
    }
  }; // Worker

  vector<unique_ptr<Worker>> workers;

  bool init(uint16_t port, unsigned threads = thread::hardware_concurrency())
  {
    if (threads == 0)
      threads = 1;
    workers.reserve(threads);
    for (unsigned i = 0; i < threads; ++i)
    {
      auto w = make_unique<Worker>();
      if (!w->init(port, (int) i))
        return false;
      workers.emplace_back(std::move(w));
    }
    return true;
  }

  void run()
  {
    for (auto& w : workers)
      w->thr = thread([w = w.get()] { w->loop(); });
    std::cout << "kqueue HTTP (" << workers.size() << " threads) on 0.0.0.0:" << PORT << "\n";
    for (auto& w : workers)
      w->thr.join();
  }
};
#endif // __APPLE__

// ====================================================================
// Windows IOCP backend (kept as multi-threaded from before)
// ====================================================================
#if defined(_WIN32)
enum class IoOp
{
  Read,
  Write
};

struct PerIo
{
  OVERLAPPED ol{};
  WSABUF wbuf{};
  IoOp op{IoOp::Read};
  DWORD transferred{0};
  char buf[READ_BUF_SIZE];
};

struct IocpServer
{
  PlatformSocket lfd{kInvalidSock};
  HANDLE iocp{NULL};
  vector<unique_ptr<Connection>> conns;
  mutex conns_mtx;
  atomic<bool> stop{false};

  bool init(uint16_t port, unsigned /*threadsIgnored*/ = 0)
  {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
    {
      cerr << "WSAStartup failed\n";
      return false;
    }
    lfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (lfd == INVALID_SOCKET)
    {
      cerr << "socket() failed\n";
      return false;
    }
    set_reuse(lfd);
    set_nonblocking(lfd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (bind(lfd, (sockaddr*) &addr, sizeof(addr)) == SOCKET_ERROR)
    {
      cerr << "bind failed: " << WSAGetLastError() << "\n";
      return false;
    }
    if (listen(lfd, BACKLOG) == SOCKET_ERROR)
    {
      cerr << "listen failed\n";
      return false;
    }

    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (!iocp)
    {
      cerr << "CreateIoCompletionPort failed\n";
      return false;
    }
    CreateIoCompletionPort((HANDLE) lfd, iocp, 0, 0);
    return true;
  }

  void add_conn(PlatformSocket s)
  {
    set_nonblocking(s);
    set_tcp_nodelay(s);
    auto c = make_unique<Connection>();
    c->fd = s;
    {
      lock_guard<mutex> lk(conns_mtx);
      conns.emplace_back(std::move(c));
    }
    Connection* cp = conns.back().get();
    CreateIoCompletionPort((HANDLE) s, iocp, (ULONG_PTR) cp, 0);
    post_read(cp);
  }

  void remove_closed()
  {
    lock_guard<mutex> lk(conns_mtx);
    conns.erase(remove_if(conns.begin(), conns.end(), [](auto& c) { return c->fd == INVALID_SOCKET; }), conns.end());
  }

  void close_conn(Connection* c)
  {
    if (!c || c->fd == INVALID_SOCKET)
      return;
    closesocket(c->fd);
    c->fd = INVALID_SOCKET;
  }

  void accept_thread()
  {
    while (!stop.load())
    {
      sockaddr_in inaddr{};
      int inlen = sizeof(inaddr);
      PlatformSocket s = ::accept(lfd, (sockaddr*) &inaddr, &inlen);
      if (s == INVALID_SOCKET)
      {
        int e = WSAGetLastError();
        if (is_wouldblock(e))
        {
          this_thread::sleep_for(1ms);
          continue;
        }
        if (e == WSAEINTR)
          continue;
        continue;
      }
      add_conn(s);
    }
  }

  void post_read(Connection* c)
  {
    auto ctx = new PerIo();
    ctx->op = IoOp::Read;
    ctx->wbuf.buf = ctx->buf;
    ctx->wbuf.len = READ_BUF_SIZE;
    DWORD flags = 0, recvd = 0;
    int rc = WSARecv(c->fd, &ctx->wbuf, 1, &recvd, &flags, &ctx->ol, NULL);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      delete ctx;
      close_conn(c);
    }
  }

  void post_write(Connection* c)
  {
    auto ctx = new PerIo();
    ctx->op = IoOp::Write;
    ctx->wbuf.buf = c->write_buf + c->write_off;
    ctx->wbuf.len = static_cast<ULONG>(c->write_len - c->write_off);
    DWORD sent = 0;
    int rc = WSASend(c->fd, &ctx->wbuf, 1, &sent, 0, &ctx->ol, NULL);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      delete ctx;
      close_conn(c);
    }
  }

  void worker_thread()
  {
    while (!stop.load())
    {
      DWORD bytes = 0;
      ULONG_PTR key = 0;
      LPOVERLAPPED pol = nullptr;
      BOOL ok = GetQueuedCompletionStatus(iocp, &bytes, &key, &pol, 1000);
      if (!ok && pol == nullptr)
      {
        vector<Connection*> snapshot;
        {
          lock_guard<mutex> lk(conns_mtx);
          for (auto& c : conns)
            if (c->fd != INVALID_SOCKET)
              snapshot.push_back(c.get());
        }
        for (auto* c : snapshot)
          if (c->is_idle_timeout())
            close_conn(c);
        remove_closed();
        continue;
      }
      if (!pol)
        continue;
      auto* ctx = CONTAINING_RECORD(pol, PerIo, ol);
      Connection* c = reinterpret_cast<Connection*>(key);
      if (!c || c->fd == INVALID_SOCKET)
      {
        delete ctx;
        continue;
      }

      if (ctx->op == IoOp::Read)
      {
        if (!ok || bytes == 0)
        {
          delete ctx;
          close_conn(c);
          continue;
        }
        size_t copy = min((size_t) bytes, READ_BUF_SIZE - c->read_len);
        memcpy(c->read_buf + c->read_len, ctx->buf, copy);
        c->read_len += copy;
        c->last_active = steady_clock::now();
        delete ctx;

        if (has_double_crlf(c->read_buf, c->read_len))
        {
          c->prepare_response(true);
          post_write(c);
        }
        else if (c->read_len < READ_BUF_SIZE)
        {
          post_read(c);
        }
        else
        {
          close_conn(c);
        }
      }
      else
      {
        if (!ok)
        {
          delete ctx;
          close_conn(c);
          continue;
        }
        c->write_off += bytes;
        c->last_active = steady_clock::now();
        delete ctx;

        if (c->write_off < c->write_len)
          post_write(c);
        else
        {
          c->served++;
          if (c->served < MAX_KEEPALIVE_REQS)
          {
            c->reset_for_next();
            post_read(c);
          }
          else
          {
            close_conn(c);
          }
        }
      }
    }
  }

  void run()
  {
    thread acc([this] { accept_thread(); });
    unsigned n = max(2u, thread::hardware_concurrency());
    vector<thread> workers;
    for (unsigned i = 0; i < n; ++i)
      workers.emplace_back([this] { worker_thread(); });
    std::cout << "IOCP HTTP (" << n << " workers) on 0.0.0.0:" << PORT << "\n";
    for (auto& t : workers)
      t.join();
    acc.join();
  }
};
#endif // _WIN32

// -------------------------------
// main(): pick backend
// -------------------------------
int main()
{
#if defined(_WIN32)
  IocpServer s;
  if (!s.init(PORT))
    return 1;
  s.run();
#elif defined(__APPLE__)
  KqueueServer s;
  if (!s.init(PORT, std::max(1u, std::thread::hardware_concurrency())))
    return 1;
  s.run();
#elif defined(__linux__)
  EpollServer s;
  if (!s.init(PORT, std::max(1u, std::thread::hardware_concurrency())))
    return 1;
  s.run();
#else
  #error "Unsupported platform"
#endif
  return 0;
}
