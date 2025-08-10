#include "xhttp/server.hpp"
#include <cstring>
#include <iostream>

namespace xhttp
{

  static constexpr const char* RESP_400 = "HTTP/1.1 400 Bad Request\r\n"
                                          "Connection: close\r\n"
                                          "Content-Length: 0\r\n\r\n";

  HttpServer::HttpServer(ServerConfig cfg, ResponseBuilder builder)
    : cfg_(std::move(cfg))
    , builder_(std::move(builder))
  {}

  HttpServer::~HttpServer()
  {
    stop();
  }

  expected<void> HttpServer::start()
  {
    if (!net_init())
      return std::unexpected(Error::SyscallFailure);

    reactors_.resize(std::max(1, cfg_.threads));
    for (auto& rp : reactors_)
    {
      rp = std::make_unique<Reactor>();
      rp->poller.reset(make_poller());
      if (!rp->poller || !rp->poller->init())
        return std::unexpected(Error::SyscallFailure);
      auto listen_res = make_listener(cfg_.reuse_port);
      if (!listen_res)
        return std::unexpected(listen_res.error());
      rp->listen_fd = *listen_res;
      rp->running = true;
      rp->thr = std::thread([this, r = rp.get()] { run_reactor(*r); });
    }
    return {};
  }

  void HttpServer::stop()
  {
    if (stopping_.exchange(true))
      return;
    for (auto& rp : reactors_)
    {
      if (!rp)
        continue;
      rp->running = false;
      if (rp->thr.joinable())
        rp->thr.join();
      if (rp->listen_fd != XHTTP_INVALID_SOCKET)
        closesock(rp->listen_fd);
      rp->conns.clear();
    }
    net_cleanup();
  }

  expected<xhttp_socket_t> HttpServer::make_listener(bool reuse_port)
  {
    xhttp_socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s == XHTTP_INVALID_SOCKET)
      return std::unexpected(Error::SyscallFailure);
    if (!set_reuseaddr(s))
    {
      closesock(s);
      return std::unexpected(Error::SyscallFailure);
    }
    if (reuse_port)
      set_reuseport(s);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.endpoint.port);
    if (cfg_.endpoint.address.empty() || cfg_.endpoint.address == "0.0.0.0")
      addr.sin_addr.s_addr = htonl(INADDR_ANY);
    else
      inet_pton(AF_INET, cfg_.endpoint.address.c_str(), &addr.sin_addr);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
    {
      closesock(s);
      return std::unexpected(Error::SyscallFailure);
    }
    if (::listen(s, cfg_.backlog) != 0)
    {
      closesock(s);
      return std::unexpected(Error::SyscallFailure);
    }
    set_nonblocking(s);
    return s;
  }

  void HttpServer::run_reactor(Reactor& r)
  {
    r.poller->add(r.listen_fd, XHTTP_EV_READ, nullptr);
    while (r.running)
    {
      using namespace std::chrono;
      auto now = Clock::now();
      int to_ms =
        (int) std::clamp(std::chrono::duration_cast<milliseconds>(r.timers.next_deadline() - now).count(), 0ll, 1000ll);
      int n = r.poller->wait(to_ms);
      std::cout << "Poll returned: " << n << " events\n";
      if (n < 0)
        continue;

      for (auto ev : r.poller->events())
      {
        std::cout << "Event: fd=" << ev.fd << ", events=" << ev.events << ", user=" << ev.user << std::endl;
        if (ev.fd == r.listen_fd || ev.user == nullptr)
        {
          if (ev.events & XHTTP_EV_READ)
            handle_accept(r);
          continue;
        }
        auto it = r.conns.find(ev.fd);
        if (it == r.conns.end())
          continue;
        Connection& c = it->second;
        if (ev.events & (XHTTP_EV_ERR | XHTTP_EV_RDHUP))
        {
          close_conn(r, c);
          continue;
        }
        if (ev.events & XHTTP_EV_READ)
          handle_read(r, c, &ev);
        if (ev.events & XHTTP_EV_WRITE)
          handle_write(r, c);
      }
      r.timers.tick();
    }
  }

  void HttpServer::handle_accept(Reactor& r)
  {
    for (;;)
    {
      sockaddr_in cli{};
      socklen_t len = sizeof(cli);
      xhttp_socket_t cfd = ::accept(r.listen_fd, reinterpret_cast<sockaddr*>(&cli), &len);
      if (cfd == XHTTP_INVALID_SOCKET)
      {
        int e = last_socket_error();
#if defined(_WIN32)
        if (e == WSAEWOULDBLOCK)
          break;
#else
        if (e == EAGAIN || e == EWOULDBLOCK)
          break;
#endif
        break;
      }
      std::cout << "Accepted connection: " << cfd << std::endl;
      set_nonblocking(cfd);
      if (cfg_.tcp_nodelay)
        set_nodelay(cfd, true);
      Connection c{};
      c.fd = cfd;
      c.keep_alive = true;
      c.state = ConnState::Reading;
      r.conns.emplace(cfd, std::move(c));
      r.poller->add(cfd, XHTTP_EV_READ | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &r.conns.at(cfd));
      schedule_idle_timeout(r, r.conns.at(cfd));
    }
  }

  bool HttpServer::parse_request(Connection& c, RequestView& out)
  {
    auto& s = c.header_acc;
    auto pos = s.find("\r\n\r\n");
    if (pos == std::string::npos)
      return false;
    auto first_line_end = s.find("\r\n");
    if (first_line_end == std::string::npos)
      return false;
    std::string_view line{s.data(), first_line_end};
    auto sp1 = line.find(' ');
    if (sp1 == std::string::npos)
      return false;
    auto sp2 = line.find(' ', sp1 + 1);
    if (sp2 == std::string::npos)
      return false;
    out.method = std::string_view{line.data(), sp1};
    out.path = std::string_view{line.data() + sp1 + 1, sp2 - (sp1 + 1)};
    out.raw = std::string_view{s.data(), pos + 4};
    return true;
  }

  void HttpServer::handle_read(Reactor& r, Connection& c, const PollEvent* ev)
  {
#if defined(_WIN32) && defined(XHTTP_PLATFORM_WINDOWS)
    if (ev && ev->data && ev->bytes > 0)
    { // IOCP fast-path (bytes and buffer provided)
      c.header_acc.append(ev->data, ev->bytes);
      RequestView req;
      if (parse_request(c, req))
      {
        builder_(c, req);
        c.state = ConnState::Writing;
        r.poller->mod(c.fd, XHTTP_EV_WRITE | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        return;
      }
      if (c.header_acc.size() > 64 * 1024)
      {
        c.out.append(RESP_400, std::strlen(RESP_400));
        c.keep_alive = false;
        c.state = ConnState::Writing;
        r.poller->mod(c.fd, XHTTP_EV_WRITE | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        return;
      }
      return; // wait for next completion
    }
#endif
    // Readiness drain loop (Linux/macOS)
    char buf[8192];
    for (;;)
    {
      int n = ::recv(c.fd, buf, sizeof(buf), 0);
      if (n == 0)
      {
        close_conn(r, c);
        return;
      }
      if (n < 0)
      {
        int e = last_socket_error();
#if defined(_WIN32)
        if (e == WSAEWOULDBLOCK)
          break;
#else
        if (e == EAGAIN || e == EWOULDBLOCK)
          break;
#endif
        close_conn(r, c);
        return;
      }
      c.header_acc.append(buf, n);
      RequestView req;
      if (parse_request(c, req))
      {
        builder_(c, req);
        c.state = ConnState::Writing;
        r.poller->mod(c.fd, XHTTP_EV_WRITE | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        break;
      }
      else if (c.header_acc.size() > 64 * 1024)
      {
        c.out.append(RESP_400, std::strlen(RESP_400));
        c.keep_alive = false;
        c.state = ConnState::Writing;
        r.poller->mod(c.fd, XHTTP_EV_WRITE | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        break;
      }
    }
  }

  void HttpServer::handle_write(Reactor& r, Connection& c)
  {
    auto data = c.out.unsent();
    if (data.size() == 0)
    {
      if (c.keep_alive && ++c.requests_made < cfg_.max_keepalive_requests)
      {
        c.header_acc.clear();
        c.state = ConnState::Reading;
        r.poller->mod(c.fd, XHTTP_EV_READ | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        schedule_idle_timeout(r, c);
      }
      else
      {
        close_conn(r, c);
      }
      return;
    }
    int n = ::send(c.fd, reinterpret_cast<const char*>(data.data()), (int) data.size(), 0);
    if (n < 0)
    {
      int e = last_socket_error();
#if defined(_WIN32)
      if (e == WSAEWOULDBLOCK)
        return;
#else
      if (e == EAGAIN || e == EWOULDBLOCK)
        return;
#endif
      close_conn(r, c);
      return;
    }
    c.out.consumed((std::size_t) n);
    if (c.out.empty())
    {
      if (c.keep_alive && ++c.requests_made < cfg_.max_keepalive_requests)
      {
        c.header_acc.clear();
        c.state = ConnState::Reading;
        r.poller->mod(c.fd, XHTTP_EV_READ | XHTTP_EV_RDHUP | XHTTP_EV_ERR, &c);
        schedule_idle_timeout(r, c);
      }
      else
      {
        close_conn(r, c);
      }
    }
  }

  void HttpServer::schedule_idle_timeout(Reactor& r, Connection& c)
  {
    if (c.idle_timer_id)
      r.timers.cancel(c.idle_timer_id);
    c.idle_timer_id = r.timers.add_after(std::chrono::milliseconds(cfg_.keepalive_timeout_ms), [&r, fd = c.fd, this] {
      auto it = r.conns.find(fd);
      if (it != r.conns.end())
        close_conn(r, it->second);
    });
  }

  void HttpServer::close_conn(Reactor& r, Connection& c)
  {
    r.poller->del(c.fd);
    closesock(c.fd);
    if (c.idle_timer_id)
      r.timers.cancel(c.idle_timer_id);
    r.conns.erase(c.fd);
  }

} // namespace xhttp
