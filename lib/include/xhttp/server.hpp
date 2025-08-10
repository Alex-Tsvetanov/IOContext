#pragma once
#include <functional>
#include <memory>
#include <thread>
#include <vector>
#include <atomic>
#include <unordered_map>
#include "xhttp/error.hpp"
#include "xhttp/net.hpp"
#include "xhttp/poller.hpp"
#include "xhttp/timer_heap.hpp"
#include "xhttp/connection.hpp"

namespace xhttp
{

  struct ServerConfig
  {
    Endpoint endpoint{"0.0.0.0", 8080};
    int backlog{512};
    int threads{int(std::thread::hardware_concurrency() ? std::thread::hardware_concurrency() : 1)};
    int keepalive_timeout_ms{5000};
    int max_keepalive_requests{100};
    bool reuse_port{true};
    bool tcp_nodelay{true};
  };

  struct RequestView
  {
    std::string_view method;
    std::string_view path;
    std::string_view raw;
  };

  using ResponseBuilder = std::function<void(Connection& c, const RequestView&)>;

  class HttpServer
  {
  public:
    explicit HttpServer(ServerConfig cfg, ResponseBuilder builder);
    ~HttpServer();
    expected<void> start();
    void stop();

  private:
    struct Reactor
    {
      std::unique_ptr<IPoller> poller;
      TimerHeap timers;
      std::unordered_map<xhttp_socket_t, Connection> conns;
      xhttp_socket_t listen_fd{XHTTP_INVALID_SOCKET};
      std::thread thr;
      std::atomic<bool> running{false};
    };

    ServerConfig cfg_;
    ResponseBuilder builder_;
    std::vector<std::unique_ptr<Reactor>> reactors_;
    std::atomic<bool> stopping_{false};

    expected<xhttp_socket_t> make_listener(bool reuse_port);
    void run_reactor(Reactor& r);
    void handle_accept(Reactor& r);
    void handle_read(Reactor& r, Connection& c, const PollEvent* ev);
    void handle_write(Reactor& r, Connection& c);
    void close_conn(Reactor& r, Connection& c);
    void schedule_idle_timeout(Reactor& r, Connection& c);
    static bool parse_request(Connection& c, RequestView& out);
  };

} // namespace xhttp
