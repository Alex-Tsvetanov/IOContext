#ifndef SERVER_H
#define SERVER_H

#include "common/socket.hpp"
#include "worker.hpp"
#include <thread>
#include <unordered_map>
#include <vector>

struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  unsigned threads = std::max<unsigned>(1, std::thread::hardware_concurrency());
  uint16_t pending_accepts_per_worker = ACCEPTS_PER_WORKER;
  uint16_t max_keepalive_requests = 65000;
  bool reuseport = true;
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
  {}

  Server& add_route(const std::string& path, const std::function<void(const Request&, Response&)>& handler)
  {
    routes_[path] = handler;
    return *this;
  }

  const std::unordered_map<std::string, std::function<void(const Request&, Response&)>>& get_routes() const
  {
    return routes_;
  }

  void run()
  {
    const uint16_t N = std::max<uint16_t>(1, cfg_.threads);

    for (uint16_t i = 0; i < N; ++i)
    {
      listeners_.push_back(make_listener(cfg_.port, cfg_.reuseport));
    }

    WorkerConfig wcfg{};
    wcfg.accepts_per_worker = cfg_.pending_accepts_per_worker;
    wcfg.max_keepalive_requests = cfg_.max_keepalive_requests;

    workers_.reserve(N);
    threads_.reserve(N);
    for (uint16_t i = 0; i < N; ++i)
    {
      workers_.emplace_back(this, listeners_[i], wcfg);
      threads_.emplace_back(std::ref(workers_.back()));
    }
    for (auto& t : threads_)
    {
      t.join();
    }
  }

private:
  std::unordered_map<std::string, std::function<void(const Request&, Response&)>> routes_;
  ServerConfig cfg_;
  std::vector<fd_t> listeners_;
  std::vector<Worker> workers_;
  std::vector<std::thread> threads_;
};

#endif // SERVER_H