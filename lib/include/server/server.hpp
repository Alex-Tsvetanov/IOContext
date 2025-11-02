#ifndef SERVER_H
#define SERVER_H

#include "common/socket.hpp"
#include "common/coroutine_thread_pool.hpp"
#include "worker.hpp"
#include <thread>
#include <unordered_map>
#include <vector>
#include <memory>

struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  unsigned threads = std::max<unsigned>(1, std::thread::hardware_concurrency());
  uint16_t pending_accepts_per_worker = ACCEPTS_PER_WORKER;
  uint16_t max_keepalive_requests = 65000;
  bool reuseport = false; // Single io_uring, no need for SO_REUSEPORT
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
    , coro_pool_(std::max<unsigned>(1, cfg_.threads - 1)) // N-1 threads for coroutines
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

  ThreadPool& get_coro_pool() { return coro_pool_; }

  void run();

private:
  std::unordered_map<std::string, std::function<void(const Request&, Response&)>> routes_;
  ServerConfig cfg_;
  ThreadPool coro_pool_;           // Shared coroutine pool (N-1 threads)
  std::unique_ptr<Worker> worker_; // Single I/O worker
  std::thread io_thread_;          // Dedicated I/O thread
};

#endif // SERVER_H
