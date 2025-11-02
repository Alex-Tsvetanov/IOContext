#include "server/server.hpp"
#include "common/socket.hpp"

void Server::run()
{
  // Create a single listener socket (no SO_REUSEPORT needed)
  fd_t listen_fd = make_listener(cfg_.port, false);

  WorkerConfig wcfg{};
  wcfg.accepts_per_worker = cfg_.pending_accepts_per_worker;
  wcfg.max_keepalive_requests = cfg_.max_keepalive_requests;

  // Create the single I/O worker with reference to the shared coroutine pool
  worker_ = std::make_unique<Worker>(this, listen_fd, wcfg);

  // Run the I/O worker on the main thread (or dedicated thread)
  // This handles all io_uring operations
  worker_->run();
}
