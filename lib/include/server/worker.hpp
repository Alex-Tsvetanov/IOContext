#ifndef WORKER_H
#define WORKER_H

#include "../common/socket.hpp"
#include "common/connection_table.hpp"
#include "common/per_client_storage.hpp"
#include <cstdint>

class Server;

struct WorkerConfig
{
  uint16_t accepts_per_worker{ACCEPTS_PER_WORKER};
  uint16_t max_keepalive_requests{65000};
};

struct EventData
{
  fd_t fd;
  Workflow op;
  std::shared_ptr<void> hold;
};

class Worker
{
public:
  Worker(Worker&&) = default;
#ifdef WIN32
  Worker(Server* owner_, fd_t listen_fd, const WorkerConfig& cfg, HANDLE iocp);
#else
  Worker(Server* owner_, fd_t listen_fd, const WorkerConfig& cfg);
#endif
  ~Worker();

  inline void operator()() { run(); }

  void run();

  Server* server() const { return owner; }

  void post_accept();
  void post_recv(PerClientStorage* c);
  void post_send(PerClientStorage* c);
  void post_close(fd_t fd);

  void post_internal_event(PerClientStorage*, Workflow);

  void handle_accept(io_uring_cqe*, EventData*);
  void handle_recv(io_uring_cqe*, EventData*);
  void handle_send(io_uring_cqe*, EventData*);
  void handle_close(io_uring_cqe*, EventData*);

  void handle_internal_event(EventData*);

  fd_t listen_fd_;
  WorkerConfig cfg_;
  ConnTable table_;
  Server* owner{nullptr};
#ifdef WIN32
  HANDLE iocp_;
#else
  io_uring ring_{};
  int ring_fd_{-1};
  uint16_t need_submit_{0};

// --- multishot accept state ---
#ifdef IORING_ACCEPT_MULTISHOT
  bool use_ms_accept_{true};
#else
  bool use_ms_accept_{false};
#endif
  bool ms_accept_armed_{false};

  // --- multishot recv + provided buffers ---
  static constexpr int BUF_SZ = (1 << 10);

private:
  inline io_uring_sqe* new_event_for_posting();
#endif
};

#endif // WORKER_H