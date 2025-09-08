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

  void operator()() { run(); }

  void run();

  // uint16_t post_initial_accepts();

  Server* server() const { return owner; }

  bool post_accept();
  bool post_recv(PerClientStorage* c);
  bool post_parse(PerClientStorage* c);
  bool post_find_handler(PerClientStorage* c);
  bool post_generate_response(PerClientStorage* c);
  bool post_request_flush(PerClientStorage* c);
  void post_send(PerClientStorage* c);
  bool post_close(fd_t fd);

  void handle_accept(io_uring_cqe*, EventData*);
  void handle_recv(io_uring_cqe*, EventData*);
  void handle_parse(EventData*);             // internal events do not need io_uring_cqe
  void handle_find_handler(EventData*);      // internal events do not need io_uring_cqe
  void handle_generate_response(EventData*); // internal events do not need io_uring_cqe
  void handle_request_flush(EventData*);     // internal events do not need io_uring_cqe
  void handle_send(io_uring_cqe*, EventData*);
  void handle_close(io_uring_cqe*, EventData*);

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
  bool use_ms_accept_{true};
  bool ms_accept_armed_{false};

public:
  // --- multishot recv + provided buffers ---
  static constexpr int BUF_SZ = (1 << 10);

private:
  std::unique_ptr<char[]> buf_pool_;
  bool have_buf_pool_{false};
  bool use_ms_recv_{true};

  inline io_uring_sqe* get_sqe_or_submit();
  // void init_buffer_pool();
#endif
};

#endif // WORKER_H