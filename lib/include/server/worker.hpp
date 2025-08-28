#ifndef WORKER_H
#define WORKER_H

#include "../common/socket.hpp"
#include "common/connection_table.hpp"
#include <cstdint>

class Server;

struct WorkerConfig
{
  uint16_t accepts_per_worker{ACCEPTS_PER_WORKER};
  uint16_t max_keepalive_requests{65000};
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

  uint16_t post_initial_accepts();

  bool post_accept();
  void post_task(int fd, Workflow kind);
  bool start_recv(PerClientStorage* c);
  bool close_async(fd_t fd);
  bool update_pollout(PerClientStorage* c);
  void sendkick_inline(PerClientStorage* c);

  Server* server() const { return owner; }

private:
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

  // --- multishot recv + provided buffers ---
  static constexpr int BUF_SZ = RX_CAP;
  static constexpr int BUF_CNT = 2048; // per worker (~16MB)
  static constexpr int BUF_GRP = 7;

  std::unique_ptr<char[]> buf_pool_;
  bool have_buf_pool_{false};
  bool use_ms_recv_{true};

  inline io_uring_sqe* get_sqe_or_submit();
  void init_buffer_pool();
#endif
};

#endif // WORKER_H