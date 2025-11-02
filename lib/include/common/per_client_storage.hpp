#ifndef PER_CLIENT_STORAGE_H
#define PER_CLIENT_STORAGE_H

#include "common/request_processing.hpp"
#include "socket.hpp"
#include "workflow.hpp"
#include "coroutine_thread_pool.hpp"
#include <memory>
#include <queue>
#include <set>
#include <optional>

class Worker;

class PerClientStorage
{
public:
  PerClientStorage() = default;

  void reset();

  fd_t fd{-1};
  uint32_t served{0};
  bool closing{false};

  friend struct ConnTable;
  friend class Worker;
  friend Task client_workflow_coro(Worker*, PerClientStorage*);
  Worker* owner{nullptr};

  // policy
  uint16_t keepalive_limit{65000};

  // Current request processing
  std::shared_ptr<RequestProcessing> current_request;

  // Processed requests that need to find their handlers
  std::set<std::shared_ptr<RequestProcessing>> requests_needing_handlers;

  // Processed requests that need to call their handlers and generate responses
  std::set<std::shared_ptr<RequestProcessing>> requests_needing_responses;

  // RX
  std::queue<std::shared_ptr<std::vector<char>>> completed_recv_buffs;

  // TX
  std::set<std::shared_ptr<RequestProcessing>> requests_ready;
  bool send_inflight{false};

  // Coroutine workflow handle
  std::optional<std::coroutine_handle<Task::promise_type>> coro_handle;
};

#endif // PER_CLIENT_STORAGE_H
