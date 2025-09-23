#ifndef PER_CLIENT_STORAGE_H
#define PER_CLIENT_STORAGE_H

#include "common/request_processing.hpp"
#include "socket.hpp"
#include "workflow.hpp"
#include <memory>
#include <queue>
#include <set>

class Worker;

class PerClientStorage
{
public:
  PerClientStorage() = default;

  void reset();

  void post(Workflow step);

  void handle(Workflow step);

private:
  bool on_send_completed();

  void parse_step();

  void find_handler();

  void generate_response();

  fd_t fd{-1};
  uint32_t served{0};
  bool closing{false};

  friend struct ConnTable;
  friend class Worker;
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
  std::set<std::shared_ptr<RequestProcessing>> requests_in_flight;
  std::set<std::shared_ptr<RequestProcessing>> requests_ready;
  bool send_inflight{false};
};

#endif // PER_CLIENT_STORAGE_H
