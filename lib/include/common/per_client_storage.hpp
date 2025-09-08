#ifndef PER_CLIENT_STORAGE_H
#define PER_CLIENT_STORAGE_H

#include "common/request_processing.hpp"
#include "ds/queue.hpp"
#include "ds/set.hpp"
#include "socket.hpp"
#include "workflow.hpp"

class Worker;

class PerClientStorage
{
public:
  PerClientStorage() = default;

  void reset();

  void post(Workflow step);

  bool on_send_completed();

  void parse_step();

  void find_handler();

  void generate_response();

private:
  fd_t fd{-1};
  uint32_t served{0};
  bool closing{false};

  friend struct ConnTable;
  friend class Worker;
  Worker* owner{nullptr};

  // policy
  uint16_t keepalive_limit{65000};

  // state
  bool ms_recv_armed{false};
  bool pollout_armed{false};

  // Current request processing
  TSSet<RequestProcessing>::element current_request;

  // Processed requests that need to find their handlers
  TSSet<RequestProcessing> requests_needing_handlers;

  // Processed requests that need to call their handlers and generate responses
  TSSet<RequestProcessing> requests_needing_responses;

  // RX
  TSSet<void> upcoming_recv_buffs;
  TSQueue<std::vector<char>> completed_recv_buffs;

  // TX
  TSSet<RequestProcessing> requests_in_flight;
  TSSet<RequestProcessing> requests_ready;
  bool send_inflight{false};
};

#endif // PER_CLIENT_STORAGE_H
