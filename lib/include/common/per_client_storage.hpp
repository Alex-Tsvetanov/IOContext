#ifndef PER_CLIENT_STORAGE_H
#define PER_CLIENT_STORAGE_H

#include "common/owned_buf.hpp"
#include "socket.hpp"
#include "workflow.hpp"
#include "processing_machine.hpp"
#include <atomic>
#include <deque>
#include <functional>

class Worker;

class PerClientStorage
{
public:
  PerClientStorage() = default;

  void post(Workflow step);

  bool on_send_completed();

  void parse_step();

  void find_handler();

  void generate_response();

private:
  fd_t fd{-1};
  uint32_t served{0};
  bool closing{false};
  bool keep_alive{true};
  size_t body_need{0};

  ProcessingMachine parser;
  std::function<void(const Request&, Response&)> request_handler;

  friend struct ConnTable;
  friend class Worker;
  Worker* owner{nullptr};

  BatchedSendData batched_send_data;

  // policy
  uint16_t keepalive_limit{65000};

  // state
  bool ms_recv_armed{false};
  bool pollout_armed{false};

  // edge-triggered send kick
  std::atomic_flag sendkick_pending = ATOMIC_FLAG_INIT;

  // RX
  std::deque<OwnedBuf> rxq;                   // worker-thread only
  std::shared_ptr<std::vector<char>> rx_hold; // singleshot fallback

  // TX
  std::deque<OwnedBuf> txq;
  std::deque<OwnedBuf> tx_inflight;
  bool send_inflight{false};
  bool inflight_eor{false};
};

#endif // PER_CLIENT_STORAGE_H