#ifndef REQUEST_PROCESSING_H
#define REQUEST_PROCESSING_H

#include "common/processing_machine.hpp"
#include <functional>
#include <sys/socket.h>
#include "common/debug_log.hpp"

struct RequestProcessing
{
  ProcessingMachine parser;
  std::function<void(const Request&, Response&)> request_handler;
  bool closing{false};
  bool keep_alive{true};
  size_t body_need{0};

  struct BatchedSendData
  {
    std::vector<struct iovec> iov;
    struct msghdr msg{};
  } batched_send_data;

  RequestProcessing() = default;
  ~RequestProcessing()
  {
    ts_std::cout << "RequestProcessing destroyed for " << this << std::endl;
#ifdef DEBUG
#endif
  }
};

#endif // REQUEST_PROCESSING_H