#ifndef REQUEST_PROCESSING_H
#define REQUEST_PROCESSING_H

#include "common/processing_machine.hpp"
#include <functional>
#include <sys/socket.h>
#ifdef DEBUG
#include "common/debug_log.hpp"
#endif

struct RequestProcessing
{
  ProcessingMachine parser;
  std::function<void(const Request&, Response&)> request_handler;
  bool closing{false};
  bool keep_alive{true};
  size_t body_need{0};

  struct BatchedSendData
  {
    std::vector<iovec> iov;
    msghdr msg{};
  } batched_send_data;

  RequestProcessing() = default;
  ~RequestProcessing() = default;
};

#endif // REQUEST_PROCESSING_H