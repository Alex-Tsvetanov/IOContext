#ifndef PROCESSING_MACHINE_H
#define PROCESSING_MACHINE_H

#include "request.hpp"
#include "response.hpp"
#include "processing_state.hpp"

// ===================== ProcessingMachine (streaming parser) ==============
struct ProcessingMachine
{
  Request req;
  Response res;
  ProcessingState::type state{ProcessingState::not_started}; // Parsing Request
  std::string tmp_buf[2]; // up to 2 buffers used for partial parsing - [0] = method, path, protocol version, header
                          // name, body; [1] = header value
  size_t body_bytes_needed{0}; // on-line value of Content-legth header (stored while reading the segment char-by-char)
  bool keep_alive{true};       // on-line value of Connection header (stored while reading the segment char-by-char)

  void reset_parser();

  void on_segment(const char* p, size_t n);
};

#endif // PROCESSING_MACHINE_H
