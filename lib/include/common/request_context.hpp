#ifndef REQUEST_CONTEXT_H
#define REQUEST_CONTEXT_H

#include "processing_state.hpp"
struct PerClientStorage;

// ===================== RequestContext (streaming parser) ==============
struct RequestContext
{
  PerClientStorage* owner{nullptr};
  size_t state{ProcessingState::not_started};
  std::string acc; // accumulates across segments
  size_t body_bytes_needed{0};
  bool keep_alive{true};

  void reset_parser();

  void on_segment(const char* p, size_t n);
};

#endif // REQUEST_CONTEXT_H