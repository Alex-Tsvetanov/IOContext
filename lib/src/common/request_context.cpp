#include "common/request_context.hpp"

void RequestContext::reset_parser()
{
  state = ProcessingState::not_started;
  acc.clear();
  body_bytes_needed = 0;
  keep_alive = true;
}

void RequestContext::on_segment(const char* p, size_t n)
{}
