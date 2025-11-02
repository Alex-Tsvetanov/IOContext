#include "common/per_client_storage.hpp"
#include <cstddef>
#include <memory>
#ifdef DEBUG
#include <iostream>
#include "common/debug_log.hpp"
#endif

void PerClientStorage::reset()
{
  current_request.reset();
  requests_needing_handlers.clear();
  requests_needing_responses.clear();
  requests_ready.clear();
  while (!completed_recv_buffs.empty())
  {
    completed_recv_buffs.pop();
  }
  coro_handle.reset();
}
