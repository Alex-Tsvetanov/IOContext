// Windows IOCP connection table implementation
#include "iocp/iocp_connection_table.hpp"
#include "iocp/iocp_client_storage.hpp"

#ifdef _WIN32

ConnHandle IOCPConnTable::allocate()
{
  for (;;)
  {
    for (uint32_t i = 0; i < slots_.size(); ++i)
    {
      bool expected = false;
      if (slots_[i].in_use.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
      {
        auto& slot = slots_[i];
        slot.client.clear();
        return ConnHandle{i, slot.generation.load(std::memory_order_relaxed)};
      }
    }
    ::Sleep(1);
  }
}

void IOCPConnTable::close_and_recycle(ConnHandle h)
{
  if (h.index >= slots_.size())
    return;
  auto& slot = slots_[h.index];
  if (slot.in_use.exchange(false, std::memory_order_acq_rel))
  {
    auto& c = slot.client;

    if (c.s != INVALID_SOCKET)
    {
      ::closesocket(c.s); // cancels pending I/O; completions still arrive
      c.s = INVALID_SOCKET;
    }

    // Leave txq intact if a send might still be in flight; it will be cleared at next init().
    {
      std::lock_guard lk(c.tx_mtx);
      c.kick_pending = false;
    }
    {
      std::lock_guard lr(c.rx_mtx);
      c.rxq.clear();
      c.parse_inflight = false;
    }
    {
      std::lock_guard lr(c.resp_mtx);
      c.respq.clear();
      c.respond_inflight = false;
    }

    slot.generation.fetch_add(1, std::memory_order_acq_rel);
  }
}

#endif