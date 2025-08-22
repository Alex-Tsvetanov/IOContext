// Windows IOCP-specific worker implementation
#include "connection_table.hpp"
#include "server_config.hpp"
#include <memory>

#ifdef _WIN32

// Forward declarations
struct AcceptCtx;
struct WinSockWrapper;

class IOCPWorker
{
public:
  IOCPWorker(HANDLE iocp, SOCKET listen, ConnTable& table, uint16_t accepts_per_worker, uint16_t keepalive_limit,
         seconds idle_timeout);
  ~IOCPWorker() = default;

  void operator()();

private:
  HANDLE iocp_;
  SOCKET listen_;
  ConnTable& table_;
  uint16_t accepts_per_worker_;
  uint16_t keepalive_limit_;
  seconds idle_timeout_;
  std::vector<std::unique_ptr<AcceptCtx>> accept_pool_;

  void post_accept(AcceptCtx* ac);
  void maybe_start_send_worker(PerClientStorage* c);
  void run();
};

#endif