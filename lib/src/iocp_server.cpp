// Windows IOCP server implementation
#include "iocp/iocp_server.hpp"
#include "iocp/iocp_worker.hpp"
#include "iocp/winsock_wrapper.hpp"
#include <vector>

#ifdef _WIN32

IOCPServer::IOCPServer(const ServerConfig& cfg)
  : Server(cfg)
  , listen_(INVALID_SOCKET)
  , iocp_(nullptr)
{}

IOCPServer::~IOCPServer()
{
  if (listen_ != INVALID_SOCKET)
    ::closesocket(listen_);
  if (iocp_)
    CloseHandle(iocp_);
}

std::unique_ptr<ConnTable> IOCPServer::create_connection_table(const ServerConfig& cfg)
{
  return std::make_unique<IOCPConnTable>(cfg.table_capacity);
}

void IOCPServer::run()
{
  listen_ = WinSockWrapper::make_listen_socket(cfg_.port);
  WinSockWrapper::load_extensions(listen_);

  iocp_ = CreateIoCompletionPort((HANDLE) listen_, nullptr, 0, 0);
  if (!iocp_)
    WinSockWrapper::die("CreateIoCompletionPort(listener)");

  if (cfg_.threads == 0)
    cfg_.threads = 1;

  std::vector<IOCPWorker> workers;
  workers.reserve(cfg_.threads);
  threads_.reserve(cfg_.threads);

  for (uint16_t i = 0; i < cfg_.threads; ++i)
  {
    workers.emplace_back(iocp_, listen_, *table_, cfg_.pending_accepts_per_worker, cfg_.max_keepalive_requests,
                        seconds(cfg_.idle_seconds));
    threads_.emplace_back(std::ref(workers.back()));
  }

  for (auto& t : threads_)
    t.join();
}

#endif