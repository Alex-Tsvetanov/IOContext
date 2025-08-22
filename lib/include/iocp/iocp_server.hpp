// Windows IOCP-specific server implementation
#include "server.hpp"
#include "iocp_connection_table.hpp"
#include <thread>
#include <vector>

#ifdef _WIN32

class IOCPServer : public Server
{
public:
  explicit IOCPServer(const ServerConfig& cfg);
  ~IOCPServer() override;

  void run() override;

private:
  std::unique_ptr<ConnTable> create_connection_table(const ServerConfig& cfg) override;

  SOCKET listen_{INVALID_SOCKET};
  HANDLE iocp_{nullptr};
  std::vector<std::thread> threads_;
};

#endif