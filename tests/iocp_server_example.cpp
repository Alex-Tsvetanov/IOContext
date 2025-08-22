// Example of using the refactored IOCP server from the IOContext library
#include "iocontext.hpp"
#include <iostream>

int main()
{
#ifdef _WIN32
  try {
    ServerConfig cfg{};
    cfg.port = 8080;
    cfg.threads = 4;
    cfg.pending_accepts_per_worker = 128;
    cfg.max_keepalive_requests = 65000;
    cfg.table_capacity = 1u << 15;
    cfg.idle_seconds = 300;

    IOCPServer server(cfg);
    std::cout << "Starting IOCP HTTP server on port " << cfg.port << std::endl;
    std::cout << "Using " << cfg.threads << " worker threads" << std::endl;
    server.run();
  }
  catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
#else
  std::cerr << "This example requires Windows IOCP" << std::endl;
  return 1;
#endif

  return 0;
}