// Server configuration - platform independent
#include <chrono>

using namespace std::chrono;

struct ServerConfig
{
  uint16_t port = 8080;
  uint16_t threads = 1; // Will be set based on hardware_concurrency()
  uint16_t pending_accepts_per_worker = 128;
  uint16_t max_keepalive_requests = 65000; // keep connections alive for the whole run
  uint32_t table_capacity = 1u << 15;      // 32768 slots
  uint16_t idle_seconds = 300;             // generous idle window

  ServerConfig();
};