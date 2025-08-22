// Server configuration implementation - platform independent
#include "server_config.hpp"
#include <thread>

ServerConfig::ServerConfig()
{
  threads = (uint16_t) std::max<unsigned>(1u, std::thread::hardware_concurrency());
}