// Server scaffolding - platform independent interface
#include "server_config.hpp"
#include "connection_table.hpp"

class Server
{
public:
  explicit Server(const ServerConfig& cfg)
    : cfg_(cfg)
    , table_(create_connection_table(cfg))
  {}

  virtual ~Server() = default;

  virtual void run() = 0;

protected:
  ServerConfig cfg_;
  std::unique_ptr<ConnTable> table_;

  virtual std::unique_ptr<ConnTable> create_connection_table(const ServerConfig& cfg) = 0;
};