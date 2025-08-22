// Windows IOCP-specific connection table implementation
#include "connection_table.hpp"

#ifdef _WIN32

class IOCPConnTable : public ConnTable
{
public:
  explicit IOCPConnTable(size_t cap) : ConnTable(cap) {}

  ConnHandle allocate() override;

  void close_and_recycle(ConnHandle h) override;
};

#endif