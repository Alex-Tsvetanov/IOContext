
#ifdef _WIN32
  #define WIN32_LEAN_AND_MEAN
  #include <windows.h>
  #include <winsock2.h>
  #include <ws2tcpip.h>
using ConnectionId = SOCKET;
#else
using ConnectionId = int;
#endif
#include <list>

class Buffers
{
public:
  Buffers() = default;
  Buffers(const Buffers&) = delete;
  Buffers& operator=(const Buffers&) = delete;
  Buffers(Buffers&&) = default;
  Buffers& operator=(Buffers&&) = default;
  ~Buffers() = default;

  void addIncomingBuffer(const char* buffer) { readBuffers.push_back(buffer); }

  void addOutgoingBuffer(const char* buffer) { writeBuffers.push_back(buffer); }

  std::list<const char*>& getReadBuffers() { return readBuffers; }

  std::list<const char*>& getWriteBuffers() { return writeBuffers; }

  void clearReadBuffers() { readBuffers.clear(); }

  void clearWriteBuffers() { writeBuffers.clear(); }

private:
  std::list<const char*> readBuffers;
  std::list<const char*> writeBuffers;
};

// -------------------------------
// Common configuration
// -------------------------------
static constexpr int PORT = 8080;
static constexpr int BACKLOG = 512;
static constexpr int READ_BUF_SIZE = 8192;
static constexpr int WRITE_BUF_SIZE = 4096;
static constexpr int MAX_EVENTS = 1024;
static constexpr int KEEPALIVE_TIMEOUTMS = 5000;
static constexpr int MAX_KEEPALIVE_REQS = 100;

int main()
{
  return 0;
}
