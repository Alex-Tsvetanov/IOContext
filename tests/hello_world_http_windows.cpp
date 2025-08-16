#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <memory>
#include <atomic>

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

using namespace std::chrono;

constexpr int PORT = 8080;
constexpr int READ_BUF_SIZE = 4096;
constexpr int WRITE_BUF_SIZE = 4096;
constexpr int MAX_KEEPALIVE_REQUESTS = 100;
constexpr int KEEPALIVE_TIMEOUT_MS = 5000;
constexpr int MAX_PENDING_ACCEPTS = 128;

enum class ConnState
{
  ReadingHeaders,
  Writing,
  Closing
};

enum class IoOperation
{
  Accept,
  Read,
  Write
};

struct Connection;

struct PerIoContext
{
  OVERLAPPED overlapped{};
  WSABUF wsaBuf{};
  IoOperation op;
  Connection* conn;
  char buffer[READ_BUF_SIZE];
};

struct Connection
{
  SOCKET fd = INVALID_SOCKET;
  ConnState state = ConnState::ReadingHeaders;

  alignas(64) char read_buf[READ_BUF_SIZE];
  size_t read_len = 0;

  alignas(64) char write_buf[WRITE_BUF_SIZE];
  size_t write_len = 0;
  size_t write_offset = 0;

  int requests_served = 0;
  steady_clock::time_point last_active{steady_clock::now()};

  PerIoContext readCtx{};
  PerIoContext writeCtx{};

  void init(SOCKET s)
  {
    fd = s;
    state = ConnState::ReadingHeaders;
    read_len = write_len = write_offset = 0;
    last_active = steady_clock::now();

    // Prepare IO contexts
    ZeroMemory(&readCtx.overlapped, sizeof(OVERLAPPED));
    ZeroMemory(&writeCtx.overlapped, sizeof(OVERLAPPED));
    readCtx.conn = this;
    writeCtx.conn = this;
    readCtx.op = IoOperation::Read;
    writeCtx.op = IoOperation::Write;
  }

  void prepare_response()
  {
    const char body[] = "Hello, World!\n";

    std::string header = "HTTP/1.1 200 OK\r\n"
                         "Content-Type: text/plain\r\n"
                         "Content-Length: 14\r\n";

    bool keep_alive = (requests_served < MAX_KEEPALIVE_REQUESTS);
    if (keep_alive)
      header += "Connection: keep-alive\r\n\r\n";
    else
      header += "Connection: close\r\n\r\n";

    size_t header_len = header.size();
    memcpy(write_buf, header.data(), header_len);
    memcpy(write_buf + header_len, body, sizeof(body) - 1);

    write_len = header_len + sizeof(body) - 1;
    write_offset = 0;
    state = ConnState::Writing;
  }

  void reset_for_next_request()
  {
    read_len = 0;
    write_len = 0;
    write_offset = 0;
    state = ConnState::ReadingHeaders;
    last_active = steady_clock::now();
    ++requests_served;
  }

  void close_conn()
  {
    if (fd != INVALID_SOCKET)
    {
      closesocket(fd);
      fd = INVALID_SOCKET;
    }
  }
};

// Simple substring search for \r\n\r\n
bool has_double_crlf(const char* buf, size_t len)
{
  for (size_t i = 0; i + 3 < len; ++i)
  {
    if (buf[i] == '\r' && buf[i + 1] == '\n' && buf[i + 2] == '\r' && buf[i + 3] == '\n')
      return true;
  }
  return false;
}

class IocpServer
{
public:
  IocpServer()
  {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
  }

  ~IocpServer() { WSACleanup(); }

  bool start()
  {
    listen_fd = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (listen_fd == INVALID_SOCKET)
    {
      std::cerr << "Failed to create listen socket\n";
      return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, (char*) &opt, sizeof(opt));

    if (bind(listen_fd, (sockaddr*) &addr, sizeof(addr)) == SOCKET_ERROR)
    {
      std::cerr << "Bind failed\n";
      return false;
    }
    if (listen(listen_fd, SOMAXCONN) == SOCKET_ERROR)
    {
      std::cerr << "Listen failed\n";
      return false;
    }

    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);

    // Bind listener to IOCP
    CreateIoCompletionPort((HANDLE) listen_fd, iocp, (ULONG_PTR) listen_fd, 0);

    // Spawn workers
    int threads = std::thread::hardware_concurrency();
    if (threads <= 0)
      threads = 4;
    for (int i = 0; i < threads; ++i)
      workers.emplace_back(&IocpServer::worker_loop, this);

    // Start accepting connections asynchronously
    accept_loop();

    for (auto& t : workers)
      t.join();
    return true;
  }

private:
  SOCKET listen_fd = INVALID_SOCKET;
  HANDLE iocp = NULL;
  std::vector<std::thread> workers;
  std::vector<std::unique_ptr<Connection>> conns;

  void accept_loop()
  {
    while (true)
    {
      SOCKET client = WSAAccept(listen_fd, NULL, NULL, NULL, 0);
      if (client == INVALID_SOCKET)
      {
        int err = WSAGetLastError();
        if (err == WSAEWOULDBLOCK || err == WSAEINTR)
          continue;
        else
        {
          std::cerr << "Accept error: " << err << "\n";
          continue;
        }
      }

      u_long mode = 1;
      ioctlsocket(client, FIONBIO, &mode);

      auto conn = std::make_unique<Connection>();
      conn->init(client);

      CreateIoCompletionPort((HANDLE) client, iocp, (ULONG_PTR) conn.get(), 0);

      // Post first read
      start_read(conn.get());
      conns.push_back(std::move(conn));
    }
  }

  void start_read(Connection* conn)
  {
    DWORD flags = 0;
    conn->readCtx.wsaBuf.buf = conn->read_buf + conn->read_len;
    conn->readCtx.wsaBuf.len = READ_BUF_SIZE - (ULONG) conn->read_len;
    int rc = WSARecv(conn->fd, &conn->readCtx.wsaBuf, 1, NULL, &flags, &conn->readCtx.overlapped, NULL);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      conn->close_conn();
    }
  }

  void start_write(Connection* conn)
  {
    conn->writeCtx.wsaBuf.buf = conn->write_buf + conn->write_offset;
    conn->writeCtx.wsaBuf.len = (ULONG) (conn->write_len - conn->write_offset);
    int rc = WSASend(conn->fd, &conn->writeCtx.wsaBuf, 1, NULL, 0, &conn->writeCtx.overlapped, NULL);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
    {
      conn->close_conn();
    }
  }

  void worker_loop()
  {
    DWORD bytesTransferred;
    ULONG_PTR key;
    LPOVERLAPPED overlapped;

    while (true)
    {
      BOOL ok = GetQueuedCompletionStatus(iocp, &bytesTransferred, &key, &overlapped, 1000);
      auto conn = reinterpret_cast<Connection*>(key);
      auto ctx = reinterpret_cast<PerIoContext*>(overlapped);

      if (!ok || !ctx || !conn)
      {
        continue;
      }

      if (ctx->op == IoOperation::Read)
      {
        if (bytesTransferred == 0)
        {
          conn->close_conn();
          continue;
        }
        conn->read_len += bytesTransferred;
        if (has_double_crlf(conn->read_buf, conn->read_len))
        {
          conn->prepare_response();
          start_write(conn);
        }
        else
        {
          start_read(conn);
        }
      }
      else if (ctx->op == IoOperation::Write)
      {
        conn->write_offset += bytesTransferred;
        if (conn->write_offset < conn->write_len)
        {
          start_write(conn);
        }
        else
        {
          if (conn->requests_served + 1 < MAX_KEEPALIVE_REQUESTS)
          {
            conn->reset_for_next_request();
            start_read(conn);
          }
          else
          {
            conn->close_conn();
          }
        }
      }
    }
  }
};

int main()
{
  IocpServer server;
  if (!server.start())
  {
    std::cerr << "Server failed to start\n";
    return 1;
  }
  return 0;
}
