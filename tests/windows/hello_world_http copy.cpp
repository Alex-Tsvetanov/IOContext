#define UNICODE
#include <WinSock2.h>
#include <MSWSock.h>
#include <Windows.h>
#include <atomic>
#include <cstring>
#include <vector>
#include <thread>
#include <memory>
#include <system_error>
#include <iostream>

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

constexpr USHORT SERVER_PORT = 8080;
constexpr int BUFFER_SIZE = 16 * 1024; // 16KB buffer per connection
constexpr int MAX_THREADS = 32;        // Tune based on CPU cores
// constexpr int BACKLOG_SIZE = 1024;     // Async accept backlog

// Precomputed HTTP response
constexpr const char HTTP_RESPONSE[] = "HTTP/1.1 200 OK\r\n"
                                       "Content-Type: text/plain\r\n"
                                       "Content-Length: 13\r\n"
                                       "Connection: keep-alive\r\n"
                                       "\r\n"
                                       "Hello, World!";
constexpr DWORD HTTP_RESPONSE_SIZE = sizeof(HTTP_RESPONSE) - 1;

// IOCP operation types
enum class IoOperation
{
  ACCEPT,
  RECV,
  SEND
};

// Per-I/O operation context
struct OverlappedEx
{
  OVERLAPPED overlapped;
  IoOperation operation;
  SOCKET socket;
  WSABUF wsaBuf;
  char buffer[BUFFER_SIZE];
  DWORD bytesTransferred;
  DWORD flags;

  OverlappedEx() noexcept { std::memset(&overlapped, 0, sizeof(overlapped)); }
};

// Per-handle connection context
struct Connection
{
  SOCKET socket;
  std::unique_ptr<OverlappedEx> recvContext;
  std::unique_ptr<OverlappedEx> sendContext;

  explicit Connection(SOCKET s)
    : socket(s)
  {
    recvContext = std::make_unique<OverlappedEx>();
    recvContext->socket = s;
    recvContext->operation = IoOperation::RECV;
    recvContext->wsaBuf = {BUFFER_SIZE, recvContext->buffer};

    sendContext = std::make_unique<OverlappedEx>();
    sendContext->socket = s;
    sendContext->operation = IoOperation::SEND;
    sendContext->wsaBuf = {HTTP_RESPONSE_SIZE, const_cast<char*>(HTTP_RESPONSE)};
  }

  ~Connection()
  {
    if (socket != INVALID_SOCKET)
    {
      closesocket(socket);
    }
  }
};

// Server state manager
class Server
{
  HANDLE iocp;
  SOCKET listenSocket;
  std::atomic<bool> running{true};
  std::vector<std::thread> threads;
  std::unique_ptr<OverlappedEx> acceptContext;
  LPFN_ACCEPTEX AcceptEx = nullptr;
  LPFN_GETACCEPTEXSOCKADDRS GetAcceptExSockaddrs = nullptr;

  void InitWinsock()
  {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "WSAStartup failed");
    }
  }

  void CreateIOCP()
  {
    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (!iocp)
    {
      throw std::system_error(GetLastError(), std::system_category(), "CreateIoCompletionPort failed");
    }
  }

  void CreateListenSocket()
  {
    listenSocket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (listenSocket == INVALID_SOCKET)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "WSASocket failed");
    }

    // Enable address reuse
    int reuse = 1;
    if (setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse)) ==
        SOCKET_ERROR)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "setsockopt failed");
    }

    // Bind to port
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(SERVER_PORT);

    if (bind(listenSocket, reinterpret_cast<SOCKADDR*>(&serverAddr), sizeof(serverAddr)) == SOCKET_ERROR)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "bind failed");
    }

    // Start listening
    if (listen(listenSocket, SOMAXCONN) == SOCKET_ERROR)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "listen failed");
    }

    // Associate listener with IOCP
    if (!CreateIoCompletionPort(reinterpret_cast<HANDLE>(listenSocket), iocp, 0, 0))
    {
      throw std::system_error(GetLastError(), std::system_category(), "CreateIoCompletionPort failed");
    }
  }

  void LoadExtensionFunctions()
  {
    GUID acceptexGuid = WSAID_ACCEPTEX;
    DWORD bytes;
    if (WSAIoctl(listenSocket, SIO_GET_EXTENSION_FUNCTION_POINTER, &acceptexGuid, sizeof(acceptexGuid), &AcceptEx,
                 sizeof(AcceptEx), &bytes, NULL, NULL) == SOCKET_ERROR)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "WSAIoctl for AcceptEx failed");
    }

    GUID getacceptexsockaddrsGuid = WSAID_GETACCEPTEXSOCKADDRS;
    if (WSAIoctl(listenSocket, SIO_GET_EXTENSION_FUNCTION_POINTER, &getacceptexsockaddrsGuid,
                 sizeof(getacceptexsockaddrsGuid), &GetAcceptExSockaddrs, sizeof(GetAcceptExSockaddrs), &bytes, NULL,
                 NULL) == SOCKET_ERROR)
    {
      throw std::system_error(WSAGetLastError(), std::system_category(), "WSAIoctl for GetAcceptExSockaddrs failed");
    }
  }

  void StartAccept()
  {
    acceptContext = std::make_unique<OverlappedEx>();
    acceptContext->operation = IoOperation::ACCEPT;
    acceptContext->socket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

    if (!AcceptEx(listenSocket, acceptContext->socket, acceptContext->buffer, 0, sizeof(sockaddr_in) + 16,
                  sizeof(sockaddr_in) + 16, &acceptContext->bytesTransferred, &acceptContext->overlapped))
    {
      if (WSAGetLastError() != ERROR_IO_PENDING)
      {
        throw std::system_error(WSAGetLastError(), std::system_category(), "AcceptEx failed");
      }
    }
  }

  void WorkerThread()
  {
    while (running)
    {
      DWORD bytesTransferred = 0;
      ULONG_PTR completionKey = 0;
      OverlappedEx* overlappedEx = nullptr;

      BOOL status = GetQueuedCompletionStatus(iocp, &bytesTransferred, &completionKey,
                                              reinterpret_cast<LPOVERLAPPED*>(&overlappedEx), INFINITE);
      std::cout << "Worker thread received completion: "
                << "Status: " << (status ? "Success" : "Failure") << ", Bytes: " << bytesTransferred
                << ", Key: " << completionKey << std::endl;
      if (!overlappedEx)
        continue; // Shutdown signal

      // Handle connection closure or error
      if (!status || bytesTransferred == 0)
      {
        delete reinterpret_cast<Connection*>(completionKey);
        continue;
      }

      switch (overlappedEx->operation)
      {
      case IoOperation::ACCEPT:
        HandleAccept(overlappedEx);
        StartAccept(); // Post next accept immediately
        break;

      case IoOperation::RECV:
        HandleRecv(overlappedEx);
        break;

      case IoOperation::SEND:
        // Prepare for next request
        HandleSend(overlappedEx);
        break;
      }
    }
  }

  void HandleAccept(OverlappedEx* acceptCtx)
  {
    // Associate accepted socket with IOCP
    auto conn = new Connection(acceptCtx->socket);
    CreateIoCompletionPort(reinterpret_cast<HANDLE>(conn->socket), iocp, reinterpret_cast<ULONG_PTR>(conn), 0);

    // Set socket options
    setsockopt(conn->socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, reinterpret_cast<char*>(&listenSocket),
               sizeof(listenSocket));

    // Start receiving data
    DWORD flags = 0;
    if (WSARecv(conn->socket, &conn->recvContext->wsaBuf, 1, nullptr, &flags, &conn->recvContext->overlapped,
                nullptr) == SOCKET_ERROR)
    {
      if (WSAGetLastError() != WSA_IO_PENDING)
      {
        delete conn;
      }
    }
  }

  void HandleRecv(OverlappedEx* recvCtx)
  {
    auto conn = reinterpret_cast<Connection*>(recvCtx->socket);

    // Optimized request detection (look for end of headers)
    bool requestComplete = false;
    if (recvCtx->bytesTransferred >= 4)
    {
      for (DWORD i = 0; i <= recvCtx->bytesTransferred - 4; ++i)
      {
        if (std::memcmp(recvCtx->buffer + i, "\r\n\r\n", 4) == 0)
        {
          requestComplete = true;
          break;
        }
      }
    }

    if (requestComplete)
    {
      // Send response immediately
      if (WSASend(conn->socket, &conn->sendContext->wsaBuf, 1, nullptr, 0, &conn->sendContext->overlapped, nullptr) ==
          SOCKET_ERROR)
      {
        if (WSAGetLastError() != WSA_IO_PENDING)
        {
          delete conn;
        }
      }
    }
    else
    {
      // Continue receiving if request incomplete
      DWORD flags = 0;
      if (WSARecv(conn->socket, &conn->recvContext->wsaBuf, 1, nullptr, &flags, &conn->recvContext->overlapped,
                  nullptr) == SOCKET_ERROR)
      {
        if (WSAGetLastError() != WSA_IO_PENDING)
        {
          delete conn;
        }
      }
    }
  }

  void HandleSend(OverlappedEx* sendCtx)
  {
    auto conn = reinterpret_cast<Connection*>(sendCtx->socket);

    // Reset buffer for next request
    conn->recvContext->wsaBuf.len = BUFFER_SIZE;
    DWORD flags = 0;
    if (WSARecv(conn->socket, &conn->recvContext->wsaBuf, 1, nullptr, &flags, &conn->recvContext->overlapped,
                nullptr) == SOCKET_ERROR)
    {
      if (WSAGetLastError() != WSA_IO_PENDING)
      {
        delete conn;
      }
    }
  }

public:
  Server()
  {
    InitWinsock();
    CreateIOCP();
    CreateListenSocket();
    LoadExtensionFunctions();
    StartAccept();

    // Create worker threads (1 per logical processor)
    SYSTEM_INFO sysInfo;
    GetSystemInfo(&sysInfo);
    int threadCount = sysInfo.dwNumberOfProcessors > MAX_THREADS ? MAX_THREADS : sysInfo.dwNumberOfProcessors;

    for (int i = 0; i < threadCount; ++i)
    {
      threads.emplace_back([this] { WorkerThread(); });
    }
  }

  ~Server()
  {
    running = false;

    // Signal threads to exit
    for (size_t i = 0; i < threads.size(); ++i)
    {
      PostQueuedCompletionStatus(iocp, 0, 0, nullptr);
    }

    for (auto& thread : threads)
    {
      if (thread.joinable())
        thread.join();
    }

    closesocket(listenSocket);
    CloseHandle(iocp);
    WSACleanup();
  }
};

int main()
{
  try
  {
    Server server;
    printf("Server running on port %d. Press Ctrl+C to exit...\n", SERVER_PORT);
    while (true)
      Sleep(10000);
  }
  catch (const std::system_error& e)
  {
    printf("Error: %s (%d)\n", e.what(), e.code().value());
    return 1;
  }
  return 0;
}