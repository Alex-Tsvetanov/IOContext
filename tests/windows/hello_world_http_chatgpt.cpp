#define UNICODE
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <WinSock2.h>
#include <Windows.h>
#include <MSWSock.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <thread>
#include <iostream>

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

#define PORT 8080
#define MAX_ACCEPTS 128

#ifndef SIO_LOOPBACK_FAST_PATH
  #define SIO_LOOPBACK_FAST_PATH _WSAIOW(IOC_VENDOR, 16)
#endif

LPFN_ACCEPTEX lpAcceptEx = nullptr;
LPFN_DISCONNECTEX lpDisconnectEx = nullptr;

struct PER_HANDLE_DATA
{
  SOCKET socket;
  SOCKET listenSocket; // For use with SO_UPDATE_ACCEPT_CONTEXT
  SOCKADDR_IN clientAddr;
};

struct PER_IO_DATA
{
  OVERLAPPED overlapped;
  WSABUF wsaBuf;
  char acceptBuffer[2 * (sizeof(SOCKADDR_STORAGE) + 16)];
  char ioBuffer[2048];
  int operation;
};

enum
{
  OP_ACCEPT,
  OP_READ,
  OP_WRITE,
  OP_DISCONNECT
};

static const char response[] = "HTTP/1.1 200 OK\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 12\r\n"
                               "\r\n"
                               "Hello World!";

SOCKET CreateSocket()
{
  return WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
}

bool PostAccept(SOCKET listenSocket, HANDLE iocp);

DWORD WINAPI WorkerThread(LPVOID completionPortID)
{
  HANDLE completionPort = (HANDLE) completionPortID;
  DWORD bytesTransferred;
  ULONG_PTR completionKey;
  LPOVERLAPPED lpOverlapped = nullptr;

  while (true)
  {
    BOOL status = GetQueuedCompletionStatus(completionPort, &bytesTransferred, &completionKey, &lpOverlapped, INFINITE);

    std::cout << WSAGetLastError() << "\n";
    if (!status && lpOverlapped == nullptr && completionKey == 0)
    {
      std::cerr << "GQCS failed with: " << GetLastError() << "\n";
      continue;
    }

    if (lpOverlapped == nullptr)
    {
      // graceful shutdown
      break;
    }

    std::cout << "Worker thread processing completion key: " << completionKey
              << ", bytes transferred: " << bytesTransferred << "\n"
              << std::flush;

    auto* handleData = reinterpret_cast<PER_HANDLE_DATA*>(completionKey);
    auto* ioData = CONTAINING_RECORD(lpOverlapped, PER_IO_DATA, overlapped);

    switch (ioData->operation)
    {
    case OP_ACCEPT: {
      char notcp_delay = 1;
      setsockopt(handleData->socket, IPPROTO_TCP, TCP_NODELAY, &notcp_delay, sizeof(notcp_delay));
      setsockopt(handleData->socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*) &handleData->listenSocket,
                 sizeof(handleData->listenSocket));

      ioData->operation = OP_READ;
      ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));
      ioData->wsaBuf.buf = ioData->ioBuffer;
      ioData->wsaBuf.len = sizeof(ioData->ioBuffer);
      DWORD flags = 0;

      if (WSARecv(handleData->socket, &ioData->wsaBuf, 1, NULL, &flags, &ioData->overlapped, NULL) == SOCKET_ERROR &&
          WSAGetLastError() != WSA_IO_PENDING)
      {
        closesocket(handleData->socket);
        delete handleData;
        delete ioData;
      }

      PostAccept(handleData->listenSocket, completionPort);
      break;
    }

    case OP_READ: {
      ioData->operation = OP_WRITE;
      ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));
      ioData->wsaBuf.buf = const_cast<char*>(response);
      ioData->wsaBuf.len = sizeof(response) - 1;
      if (WSASend(handleData->socket, &ioData->wsaBuf, 1, NULL, 0, &ioData->overlapped, NULL) == SOCKET_ERROR &&
          WSAGetLastError() != WSA_IO_PENDING)
      {
        closesocket(handleData->socket);
        delete handleData;
        delete ioData;
      }
      break;
    }

    case OP_WRITE: {
      ioData->operation = OP_DISCONNECT;
      ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));
      if (!lpDisconnectEx(handleData->socket, &ioData->overlapped, TF_REUSE_SOCKET, 0) &&
          WSAGetLastError() != ERROR_IO_PENDING)
      {
        closesocket(handleData->socket);
        delete handleData;
        delete ioData;
      }
      break;
    }

    case OP_DISCONNECT: {
      PostAccept(handleData->listenSocket, completionPort);
      closesocket(handleData->socket);
      delete handleData;
      delete ioData;
      break;
    }
    }
  }

  return 0;
}

bool PostAccept(SOCKET listenSocket, HANDLE iocp)
{
  SOCKET acceptSocket = CreateSocket();
  if (acceptSocket == INVALID_SOCKET)
    return false;

  auto* handleData = new PER_HANDLE_DATA();
  handleData->socket = acceptSocket;
  handleData->listenSocket = listenSocket;

  auto* ioData = new PER_IO_DATA();
  ZeroMemory(ioData, sizeof(PER_IO_DATA));
  ioData->operation = OP_ACCEPT;

  HANDLE cp = CreateIoCompletionPort((HANDLE) acceptSocket, iocp, (ULONG_PTR) handleData, 0);
  if (!cp)
  {
    std::cerr << "CreateIoCompletionPort failed for accept socket: " << GetLastError() << "\n";
    closesocket(acceptSocket);
    delete handleData;
    delete ioData;
    return false;
  }

  DWORD bytes = 0;
  BOOL result = lpAcceptEx(listenSocket, acceptSocket, ioData->acceptBuffer, 0, sizeof(SOCKADDR_STORAGE) + 16,
                           sizeof(SOCKADDR_STORAGE) + 16, &bytes, &ioData->overlapped);

  if (!result && WSAGetLastError() != ERROR_IO_PENDING)
  {
    std::cerr << "AcceptEx failed with error: " << WSAGetLastError() << "\n";
    closesocket(acceptSocket);
    delete handleData;
    delete ioData;
    return false;
  }

  return true;
}

int main()
{
  WSADATA wsaData;
  SOCKET listenSocket = INVALID_SOCKET;
  HANDLE completionPort;
  SYSTEM_INFO systemInfo;
  std::vector<std::thread> threads;

  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    return 1;

  completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
  if (!completionPort)
    return 1;

  listenSocket = CreateSocket();
  BOOL opt = TRUE;
  setsockopt(listenSocket, SOL_SOCKET, SO_REUSEADDR, (char*) &opt, sizeof(opt));
  setsockopt(listenSocket, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (char*) &opt, sizeof(opt));

  SOCKADDR_IN serverAddr{};
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serverAddr.sin_port = htons(PORT);

  if (bind(listenSocket, (SOCKADDR*) &serverAddr, sizeof(serverAddr)) == SOCKET_ERROR ||
      listen(listenSocket, SOMAXCONN) == SOCKET_ERROR)
  {
    return 1;
  }

  if (!CreateIoCompletionPort((HANDLE) listenSocket, completionPort, 0, 0))
  {
    std::cerr << "Failed to associate listen socket with IOCP\n";
    return 1;
  }

  GUID guidAcceptEx = WSAID_ACCEPTEX;
  DWORD bytes = 0;
  WSAIoctl(listenSocket, SIO_GET_EXTENSION_FUNCTION_POINTER, &guidAcceptEx, sizeof(guidAcceptEx), &lpAcceptEx,
           sizeof(lpAcceptEx), &bytes, NULL, NULL);

  GUID guidDisconnectEx = WSAID_DISCONNECTEX;
  WSAIoctl(listenSocket, SIO_GET_EXTENSION_FUNCTION_POINTER, &guidDisconnectEx, sizeof(guidDisconnectEx),
           &lpDisconnectEx, sizeof(lpDisconnectEx), &bytes, NULL, NULL);

  if (!lpAcceptEx || !lpDisconnectEx)
  {
    std::cerr << "Failed to load AcceptEx or DisconnectEx\n";
    return 1;
  }

  printf("Server listening on port %d...\n", PORT);

  for (int i = 0; i < MAX_ACCEPTS; ++i)
    PostAccept(listenSocket, completionPort);

  GetSystemInfo(&systemInfo);
  DWORD threadCount = std::max<DWORD>(4, systemInfo.dwNumberOfProcessors * 2);
  for (DWORD i = 0; i < threadCount; ++i)
    threads.emplace_back(WorkerThread, completionPort);

  for (auto& t : threads)
    t.join();

  CloseHandle(completionPort);
  closesocket(listenSocket);
  WSACleanup();
  return 0;
}
