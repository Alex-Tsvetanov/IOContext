#define UNICODE
#include <WinSock2.h>
#include <Windows.h>
#include <MSWSock.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <string>
#include <thread>
#include <iostream>

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")

#define PORT 8080
#define BUFFER_SIZE 2048

struct PER_HANDLE_DATA
{
  SOCKET socket;
  SOCKADDR_IN clientAddr;
};

struct PER_IO_DATA
{
  OVERLAPPED overlapped;
  WSABUF wsaBuf;
  char buffer[BUFFER_SIZE];
  int operation;
};

enum
{
  OP_READ,
  OP_WRITE
};

static const char response[] = "HTTP/1.1 200 OK\r\n"
                               "Content-Type: text/plain\r\n"
                               "Content-Length: 12\r\n"
                               "\r\n"
                               "Hello World!";

DWORD WINAPI WorkerThread(LPVOID completionPortID)
{
  HANDLE completionPort = (HANDLE) completionPortID;
  DWORD bytesTransferred;
  ULONG_PTR completionKey;
  PER_IO_DATA* ioData;
  PER_HANDLE_DATA* handleData;

  while (true)
  {
    BOOL status =
      GetQueuedCompletionStatus(completionPort, &bytesTransferred, &completionKey, (LPOVERLAPPED*) &ioData, INFINITE);

    handleData = (PER_HANDLE_DATA*) completionKey;

    if (bytesTransferred == 0 && ioData == NULL)
      break;

    if (!status || bytesTransferred == 0)
    {
      closesocket(handleData->socket);
      delete handleData;
      delete ioData;
      continue;
    }

    switch (ioData->operation)
    {
    case OP_READ: {
      ZeroMemory(&ioData->overlapped, sizeof(OVERLAPPED));
      ioData->operation = OP_WRITE;
      ioData->wsaBuf.buf = const_cast<char*>(response);
      ioData->wsaBuf.len = sizeof(response) - 1;
      WSASend(handleData->socket, &ioData->wsaBuf, 1, NULL, 0, &ioData->overlapped, NULL);
      break;
    }

    case OP_WRITE: {
      shutdown(handleData->socket, SD_SEND);
      closesocket(handleData->socket);
      delete handleData;
      delete ioData;
      break;
    }
    }
  }
  return 0;
}

int main()
{
  WSADATA wsaData;
  SOCKET listenSocket = INVALID_SOCKET;
  HANDLE completionPort;
  SYSTEM_INFO systemInfo;
  std::vector<std::thread> threads;

  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
  {
    fprintf(stderr, "WSAStartup failed: %d\n", WSAGetLastError());
    return 1;
  }

  completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
  if (!completionPort)
  {
    fprintf(stderr, "CreateIoCompletionPort failed: %lu\n", GetLastError());
    WSACleanup();
    return 1;
  }

  GetSystemInfo(&systemInfo);
  DWORD threadCount = std::max<DWORD>(4, systemInfo.dwNumberOfProcessors * 2);
  for (DWORD i = 0; i < threadCount; ++i)
    threads.emplace_back(WorkerThread, completionPort);

  listenSocket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
  if (listenSocket == INVALID_SOCKET)
  {
    fprintf(stderr, "WSASocket failed: %d\n", WSAGetLastError());
    CloseHandle(completionPort);
    WSACleanup();
    return 1;
  }

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
    fprintf(stderr, "bind/listen failed: %d\n", WSAGetLastError());
    closesocket(listenSocket);
    CloseHandle(completionPort);
    WSACleanup();
    return 1;
  }

  printf("Server listening on port %d with %lu threads...\n", PORT, threadCount);

  while (true)
  {
    PER_HANDLE_DATA* handleData = new PER_HANDLE_DATA();
    int addrLen = sizeof(handleData->clientAddr);
    handleData->socket = accept(listenSocket, (SOCKADDR*) &handleData->clientAddr, &addrLen);
    if (handleData->socket == INVALID_SOCKET)
    {
      delete handleData;
      continue;
    }

    DWORD flag = 1;
    setsockopt(handleData->socket, IPPROTO_TCP, TCP_NODELAY, (char*) &flag, sizeof(flag));

    if (!CreateIoCompletionPort((HANDLE) handleData->socket, completionPort, (ULONG_PTR) handleData, 0))
    {
      closesocket(handleData->socket);
      delete handleData;
      continue;
    }

    PER_IO_DATA* ioData = new PER_IO_DATA();
    ZeroMemory(ioData, sizeof(PER_IO_DATA));
    ioData->operation = OP_READ;
    ioData->wsaBuf.buf = ioData->buffer;
    ioData->wsaBuf.len = BUFFER_SIZE;

    DWORD flags = 0;
    if (WSARecv(handleData->socket, &ioData->wsaBuf, 1, NULL, &flags, &ioData->overlapped, NULL) == SOCKET_ERROR &&
        WSAGetLastError() != WSA_IO_PENDING)
    {
      closesocket(handleData->socket);
      delete handleData;
      delete ioData;
    }
  }

  for ([[maybe_unused]] auto& t : threads)
    PostQueuedCompletionStatus(completionPort, 0, 0, NULL);
  for (auto& t : threads)
    t.join();

  CloseHandle(completionPort);
  closesocket(listenSocket);
  WSACleanup();
  return 0;
}
