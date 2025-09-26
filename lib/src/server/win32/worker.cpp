#ifdef WIN32
#include "server/worker.hpp"
#include <windows.h>
#include <iostream>
#include <thread>

Worker::Worker(fd_t listener, const WorkerConfig& cfg, HANDLE iocp)
  : listener_(listener)
  , cfg_(cfg)
  , iocp_(iocp)
{
  // Associate the listener socket with the IOCP
  if (CreateIoCompletionPort(reinterpret_cast<HANDLE>(listener_), iocp_, reinterpret_cast<ULONG_PTR>(this), 0) ==
      nullptr)
  {
    std::cerr << "Failed to associate listener with IOCP: " << GetLastError() << std::endl;
    std::exit(1);
  }
}

void Worker::run()
{
  // Start worker threads
  for (int i = 0; i < cfg_.accepts_per_worker; ++i)
  {
    post_accept();
  }

  DWORD bytes_transferred;
  ULONG_PTR completion_key;
  OVERLAPPED* overlapped;

  while (true)
  {
    BOOL result = GetQueuedCompletionStatus(iocp_, &bytes_transferred, &completion_key, &overlapped, INFINITE);

    if (!result)
    {
      DWORD error = GetLastError();
      if (error != WAIT_TIMEOUT)
      {
        std::cerr << "GetQueuedCompletionStatus failed: " << error << std::endl;
      }
      continue;
    }

    // Process the completion event
    auto* event_data = reinterpret_cast<EventData*>(completion_key);
    if (event_data)
    {
      switch (event_data->op)
      {
      case Workflow::Accept:
        handle_accept(event_data);
        break;
      case Workflow::Recv:
        handle_recv(event_data, bytes_transferred);
        break;
      case Workflow::Send:
        handle_send(event_data, bytes_transferred);
        break;
      case Workflow::Parse:
      case Workflow::FindHandler:
      case Workflow::GenerateResponse:
        handle_internal_event(event_data);
        break;
      case Workflow::RequestFlush: {
        auto* c = table_.get(event_data->fd);
        if (!c)
        {
          break;
        }
        if (c->send_inflight)
        {
          break;
        }
        post_send(c);
      }
      break;
      case Workflow::RequestClose:
        handle_close(event_data);
        break;
      case Workflow::Cancelling:
        // no-op, just a barrier
        break;
      case Workflow::Closed:
        handle_close(event_data);
        break;
      default:
        std::cerr << "Unknown operation: " << static_cast<int>(event_data->op) << std::endl;
        break;
      }

      delete event_data;
    }
  }
}

void Worker::post_accept()
{
  auto* event_data = new EventData;
  event_data->fd = listener_;
  event_data->op = Workflow::Accept;

  auto* overlapped = new OVERLAPPED{};

  SOCKET client_socket = WSASocket(AF_INET, SOCK_STREAM, 0, nullptr, 0, WSA_FLAG_OVERLAPPED);
  if (client_socket == INVALID_SOCKET)
  {
    std::cerr << "Failed to create client socket: " << WSAGetLastError() << std::endl;
    delete event_data;
    delete overlapped;
    return;
  }

  if (!AcceptEx(listener_, client_socket, event_data->buffer, 0, sizeof(sockaddr_in) + 16, sizeof(sockaddr_in) + 16,
                nullptr, overlapped))
  {
    int error = WSAGetLastError();
    if (error != ERROR_IO_PENDING)
    {
      std::cerr << "AcceptEx failed: " << error << std::endl;
      delete event_data;
      delete overlapped;
      closesocket(client_socket);
      return;
    }
  }

  event_data->client_socket = client_socket;
}

void Worker::handle_accept(EventData* data)
{
  // Associate the client socket with the IOCP
  if (CreateIoCompletionPort(reinterpret_cast<HANDLE>(data->client_socket), iocp_, reinterpret_cast<ULONG_PTR>(data),
                             0) == nullptr)
  {
    std::cerr << "Failed to associate client socket with IOCP: " << GetLastError() << std::endl;
    closesocket(data->client_socket);
    return;
  }

  // Post a receive operation for the new client
  post_recv(data);

  // Post another accept operation
  post_accept();
}

void Worker::post_recv(EventData* data)
{
  auto* overlapped = new OVERLAPPED{};
  WSABUF buffer;
  buffer.buf = data->buffer;
  buffer.len = sizeof(data->buffer);

  DWORD flags = 0;
  if (WSARecv(data->client_socket, &buffer, 1, nullptr, &flags, overlapped, nullptr) == SOCKET_ERROR)
  {
    int error = WSAGetLastError();
    if (error != WSA_IO_PENDING)
    {
      std::cerr << "WSARecv failed: " << error << std::endl;
      closesocket(data->client_socket);
      delete overlapped;
      return;
    }
  }

  data->op = Workflow::Recv;
}

void Worker::handle_recv(EventData* data, DWORD bytes_transferred)
{
  if (bytes_transferred == 0)
  {
    closesocket(data->client_socket);
    return;
  }

  // Process the received data
  std::cout << "Received data: " << std::string(data->buffer, bytes_transferred) << std::endl;

  // Echo the data back to the client
  post_send(data, bytes_transferred);
}

void Worker::post_send(EventData* data, DWORD bytes_to_send)
{
  auto* overlapped = new OVERLAPPED{};
  WSABUF buffer;
  buffer.buf = data->buffer;
  buffer.len = bytes_to_send;

  if (WSASend(data->client_socket, &buffer, 1, nullptr, 0, overlapped, nullptr) == SOCKET_ERROR)
  {
    int error = WSAGetLastError();
    if (error != WSA_IO_PENDING)
    {
      std::cerr << "WSASend failed: " << error << std::endl;
      closesocket(data->client_socket);
      delete overlapped;
      return;
    }
  }

  data->op = Workflow::Send;
}

void Worker::handle_send(EventData* data, DWORD bytes_transferred)
{
  // Check if all data has been sent
  if (bytes_transferred < sizeof(data->buffer))
  {
    post_send(data, sizeof(data->buffer) - bytes_transferred);
  }
  else
  {
    // Close the connection after sending
    closesocket(data->client_socket);
  }
}

#endif