#include "server/client_workflow.hpp"
#include "common/processing_state.hpp"
#include "common/request_processing.hpp"
#include "server/server.hpp"
#include "server/worker.hpp"
#include <memory>

#define DEBUG
#ifdef DEBUG
#include "common/debug_log.hpp"
#endif

// Main client workflow coroutine
Task client_workflow_coro(Worker* worker, PerClientStorage* client)
{
  auto handle = co_await current_handle();
  client->coro_handle = handle;

  // Set the client FD in the promise so I/O completions can find it
  handle.promise().client_fd = client->fd;

#ifdef DEBUG
  ts_std::cout << "[Coroutine " << handle.address() << "] Client workflow started for fd=" << client->fd << std::flush;
#endif

  // Pre-allocated read buffer on coroutine stack
  char read_buffer[Worker::BUF_SZ];

  // Main loop: handle requests until connection closes
  while (!client->closing)
  {
    worker->post_recv(client, read_buffer, sizeof(read_buffer), handle.address()); // Post recv to start receiving data
    co_yield false;                                                                // Wait for recv I/O completion

    // Get the number of bytes received from the promise
    int bytes_received = handle.promise().io_result;
    ts_std::cout << "[Coroutine " << handle.address() << "] Received " << bytes_received
                 << " bytes for fd=" << client->fd << std::flush;

    // Check if we should close (connection may have been closed during recv)
    if (client->closing)
    {
      break;
    }

    if (bytes_received <= 0)
    {
      // Connection closed or error
      break;
    }

#ifdef DEBUG
    ts_std::cout << "[Coroutine " << handle.address() << "] Processing " << bytes_received
                 << " bytes for fd=" << client->fd << std::flush;
#endif

    // ========== PARSE RECEIVED DATA ==========
    if (!client->current_request)
    {
      client->current_request = std::make_shared<RequestProcessing>();
    }

    bool emit_finding_handlers = false;
    bool emit_generating_response = false;
    int yield_counter = 0;

    // Parse the data directly from read_buffer
    const char* seg = read_buffer;
    size_t len = bytes_received;

    size_t processed = 0;
    while (processed < len)
    {
      processed += client->current_request->parser.on_segment(seg + processed, len - processed);

#ifdef DEBUG
      ts_std::cout << "[Coroutine " << handle.address() << "] Parsed " << processed << "/" << len
                   << " bytes for fd=" << client->fd << ", state=" << client->current_request->parser.state
                   << std::flush;
#endif

      // Yield occasionally to let other clients make progress
      if (++yield_counter % 10 == 0)
      {
        co_yield true; // Ready to resume immediately
      }

      if (client->current_request->parser.state == ProcessingState::error_state)
      {
        client->requests_needing_handlers.insert(client->current_request);
        emit_finding_handlers = true;
        client->current_request = std::make_shared<RequestProcessing>();
      }

      if (client->current_request->parser.state > ProcessingState::HTTP11::path &&
          !client->current_request->request_handler)
      {
        client->requests_needing_handlers.insert(client->current_request);
        emit_finding_handlers = true;
      }

      if (processed < len && client->current_request->parser.state == ProcessingState::completed_state)
      {
        if (client->current_request->request_handler)
        {
          emit_generating_response = true;
          client->requests_needing_responses.insert(client->current_request);
        }
        client->current_request = std::make_shared<RequestProcessing>();
      }
    }

    // ========== FIND HANDLERS ==========
    if (emit_finding_handlers)
    {
#ifdef DEBUG
      ts_std::cout << "[Coroutine " << handle.address() << "] Finding handler for fd=" << client->fd << std::flush;
#endif
      bool still_needs_response = false;
      for (const auto& req : client->requests_needing_handlers)
      {
        if (req->parser.state == ProcessingState::error_state)
        {
          auto it = worker->server()->get_routes().find("500");
          if (it != worker->server()->get_routes().end())
          {
            req->request_handler = it->second;
            client->requests_needing_responses.insert(req);
            still_needs_response = true;
          }
          continue;
        }

        auto it = worker->server()->get_routes().find(req->parser.req.path);
        if (it != worker->server()->get_routes().end())
        {
          req->request_handler = it->second;
          client->requests_needing_responses.insert(req);
          still_needs_response = true;
        }
        else
        {
          auto it = worker->server()->get_routes().find("404");
          if (it != worker->server()->get_routes().end())
          {
            req->request_handler = it->second;
            client->requests_needing_responses.insert(req);
            still_needs_response = (req->parser.state == ProcessingState::completed_state);
          }
        }
      }
      client->requests_needing_handlers.clear();
      emit_generating_response = emit_generating_response || still_needs_response;
    }

    // ========== GENERATE RESPONSES ==========
    if (emit_generating_response)
    {
      for (auto& req : client->requests_needing_responses)
      {
        // Yield before calling handler to allow other clients to progress
        co_yield true;

        std::invoke(req->request_handler, req->parser.req, req->parser.res);

        // First line
        {
          req->batched_send_data.iov.emplace_back((void*) "HTTP/1.1 ", 9);
          req->batched_send_data.iov.emplace_back((void*) req->parser.res.code.data(), req->parser.res.code.size());
          req->batched_send_data.iov.emplace_back((void*) "\r\n", 2);
        }

        // Headers
        {
          req->parser.res.headers["Content-Length"] = std::to_string(req->parser.res.body.size());
          for (const auto& h : req->parser.res.headers)
          {
            req->batched_send_data.iov.emplace_back((void*) h.first.data(), h.first.size());
            req->batched_send_data.iov.emplace_back((void*) ": ", 2);
            req->batched_send_data.iov.emplace_back((void*) h.second.data(), h.second.size());
            req->batched_send_data.iov.emplace_back((void*) "\r\n", 2);
          }
          req->batched_send_data.iov.emplace_back((void*) "\r\n", 2);
        }

        // Body
        {
          req->batched_send_data.iov.emplace_back((void*) req->parser.res.body.data(), req->parser.res.body.size());
        }

        req->batched_send_data.msg.msg_iov = req->batched_send_data.iov.data();
        req->batched_send_data.msg.msg_iovlen = req->batched_send_data.iov.size();

#ifdef DEBUG
        for (const auto& iovec : req->batched_send_data.iov)
        {
          ts_std::cout << std::string_view(static_cast<const char*>(iovec.iov_base), iovec.iov_len);
        }
        ts_std::cout << std::endl;
#endif
      }
      client->requests_ready.insert(client->requests_needing_responses.begin(),
                                    client->requests_needing_responses.end());
      client->requests_needing_responses.clear();
    }

    // ========== SEND RESPONSES ==========
    while (!client->requests_ready.empty() && !client->closing)
    {
      // Get the first ready request
      auto it = client->requests_ready.begin();
      auto& req = *it;

      // Post send and wait for completion
      worker->post_send(client, &req->batched_send_data.msg, handle.address());
      co_yield false; // Wait for send I/O completion

      // Check send result
      int bytes_sent = handle.promise().io_result;
      if (bytes_sent < 0)
      {
        // Send error - close connection
        client->closing = true;
        break;
      }

#ifdef DEBUG
      ts_std::cout << "[Coroutine " << handle.address() << "] Sent " << bytes_sent << " bytes for fd=" << client->fd
                   << std::flush;
#endif

      // Remove the completed request
      client->requests_ready.erase(it);
      client->served++;

      // After successful send, check keepalive limits
      if (client->served >= client->keepalive_limit)
      {
        client->closing = true;
        break;
      }
    }
  }

  // Close the connection
  worker->post_close(client->fd, handle.address());
  co_yield false; // Wait for close I/O completion

#ifdef DEBUG
  ts_std::cout << "[Coroutine " << handle.address() << "] Client workflow completed for fd=" << client->fd
               << std::flush;
#endif

  client->coro_handle.reset();
  handle.promise().client_fd = -1;
  co_return;
}
