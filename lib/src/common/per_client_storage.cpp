#include "common/per_client_storage.hpp"
#include "common/processing_state.hpp"
#include "common/workflow.hpp"
#include "server/worker.hpp"
#include "server/server.hpp"
#include <cstddef>
#include <memory>
#ifdef DEBUG
#include <iostream>
#include "common/debug_log.hpp"
#endif

void PerClientStorage::reset()
{
  current_request.reset();
  requests_needing_handlers.clear();
  requests_needing_responses.clear();
  requests_in_flight.clear();
  requests_ready.clear();
  while (!completed_recv_buffs.empty())
  {
    completed_recv_buffs.pop();
  }
}

void PerClientStorage::post(Workflow wf)
{
  switch (wf)
  {
  case Workflow::FindHandler:
  case Workflow::GenerateResponse:
  case Workflow::Parse:
  case Workflow::RequestFlush:
#ifndef __APPLE__
    owner->post_internal_event(this, wf);
#endif
    break;
  default:
    break;
  }
}
void PerClientStorage::handle(Workflow wf)
{
  switch (wf)
  {
  case Workflow::FindHandler:
    find_handler();
    break;
  case Workflow::GenerateResponse:
    generate_response();
    break;
  case Workflow::Parse:
    parse_step();
    break;
  default:
    break;
  }
}

void PerClientStorage::parse_step()
{
  if (!current_request)
  {
    current_request = std::make_shared<RequestProcessing>();
  }

  [[maybe_unused]] const auto t0 = std::chrono::steady_clock::now();
  bool emit_finding_handlers = false;
  bool emit_generating_response = false;
  while (!completed_recv_buffs.empty())
  {
    auto buff = completed_recv_buffs.front();
    completed_recv_buffs.pop();
    const char* seg = buff->data();
    size_t len = buff->size();
#ifdef DEBUG
    ts_std::cout << "Parsing buffer:\n" << std::string_view(seg, len) << std::endl;
#endif
    size_t processed = 0;
    while (processed < len)
    {
      processed += this->current_request->parser.on_segment(seg + processed, len - processed);
#ifdef DEBUG
      ts_std::cout << "Parsed " << processed << "/" << len << " bytes for fd=" << fd
                   << ", state=" << this->current_request->parser.state << std::endl;
#endif
      if (this->current_request->parser.state == ProcessingState::error_state)
      {
        this->requests_needing_handlers.insert(this->current_request);
        emit_finding_handlers = true;
        this->current_request = std::make_shared<RequestProcessing>();
      }
      if (this->current_request->parser.state > ProcessingState::HTTP11::path &&
          !this->current_request->request_handler)
      {
        this->requests_needing_handlers.insert(this->current_request);
        emit_finding_handlers = true;
      }
      if (processed < len && this->current_request->parser.state == ProcessingState::completed_state)
      {
        if (this->current_request->request_handler)
        {
          emit_generating_response = true;
          this->requests_needing_responses.insert(this->current_request);
        }
        this->current_request = std::make_shared<RequestProcessing>();
      }
    }
  }
  if (emit_finding_handlers)
  {
    this->post(Workflow::FindHandler);
  }
  if (emit_generating_response)
  {
    this->post(Workflow::GenerateResponse);
  }
}

bool PerClientStorage::on_send_completed()
{
  return this->requests_ready.empty() && this->requests_in_flight.empty();
}

void PerClientStorage::find_handler()
{
#ifdef DEBUG
  ts_std::cout << "Finding handler for fd=" << this->fd << std::endl;
#endif // DEBUG
  bool emit_generating_response = false;
  for (const auto& req : requests_needing_handlers)
  {
#ifdef DEBUG
    ts_std::cout << "Request state: " << req->parser.state << std::endl;
#endif // DEBUG
    if (req->parser.state == ProcessingState::error_state)
    {
      auto it = owner->server()->get_routes().find("500");
      if (it != owner->server()->get_routes().end())
      {
        req->request_handler = it->second;
        requests_needing_responses.insert(req);
        emit_generating_response = true;
      }
      continue;
    }
    // Find the appropriate handler for the request
    auto it = owner->server()->get_routes().find(req->parser.req.path);
    if (it != owner->server()->get_routes().end())
    {
      req->request_handler = it->second;
      requests_needing_responses.insert(req);
      emit_generating_response = true;
    }
    else
    {
      auto it = owner->server()->get_routes().find("404");
      if (it != owner->server()->get_routes().end())
      {
        req->request_handler = it->second;
        requests_needing_responses.insert(req);
        emit_generating_response = (req->parser.state == ProcessingState::completed_state);
      }
    }
  }
  requests_needing_handlers.clear();
  if (emit_generating_response)
  {
    this->post(Workflow::GenerateResponse);
  }
}

void PerClientStorage::generate_response()
{
  for (auto& req : requests_needing_responses)
  {
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
      // End of headers
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
  this->requests_ready.insert(this->requests_needing_responses.begin(), this->requests_needing_responses.end());
  this->requests_needing_responses.clear();
  this->post(Workflow::RequestFlush);
}
