#include "common/per_client_storage.hpp"
#include "common/processing_state.hpp"
#include "common/workflow.hpp"
#include "server/worker.hpp"
#include "server/server.hpp"
#include <cstring>
#include <iostream>

void PerClientStorage::post(Workflow wf)
{
  owner->post_task(fd, wf);
}

void PerClientStorage::parse_step()
{
  const auto t0 = std::chrono::steady_clock::now();
  int processed = 0;
  for (;;)
  {
    if (rxq.empty())
    {
      break;
    }
    int take = std::min<int>(PROC_MAX_SEGMENTS, (int) rxq.size());
    for (int i = 0; i < take; ++i)
    {
      auto seg = std::move(rxq.front());
      rxq.pop_front();
      parser.on_segment(seg.wb.buf, seg.wb.len);
      if (parser.state == ProcessingState::error_state)
      {
        std::cerr << "Error parsing HTTP request" << std::endl;
        closing = true;
        this->post(Workflow::RequestClose);
        return;
      }
      if (parser.state > ProcessingState::HTTP11::path) // path has been parsed
      {
        this->post(Workflow::FindHandler);
      }
      if (parser.state == ProcessingState::HTTP11::body) // body has started being parsed
      {
        if (parser.req.headers.count("Content-Length") > 0)
        {
          if (parser.req.body.size() == std::stoull(parser.req.headers["Content-Length"]))
          {
            this->post(Workflow::GenerateResponse);
          }
        }
        else
        {
          this->post(Workflow::GenerateResponse);
        }
      }
    }
    if (++processed >= PROC_MAX_SEGMENTS)
    {
      break;
    }
    if (std::chrono::steady_clock::now() - t0 >= PROC_TIME_BUDGET)
    {
      break;
    }
  }
}

bool PerClientStorage::on_send_completed()
{
  bool finished = false;
  tx_inflight.clear();
  send_inflight = false;
  if (inflight_eor)
  {
    finished = true;
  }
  inflight_eor = false;

  if (finished)
  {
    served++;
    if (served >= keepalive_limit || !keep_alive || closing)
    {
      return true;
    }
  }
  return false;
}

void PerClientStorage::find_handler()
{
  // Find the appropriate handler for the request
  auto it = owner->server()->get_routes().find(parser.req.path);
  if (it != owner->server()->get_routes().end())
  {
    request_handler = it->second;
  }
}

void PerClientStorage::generate_response()
{
  if (request_handler)
  {
    // Generate the response using the found handler
    std::invoke(request_handler, parser.req, parser.res);

    // initial headers
    {
      std::shared_ptr<std::string> response = std::make_shared<std::string>();
      *response += "HTTP/1.1 " + parser.res.code + "\r\n";
      *response += "Content-Length: " + std::to_string(parser.res.body.size()) + "\r\n";
      this->txq.emplace_back(OwnedBuf{StringBuf{response->size(), response->data()}, false, response});
    }

    // Additional headers
    {
      if (!parser.res.headers.empty())
      {
        for (const auto& h : parser.res.headers)
        {
          std::shared_ptr<std::string> header = std::make_shared<std::string>();
          *header += h.first + ": " + h.second + "\r\n";
          this->txq.emplace_back(OwnedBuf{StringBuf{header->size(), header->data()}, false, header});
        }
      }
      // End of headers
      this->txq.emplace_back(OwnedBuf::literal("\r\n", 2, false));
    }

    // Body
    {
      std::shared_ptr<std::string> body = std::make_shared<std::string>();
      *body += parser.res.body;
      this->txq.emplace_back(OwnedBuf{StringBuf{body->size(), body->data()}, false, body});
    }

    this->post(Workflow::RequestFlush);
  }
}
