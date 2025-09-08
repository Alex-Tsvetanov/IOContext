#include "common/processing_machine.hpp"
#include "common/processing_state.hpp"
#include "common/debug_log.hpp"
#include <string>
#include <string_view>

void ProcessingMachine::reset_parser()
{
  req.reset();
  res.reset();
  state = ProcessingState::not_started;
  tmp_buf[0].clear();
  tmp_buf[1].clear();
  body_bytes_needed = -1;
  keep_alive = true;
}

size_t ProcessingMachine::on_segment(const char* p, size_t n)
{
  if (state == ProcessingState::not_started)
  {
    state = ProcessingState::HTTP11::method; // HTTP 1.1 only support
  }

#ifdef DEBUG
  ts_std::cout << "Parsing data:\n" << std::string_view(p, n) << std::endl;
#endif // DEBUG

  size_t i = 0;
  while (i < n && state != ProcessingState::error_state && state != ProcessingState::completed_state)
  {
    char c = p[i++];
    switch (state)
    {
    case ProcessingState::HTTP11::method: {
      if (c == ' ')
      {
        req.method = std::move(tmp_buf[0]);
        tmp_buf[0].clear();
        state = ProcessingState::HTTP11::path;
      }
      else if (c == '\r' || c == '\n')
      {
        state = ProcessingState::error_state;
      }
      else
      {
        tmp_buf[0] += c;
      }
      break;
    }

    case ProcessingState::HTTP11::path: {
      if (c == ' ')
      {
        req.path = std::move(tmp_buf[0]);
        tmp_buf[0].clear();
        state = ProcessingState::HTTP11::protocol_version;
      }
      else if (c == '\r' || c == '\n')
      {
        state = ProcessingState::error_state;
      }
      else
      {
        tmp_buf[0] += c;
      }
      break;
    }

    case ProcessingState::HTTP11::protocol_version: {
      if (c == '\r')
      {
        req.protocol = std::move(tmp_buf[0]);
        tmp_buf[0].clear();
        state = ProcessingState::HTTP11::cr;
      }
      else if (c == '\n')
      {
        state = ProcessingState::error_state;
      }
      else
      {
        tmp_buf[0] += c;
      }
      break;
    }
    case ProcessingState::HTTP11::header_name: {
      if (c == ':')
      {
        state = ProcessingState::HTTP11::header_value;
      }
      else if (c == '\r' || c == '\n')
      {
        state = ProcessingState::error_state;
      }
      else
      {
        tmp_buf[0] += c;
      }
      break;
    }

    case ProcessingState::HTTP11::header_value: {
      if (c == '\r')
      {
        // store header
        std::string name = std::move(tmp_buf[0]);
        std::string value = std::move(tmp_buf[1]);
        tmp_buf[0].clear();
        tmp_buf[1].clear();
        // store header
#ifdef DEBUG
        ts_std::cout << "Parsed header: " << name << ": " << value << std::endl;
#endif // DEBUG
        req.headers.emplace(std::move(name), std::move(value));
        if (req.headers.contains("Content-Length"))
        {
          body_bytes_needed = std::stoull(req.headers["Content-Length"]);
        }
        state = ProcessingState::HTTP11::cr;
      }
      else
      {
        tmp_buf[1] += c;
      }
      break;
    }

    case ProcessingState::HTTP11::cr: {
      if (c == '\n')
      {
        state = ProcessingState::HTTP11::crlf;
      }
      else
      {
        state = ProcessingState::error_state;
      }
      break;
    }
    case ProcessingState::HTTP11::crlf: {
      if (c == '\r')
      {
        state = ProcessingState::HTTP11::crlfcr;
      }
      else
      {
        state = ProcessingState::HTTP11::header_name;
        tmp_buf[0] += c;
      }
      break;
    }
    case ProcessingState::HTTP11::crlfcr: {
      if (c == '\n')
      {
        state = ProcessingState::HTTP11::crlfcrlf;
        state = (body_bytes_needed > 0) ? ProcessingState::HTTP11::body : ProcessingState::completed_state;
      }
      else
      {
        state = ProcessingState::error_state;
      }
      break;
    }
    case ProcessingState::HTTP11::body: {
      if (req.body.size() < static_cast<size_t>(body_bytes_needed))
      {
        req.body += c;
      }
      if (req.body.size() == static_cast<size_t>(body_bytes_needed))
      {
        state = ProcessingState::completed_state;
      }
      break;
    }
    }
  }
  return i;
}
