// RequestContext implementation - platform independent
#include "request_context.hpp"
#include "iocp/iocp_client_storage.hpp" // Include the Windows implementation for now
#include <algorithm>
#include <string>

void RequestContext::reset_parser()
{
  state = PS::StartLine;
  acc.clear();
  body_bytes_needed = 0;
  keep_alive = true;
}

bool RequestContext::find_double_crlf(const std::string& s, size_t& pos)
{
  auto i = s.find("\r\n\r\n");
  if (i == std::string::npos)
    return false;
  pos = i;
  return true;
}

void RequestContext::parse_request_line(const std::string& line, Request& req)
{
  auto p1 = line.find(' ');
  auto p2 = (p1 == std::string::npos) ? std::string::npos : line.find(' ', p1 + 1);
  req.method = (p1 == std::string::npos) ? line : line.substr(0, p1);
  req.path = (p1 == std::string::npos || p2 == std::string::npos) ? "/" : line.substr(p1 + 1, p2 - p1 - 1);
  req.protocol = (p2 == std::string::npos) ? "HTTP/1.1" : line.substr(p2 + 1);
}

void RequestContext::parse_headers(const std::string& block, Request& req, bool& keep_alive, size_t& content_len)
{
  keep_alive = true;
  content_len = 0;
  size_t start = 0;
  while (start < block.size())
  {
    auto end = block.find("\r\n", start);
    if (end == std::string::npos)
      end = block.size();
    if (end == start)
      break;
    auto colon = block.find(':', start);
    if (colon != std::string::npos && colon < end)
    {
      std::string k = block.substr(start, colon - start);
      size_t vbeg = colon + 1;
      while (vbeg < end && (block[vbeg] == ' ' || block[vbeg] == '\t'))
        ++vbeg;
      std::string v = block.substr(vbeg, end - vbeg);
      req.headers.emplace(std::move(k), std::move(v));
    }
    start = end + 2;
  }
  auto it = req.headers.find("Connection");
  if (it != req.headers.end())
  {
    std::string v = it->second;
    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
    keep_alive = (v.find("close") == std::string::npos);
  }
  auto it2 = req.headers.find("Content-Length");
  if (it2 != req.headers.end())
  {
    content_len = (size_t) std::strtoull(it2->second.c_str(), nullptr, 10);
  }
}

void RequestContext::on_segment(const char* p, size_t n)
{
  acc.append(p, n);

  for (;;)
  {
    if (state == PS::StartLine)
    {
      auto eol = acc.find("\r\n");
      if (eol == std::string::npos)
        return;
      parse_request_line(acc.substr(0, eol), *owner->req);
      acc.erase(0, eol + 2);
      state = PS::Headers;
    }
    if (state == PS::Headers)
    {
      size_t hdr_end = 0;
      if (!find_double_crlf(acc, hdr_end))
        return;
      size_t content_len = 0;
      parse_headers(acc.substr(0, hdr_end + 2), *owner->req, keep_alive, content_len);
      acc.erase(0, hdr_end + 4);
      owner->req->body.clear();
      owner->req->body.reserve(content_len);
      body_bytes_needed = content_len;
      state = PS::Body;
    }
    if (state == PS::Body)
    {
      size_t take = std::min<size_t>(body_bytes_needed, acc.size());
      if (take > 0)
      {
        owner->req->body.append(acc.data(), take);
        acc.erase(0, take);
        body_bytes_needed -= take;
      }
      if (body_bytes_needed > 0)
        return;

      owner->enqueue_request_for_response(owner->req);
      owner->res = std::make_shared<Response>();
      owner->req = std::make_shared<Request>();
      state = PS::StartLine;
    }
  }
}