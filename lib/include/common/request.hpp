#ifndef REQUEST_H
#define REQUEST_H

#include <string>
#include <unordered_map>

struct Request
{
  std::string method, path, protocol;
  std::unordered_map<std::string, std::string> headers;
  std::string body;

  void reset()
  {
    method.clear();
    path.clear();
    protocol.clear();
    headers.clear();
    body.clear();
  }
};

#endif // REQUEST_H