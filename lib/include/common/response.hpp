#ifndef RESPONSE_H
#define RESPONSE_H

#include <string>
#include <unordered_map>

struct Response
{
  std::string protocol{"HTTP/1.1"};
  std::string code{"200 OK"};
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

#endif // RESPONSE_H