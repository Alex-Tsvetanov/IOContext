#ifndef REQUEST_H
#define REQUEST_H

struct Request
{
  std::string method, path, protocol;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

#endif // REQUEST_H