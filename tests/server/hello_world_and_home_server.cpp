#include <server/server.hpp>

int main()
{
  ServerConfig config;
  config.port = 8080;
  config.threads = std::thread::hardware_concurrency() / 2;

  Server server(config);

  server.add_route("/hello", [](const Request&, Response& res) {
    res.code = "200 OK";
    res.body = "Hello, World!";
    res.protocol = "HTTP/1.1";
    res.headers["Content-Type"] = "text/plain";
  });

  server.add_route("/", [](const Request&, Response& res) {
    res.code = "200 OK";
    res.body = "<h1>Hello, World!</h1>";
    res.protocol = "HTTP/1.1";
    res.headers["Content-Type"] = "text/html";
  });

  server.run();

  return 0;
}