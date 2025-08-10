#include "xhttp/server.hpp"
#include <iostream>

int main() {
    using namespace xhttp;
    ServerConfig cfg;
    cfg.endpoint = {"0.0.0.0", 8080};
    cfg.threads = std::max(1u, std::thread::hardware_concurrency());
    cfg.keepalive_timeout_ms = 5000;
    cfg.max_keepalive_requests = 100;
    cfg.reuse_port = true;
    cfg.tcp_nodelay = true;

    auto builder = [&](Connection& c, const RequestView& req) {
        std::string body = "Hello from xhttp! You requested: " + std::string(req.path) + "\n";
        std::string header = "HTTP/1.1 200 OK\r\n"
                             "Content-Type: text/plain\r\n"
                             "Connection: keep-alive\r\n"
                             "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
        c.out.append(header.data(), header.size());
        c.out.append(body.data(), body.size());
        c.keep_alive = true;
    };

    HttpServer srv(cfg, builder);
    if (!srv.start()) {
        std::cerr << "Failed to start server\n";
        return 1;
    }
    std::cout << "Server started on " << cfg.endpoint.address << ":" << cfg.endpoint.port << std::endl;
    while (true) std::this_thread::sleep_for(std::chrono::seconds(60));
}
