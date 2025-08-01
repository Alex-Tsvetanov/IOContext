#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>
#include <thread>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

class HttpSession : public std::enable_shared_from_this<HttpSession> {
    tcp::socket socket_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;
    http::response<http::string_body> res_;

public:
    explicit HttpSession(tcp::socket socket) : socket_(std::move(socket)) {}

    void run() { do_read(); }

private:
    void do_read() {
        auto self = shared_from_this();
        http::async_read(socket_, buffer_, req_,
            [self](beast::error_code ec, std::size_t) {
                if (!ec) self->handle_request();
            });
    }

    void handle_request() {
        res_ = { http::status::ok, req_.version() };
        res_.set(http::field::server, "Boost.Beast");
        res_.set(http::field::content_type, "text/plain");
        res_.keep_alive(false);
        res_.body() = "Hello, World!\n";
        res_.prepare_payload();
        do_write();
    }

    void do_write() {
        auto self = shared_from_this();
        http::async_write(socket_, res_,
            [self](beast::error_code ec, std::size_t) {
                if (!ec && self->res_.need_eof() == false)
                    self->do_read();
            });
    }
};

class HttpServer {
    asio::io_context ioc_;
    tcp::acceptor acceptor_;
    std::vector<std::thread> threads_;

public:
    HttpServer(asio::ip::address addr, unsigned short port, int threads)
        : ioc_(threads), acceptor_(ioc_, {addr, port}) {}

    void run(int thread_count) {
        do_accept();
        for (int i = 0; i < thread_count; ++i)
            threads_.emplace_back([this] { ioc_.run(); });
        for (auto& t : threads_) t.join();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](beast::error_code ec, tcp::socket socket) {
                if (!ec) std::make_shared<HttpSession>(std::move(socket))->run();
                do_accept();
            });
    }
};

int main() {
    try {
        HttpServer server(asio::ip::make_address("0.0.0.0"), 8080, std::thread::hardware_concurrency());
        server.run(std::thread::hardware_concurrency());
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}
