#define CATCH_CONFIG_MAIN
#include <catch2/catch_all.hpp>
#include "common/processing_machine.hpp"
#include <iostream>

TEST_CASE("ProcessingMachine parses HTTP 1.1 requests correctly", "[ProcessingMachine]")
{
  ProcessingMachine machine;

  SECTION("Valid HTTP 1.1 request")
  {
    const char* request = "GET /index.html HTTP/1.1\r\n"
                          "Host: example.com\r\n"
                          "Connection: keep-alive\r\n"
                          "\r\n";
    machine.on_segment(request, strlen(request));

    REQUIRE(machine.state == ProcessingState::HTTP11::body);
    REQUIRE(machine.req.method == "GET");
    REQUIRE(machine.req.path == "/index.html");
    REQUIRE(machine.req.protocol == "HTTP/1.1");
    REQUIRE(machine.req.headers["Host"] == " example.com");
    REQUIRE(machine.req.headers["Connection"] == " keep-alive");
  }

  SECTION("Partial HTTP 1.1 request")
  {
    const char* request = "GET /index.html HTTP/1.1\r\n"
                          "Host: example.com\r\n"
                          "Connection: keep-alive"; // Missing final \r\n
    machine.on_segment(request, strlen(request));

    REQUIRE(machine.state == ProcessingState::HTTP11::header_value);
  }

  SECTION("Invalid HTTP 1.1 request with malformed header")
  {
    const char* request = "GET /index.html HTTP/1.1\r\n"
                          "Host example.com\r\n" // Missing colon
                          "Connection: keep-alive\r\n"
                          "\r\n";
    machine.on_segment(request, strlen(request));

    REQUIRE(machine.state == ProcessingState::error_state);
  }

  SECTION("Valid HTTP 1.1 request with body")
  {
    const char* request = "POST /submit HTTP/1.1\r\n"
                          "Host: example.com\r\n"
                          "Content-Length: 11\r\n"
                          "\r\n"
                          "Hello World";
    machine.on_segment(request, strlen(request));

    REQUIRE(machine.state == ProcessingState::HTTP11::body);
    REQUIRE(machine.req.method == "POST");
    REQUIRE(machine.req.path == "/submit");
    REQUIRE(machine.req.protocol == "HTTP/1.1");
    REQUIRE(machine.req.headers.size() == 2);
    REQUIRE(machine.req.headers["Host"] == " example.com");
    REQUIRE(machine.req.headers["Content-Length"] == " 11");
    REQUIRE(machine.req.body == "Hello World");
  }

  SECTION("Reset resets the request and state")
  {
    const char* request = "GET /index.html HTTP/1.1\r\n"
                          "Host: example.com\r\n"
                          "Connection: keep-alive\r\n"
                          "\r\n";
    machine.on_segment(request, strlen(request));
    machine.reset_parser();

    REQUIRE(machine.req.method.empty());
    REQUIRE(machine.req.path.empty());
    REQUIRE(machine.req.protocol.empty());
    REQUIRE(machine.req.headers.empty());
    REQUIRE(machine.req.body.empty());
    REQUIRE(machine.res.protocol == "HTTP/1.1");
    REQUIRE(machine.res.code == "200 OK");
    REQUIRE(machine.res.headers.empty());
    REQUIRE(machine.res.body.empty());
    REQUIRE(machine.state == ProcessingState::not_started);
    REQUIRE(machine.tmp_buf[0].empty());
    REQUIRE(machine.tmp_buf[1].empty());
    REQUIRE(machine.body_bytes_needed == 0);
    REQUIRE(machine.keep_alive == true);
  }
}
