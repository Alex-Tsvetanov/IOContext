// RequestContext (streaming parser) - platform independent
#include <string>
#include "http.hpp"

// Forward declaration for platform-specific storage
struct PerClientStorage;

// Platform-independent request context interface
class RequestContext
{
public:
  PerClientStorage* owner{nullptr};
  enum class PS
  {
    StartLine,
    Headers,
    Body
  } state{PS::StartLine};
  std::string acc; // accumulates across segments
  size_t body_bytes_needed{0};
  bool keep_alive{true};

  void reset_parser();
  void on_segment(const char* p, size_t n);

  static bool find_double_crlf(const std::string& s, size_t& pos);
  static void parse_request_line(const std::string& line, Request& req);
  static void parse_headers(const std::string& block, Request& req, bool& keep_alive, size_t& content_len);
};