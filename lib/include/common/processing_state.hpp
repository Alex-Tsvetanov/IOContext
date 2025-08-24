#ifndef PROCESSING_STATE_H
#define PROCESSING_STATE_H

namespace ProcessingState
{
  static inline constexpr size_t not_started{static_cast<size_t>(-1)};

  // --- grouped, named states for HTTP/1.1 ---
  namespace HTTP11
  {
    static inline constexpr size_t method{0};
    static inline constexpr size_t path{1};
    static inline constexpr size_t protocol_version{2};
    static inline constexpr size_t header_name{3};
    static inline constexpr size_t header_value{4};
    static inline constexpr size_t cr{5};       // "\r"
    static inline constexpr size_t crlf{6};     // "\r\n"
    static inline constexpr size_t crlfcr{7};   // "\r\n\r"
    static inline constexpr size_t crlfcrlf{8}; // "\r\n\r\n"
    static inline constexpr size_t body{9};
  }; // namespace HTTP11
}; // namespace ProcessingState

#endif // PROCESSING_STATE_H
