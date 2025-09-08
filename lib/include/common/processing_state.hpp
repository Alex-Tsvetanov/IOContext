#ifndef PROCESSING_STATE_H
#define PROCESSING_STATE_H

#include <cstdint>

namespace ProcessingState
{
  using type = uint64_t;
  static inline constexpr type not_started{static_cast<type>(-1)};
  static inline constexpr type error_state{static_cast<type>(-2)};
  static inline constexpr type completed_state{static_cast<type>(-3)};

  // --- grouped, named states for HTTP/1.1 ---
  namespace HTTP11
  {
    static inline constexpr ProcessingState::type method{0};
    static inline constexpr ProcessingState::type path{1};
    static inline constexpr ProcessingState::type protocol_version{2};
    static inline constexpr ProcessingState::type header_name{3};
    static inline constexpr ProcessingState::type header_value{4};
    static inline constexpr ProcessingState::type cr{5};       // "\r"
    static inline constexpr ProcessingState::type crlf{6};     // "\r\n"
    static inline constexpr ProcessingState::type crlfcr{7};   // "\r\n\r"
    static inline constexpr ProcessingState::type crlfcrlf{8}; // "\r\n\r\n"
    static inline constexpr ProcessingState::type body{9};
  }; // namespace HTTP11
}; // namespace ProcessingState

#endif // PROCESSING_STATE_H
