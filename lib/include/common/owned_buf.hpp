#ifndef OWNED_BUF_H
#define OWNED_BUF_H

#include <string_view>
#include <memory>
#include "./string_buffer.hpp"

struct OwnedBuf
{
  StringBuf wb{};
  bool eor{false};               // marks end-of-response (for Send batching)
  std::shared_ptr<void> guard{}; // keeps backing memory alive

  static OwnedBuf literal(const char* p, size_t n, bool eor = false);

  static OwnedBuf copy(std::string_view sv, bool eor = false);
};

#endif // OWNED_BUF_H