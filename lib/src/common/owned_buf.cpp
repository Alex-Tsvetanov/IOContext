#include "common/owned_buf.hpp"
#include <vector>

OwnedBuf OwnedBuf::literal(const char* p, size_t n, bool eor)
{
  return {StringBuf{n, const_cast<char*>(p)}, eor, {}};
}

OwnedBuf OwnedBuf::copy(std::string_view sv, bool eor)
{
  auto buf = std::make_shared<std::vector<char>>(sv.begin(), sv.end());
  return {StringBuf{buf->size(), buf->data()}, eor, buf};
}
