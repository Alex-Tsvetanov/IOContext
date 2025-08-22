#ifndef STRING_BUFFER_H
#define STRING_BUFFER_H

#include <cstddef>

struct StringBuf
{
  size_t len;
  const char* buf;
};

#endif // STRING_BUFFER_H
