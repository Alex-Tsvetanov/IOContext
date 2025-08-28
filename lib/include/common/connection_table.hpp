#ifndef CONNECTION_TABLE_H
#define CONNECTION_TABLE_H

#include "common/per_client_storage.hpp"
#include <memory>
#include <unordered_map>

struct ConnTable
{
  std::unordered_map<fd_t, std::unique_ptr<PerClientStorage>> map;

  PerClientStorage* get(fd_t fd)
  {
    auto it = map.find(fd);
    return (it == map.end()) ? nullptr : it->second.get();
  }
  PerClientStorage* add(fd_t fd)
  {
    auto p = std::make_unique<PerClientStorage>();
    p->fd = fd;
    auto* raw = p.get();
    map.emplace(fd, std::move(p));
    return raw;
  }
  void erase(fd_t fd) { map.erase(fd); }
};

#endif // CONNECTION_TABLE_H