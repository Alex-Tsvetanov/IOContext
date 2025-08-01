#pragma once

namespace io
{
  class Poller
  {
  public:
    Poller() = default;
    ~Poller() = default;

    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;
    Poller(Poller&&) = default;
    Poller& operator=(Poller&&) = default;

    void add();
  };
}