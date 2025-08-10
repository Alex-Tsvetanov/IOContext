#include "xhttp/poller.hpp"
#if defined(XHTTP_PLATFORM_LINUX)
  #include <sys/epoll.h>
  #include <unistd.h>
  #include <vector>

namespace xhttp
{

  class EpollPoller: public IPoller
  {
    int ep_{-1};
    std::vector<epoll_event> evs_;
    std::vector<PollEvent> out_;

  public:
    bool init() override
    {
      ep_ = epoll_create1(EPOLL_CLOEXEC);
      evs_.resize(1024);
      return ep_ >= 0;
    }
    bool add(xhttp_socket_t fd, uint32_t events, void* user) override
    {
      epoll_event ev{};
      ev.data.ptr = user ? user : reinterpret_cast<void*>(static_cast<intptr_t>(fd));
      if (events & XHTTP_EV_READ)
        ev.events |= EPOLLIN | EPOLLRDHUP;
      if (events & XHTTP_EV_WRITE)
        ev.events |= EPOLLOUT;
      ev.events |= EPOLLET;
      return epoll_ctl(ep_, EPOLL_CTL_ADD, fd, &ev) == 0;
    }
    bool mod(xhttp_socket_t fd, uint32_t events, void* user) override
    {
      epoll_event ev{};
      ev.data.ptr = user ? user : reinterpret_cast<void*>(static_cast<intptr_t>(fd));
      if (events & XHTTP_EV_READ)
        ev.events |= EPOLLIN | EPOLLRDHUP;
      if (events & XHTTP_EV_WRITE)
        ev.events |= EPOLLOUT;
      ev.events |= EPOLLET;
      return epoll_ctl(ep_, EPOLL_CTL_MOD, fd, &ev) == 0;
    }
    void del(xhttp_socket_t fd) override
    {
      epoll_event ev{};
      epoll_ctl(ep_, EPOLL_CTL_DEL, fd, &ev);
    }
    int wait(int timeout_ms) override
    {
      int n = epoll_wait(ep_, evs_.data(), (int) evs_.size(), timeout_ms);
      if (n < 0)
        return n;
      out_.clear();
      out_.reserve(n);
      for (int i = 0; i < n; i++)
      {
        PollEvent pe{};
        pe.fd = -1;
        pe.user = evs_[i].data.ptr;
        if (evs_[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
          pe.events |= XHTTP_EV_ERR | XHTTP_EV_RDHUP;
        if (evs_[i].events & EPOLLIN)
          pe.events |= XHTTP_EV_READ;
        if (evs_[i].events & EPOLLOUT)
          pe.events |= XHTTP_EV_WRITE;
        intptr_t v = reinterpret_cast<intptr_t>(evs_[i].data.ptr);
        if (v && v < (1LL << 32))
          pe.fd = static_cast<int>(v);
        out_.push_back(pe);
      }
      return n;
    }
    std::span<const PollEvent> events() const override { return out_; }
  };

  IPoller* make_poller()
  {
    return new EpollPoller();
  }

} // namespace xhttp
#endif
