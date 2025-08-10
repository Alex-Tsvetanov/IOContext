#include "xhttp/poller.hpp"
#if defined(XHTTP_PLATFORM_MACOS)
  #include <sys/event.h>
  #include <sys/time.h>
  #include <vector>

namespace xhttp
{

  class KqueuePoller: public IPoller
  {
    int kq_{-1};
    std::vector<struct kevent> evs_;
    std::vector<PollEvent> out_;
    static void set_ev(struct kevent& kev, xhttp_socket_t fd, int16_t filt, uint16_t flags, void* user)
    {
      XHTTP_EV_SET(&kev, fd, filt, flags, 0, 0, user);
    }

  public:
    bool init() override
    {
      kq_ = kqueue();
      evs_.resize(1024);
      return kq_ >= 0;
    }
    bool add(xhttp_socket_t fd, uint32_t events, void* user) override
    {
      struct kevent ch[2];
      int n = 0;
      if (events & XHTTP_EV_READ)
        set_ev(ch[n++], fd, EVFILT_READ, XHTTP_EV_ADD | XHTTP_EV_ENABLE | XHTTP_EV_CLEAR, user);
      if (events & XHTTP_EV_WRITE)
        set_ev(ch[n++], fd, EVFILT_WRITE, XHTTP_EV_ADD | XHTTP_EV_DISABLE, user);
      return kevent(kq_, ch, n, nullptr, 0, nullptr) == 0;
    }
    bool mod(xhttp_socket_t fd, uint32_t events, void* user) override
    {
      struct kevent ch[2];
      int n = 0;
      if (events & XHTTP_EV_READ)
        set_ev(ch[n++], fd, EVFILT_READ, XHTTP_EV_ADD | XHTTP_EV_ENABLE | XHTTP_EV_CLEAR, user);
      else
        set_ev(ch[n++], fd, EVFILT_READ, XHTTP_EV_ADD | XHTTP_EV_DISABLE, user);
      if (events & XHTTP_EV_WRITE)
        set_ev(ch[n++], fd, EVFILT_WRITE, XHTTP_EV_ADD | XHTTP_EV_ENABLE, user);
      else
        set_ev(ch[n++], fd, EVFILT_WRITE, XHTTP_EV_ADD | XHTTP_EV_DISABLE, user);
      return kevent(kq_, ch, n, nullptr, 0, nullptr) == 0;
    }
    void del(xhttp_socket_t fd) override
    {
      struct kevent ch[2];
      set_ev(ch[0], fd, EVFILT_READ, XHTTP_EV_DELETE, nullptr);
      set_ev(ch[1], fd, EVFILT_WRITE, XHTTP_EV_DELETE, nullptr);
      kevent(kq_, ch, 2, nullptr, 0, nullptr);
    }
    int wait(int timeout_ms) override
    {
      struct timespec ts{timeout_ms / 1000, (timeout_ms % 1000) * 1000000L};
      int n = kevent(kq_, nullptr, 0, evs_.data(), (int) evs_.size(), &ts);
      if (n < 0)
        return n;
      out_.clear();
      out_.reserve(n);
      for (int i = 0; i < n; i++)
      {
        PollEvent pe{};
        pe.fd = (xhttp_socket_t) evs_[i].ident;
        pe.user = evs_[i].udata;
        if (evs_[i].filter == EVFILT_READ)
          pe.events |= XHTTP_EV_READ;
        if (evs_[i].filter == EVFILT_WRITE)
          pe.events |= XHTTP_EV_WRITE;
        if (evs_[i].flags & (EV_EOF | EV_ERROR))
          pe.events |= XHTTP_EV_ERR;
        out_.push_back(pe);
      }
      return n;
    }
    std::span<const PollEvent> events() const override { return out_; }
  };

  IPoller* make_poller()
  {
    return new KqueuePoller();
  }

} // namespace xhttp
#endif
