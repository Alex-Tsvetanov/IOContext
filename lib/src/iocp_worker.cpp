// Windows IOCP worker implementation
#include "iocp/iocp_worker.hpp"
#include "iocp/iocp_helpers.hpp"
#include "iocp/iocp_client_storage.hpp"
#include "iocp/iocp_types.hpp"
#include "http.hpp"
#include <memory>
#include <cstring>
#include <algorithm>

#ifdef _WIN32

#ifndef CONTAINING_RECORD
#include <cstddef>
#define CONTAINING_RECORD(ptr, type, member)                                                                         \
  (reinterpret_cast<type*>(reinterpret_cast<char*>(ptr) - offsetof(type, member)))
#endif

IOCPWorker::IOCPWorker(HANDLE iocp, SOCKET listen, ConnTable& table, uint16_t accepts_per_worker, uint16_t keepalive_limit,
         seconds idle_timeout)
  : iocp_(iocp)
  , listen_(listen)
  , table_(table)
  , accepts_per_worker_(accepts_per_worker)
  , keepalive_limit_(keepalive_limit)
  , idle_timeout_(idle_timeout)
{}

void IOCPWorker::post_accept(AcceptCtx* ac)
{
  if (ac->acceptSock != INVALID_SOCKET)
  {
    ac->acceptSock = INVALID_SOCKET;
  }
  ZeroMemory(&ac->hdr.ol, sizeof(ac->hdr.ol));
  ac->hdr.op = Op::Accept;
  ac->hdr.s = listen_;
  ac->acceptSock = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
  if (ac->acceptSock == INVALID_SOCKET)
    WinSockWrapper::die("WSASocket acceptSock");
  DWORD bytes = 0;
  BOOL ok =
    WinSockWrapper::AcceptExPtr(listen_, ac->acceptSock, ac->addrbuf, 0, ADDR_LEN, ADDR_LEN, &bytes, &ac->hdr.ol);
  if (!ok && WSAGetLastError() != ERROR_IO_PENDING)
    WinSockWrapper::die("AcceptEx");
}

void IOCPWorker::maybe_start_send_worker(PerClientStorage* c)
{
  int tx_count = 0;
  bool eor = false;

  {
    std::lock_guard lk(c->tx_mtx);
    c->kick_pending = false;
    if (c->send_inflight || c->txq.empty())
      return;

    for (auto it = c->txq.begin(); it != c->txq.end() && tx_count < MAX_WSABUF; ++it, ++tx_count)
    {
      c->tx[tx_count] = it->wb;
      if (it->eor)
      {
        eor = true;
        ++tx_count;
        break;
      }
    }
    if (tx_count == 0)
      return; // nothing to send (paranoia)

    c->inflight_count = tx_count;
    c->inflight_eor = eor;
    c->send_inflight = true;
  }

  ZeroMemory(&c->wctx.ol, sizeof(c->wctx.ol));
  c->wctx.op = Op::Send;
  c->wctx.s = c->s;

  DWORD sent = 0;
  int rc = WSASend(c->s, c->tx, tx_count, &sent, 0, &c->wctx.ol, nullptr);
  if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
  {
    std::lock_guard lk(c->tx_mtx);
    c->send_inflight = false;
    ::closesocket(c->s);
    c->closing = true;
  }
}

void IOCPWorker::run()
{
    // Accept pool
    accept_pool_.reserve(accepts_per_worker_);
    for (uint16_t i = 0; i < accepts_per_worker_; ++i)
    {
      accept_pool_.emplace_back(std::make_unique<AcceptCtx>());
      post_accept(accept_pool_.back().get());
    }

    for (;;)
    {
      DWORD bytes = 0;
      ULONG_PTR key = 0;
      OVERLAPPED* pol = nullptr;
      BOOL ok = GetQueuedCompletionStatus(iocp_, &bytes, &key, &pol, 1000);

      if (!pol)
      {
        table_.sweep_idle(idle_timeout_, [&](ConnHandle h) {
          if (auto* c = table_.try_get(h))
          {
            c->closing = true;
            table_.close_and_recycle(h);
          }
        });
        continue;
      }

      IoHeader* hdr = CONTAINING_RECORD(pol, IoHeader, ol);
      ConnHandle h = unpack_key(key);

      switch (hdr->op)
      {
      case Op::Accept: {
        auto* ac = CONTAINING_RECORD(hdr, AcceptCtx, hdr);
        SOCKET s = ac->acceptSock;

        if (!ok)
        {
          if (s != INVALID_SOCKET)
            ::closesocket(s);
          post_accept(ac);
          break;
        }
        if (setsockopt(s, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*) &listen_, sizeof(listen_)) == SOCKET_ERROR)
        {
          ::closesocket(s);
          post_accept(ac);
          break;
        }

        ConnHandle nh = table_.allocate(s);
        if (!CreateIoCompletionPort((HANDLE) s, iocp_, pack_key(nh), 0))
        {
          table_.close_and_recycle(nh);
          post_accept(ac);
          break;
        }
        BOOL nd = TRUE;
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*) &nd, sizeof(nd));

        if (auto* c = table_.try_get(nh))
        {
          c->init(s, iocp_, nh);
          c->mark_activity();
          start_recv(c);
        }
        post_accept(ac);
      }
      break;

      case Op::Recv: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        if (!ok || bytes == 0)
        {
          table_.close_and_recycle(h);
          break;
        }
        c->mark_activity();

        std::shared_ptr<std::vector<char>> hold;
        {
          std::lock_guard lk(c->rx_mtx);
          hold = c->rx_hold;
          c->rx_hold.reset();
          if (hold)
            c->rxq.emplace_back(OwnedBuf{WSABUF{(ULONG) bytes, hold->data()}, false, hold});
          if (!c->parse_inflight)
          {
            c->parse_inflight = true;
            PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->proc.ol);
          }
        }

        if (!c->closing)
          start_recv(c);
      }
      break;

      case Op::Process: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        auto t0 = steady_clock::now();
        int processed = 0;

        for (;;)
        {
          std::vector<OwnedBuf> segs;
          {
            std::lock_guard lk(c->rx_mtx);
            int take = std::min<int>(PROC_MAX_SEGMENTS, (int) c->rxq.size());
            for (int i = 0; i < take; ++i)
            {
              segs.emplace_back(std::move(c->rxq.front()));
              c->rxq.pop_front();
            }
          }
          if (segs.empty())
            break;

          for (auto& seg : segs)
          {
            c->http.on_segment(seg.wb.buf, seg.wb.len);
            if (++processed >= PROC_MAX_SEGMENTS)
              break;
          }
          if (processed >= PROC_MAX_SEGMENTS)
            break;
          if (steady_clock::now() - t0 >= PROC_TIME_BUDGET)
            break;
        }

        bool repost = false;
        {
          std::lock_guard lk(c->rx_mtx);
          if (!c->rxq.empty())
            repost = true;
          else
            c->parse_inflight = false;
        }
        if (repost)
          PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->proc.ol);
      }
      break;

      case Op::Respond: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        std::vector<std::shared_ptr<Request>> reqs;
        {
          std::lock_guard lr(c->resp_mtx);
          while (!c->respq.empty())
          {
            reqs.emplace_back(std::move(c->respq.front()));
            c->respq.pop_front();
          }
          c->respond_inflight = false;
        }

        for (auto& rq : reqs)
        {
          bool keep = c->http.keep_alive && (c->served < keepalive_limit_ - 1);
          const char* hdr = keep ? kHdrKeep : kHdrClose;
          c->tx_enqueue_literal(hdr, std::strlen(hdr), false);
          c->tx_enqueue_literal(kBody, sizeof(kBody) - 1, true);
        }

        {
          std::lock_guard lr(c->resp_mtx);
          if (!c->respq.empty() && !c->respond_inflight)
          {
            c->respond_inflight = true;
            PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->resp.ol);
          }
        }
      }
      break;

      case Op::Kick: {
        auto* c = table_.try_get(h);
        if (!c)
          break;
        maybe_start_send_worker(c);
      }
      break;

      case Op::Send: {
        auto* c = table_.try_get(h);
        if (!c)
          break;

        if (!ok)
        {
          table_.close_and_recycle(h);
          break;
        }

        bool finished = false;
        {
          std::lock_guard lk(c->tx_mtx);
          for (int i = 0; i < c->inflight_count && !c->txq.empty(); ++i)
            c->txq.pop_front();
          finished = c->inflight_eor;
          c->inflight_count = 0;
          c->inflight_eor = false;
          c->send_inflight = false;

          if (!c->txq.empty())
          {
            if (!c->kick_pending)
            {
              c->kick_pending = true;
              PostQueuedCompletionStatus(iocp_, 0, pack_key(h), &c->kick.ol);
            }
          }
        }

        if (finished)
        {
          c->served++;
          if (c->served < keepalive_limit_ && !c->closing)
          {
            c->mark_activity();
          }
          else
          {
            table_.close_and_recycle(h);
          }
        }
      }
      break;
      } // switch
    } // loop
  }
}