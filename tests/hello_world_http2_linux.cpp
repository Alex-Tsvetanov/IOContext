#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <functional>
#include <iostream>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

using namespace std::chrono;

// ===================== Tunables =====================================
static constexpr int RX_CAP = 8192;
static constexpr int MAX_WSABUF = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int ACCEPTS_PER_WORKER = 128;

static constexpr int PROC_MAX_SEGMENTS = 16;
static constexpr auto PROC_TIME_BUDGET = std::chrono::microseconds(250);

// Demo payload
static constexpr const char kBody[] = "Hello, World!\n";
static constexpr const char kHdrKeep[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\n";
static constexpr const char kHdrClose[] =
  "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: close\r\n\r\n";

// ===================== HTTP model (placeholders) =====================
struct Request
{
  std::string method, path, protocol;
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};
struct Response
{
  std::string protocol{"HTTP/1.1"};
  std::string code{"200 OK"};
  std::unordered_map<std::string, std::string> headers;
  std::string body;
};

// ===================== Epoll Event System ================================
enum class Op
{
  Accept,
  Recv,
  Send,
  Process,
  Respond
};

struct IoEvent
{
  Op op{};
  int socket{-1};
  void* user_data{nullptr};
};

// ===================== Buffer wrapper =================================
struct OwnedBuf
{
  std::vector<char> data;
  bool eor{false}; // marks end-of-response (for Send batching)

  static OwnedBuf literal(const char* p, size_t n, bool eor = false)
  {
    OwnedBuf buf;
    buf.data.assign(p, p + n);
    buf.eor = eor;
    return buf;
  }
  static OwnedBuf copy(std::string_view sv, bool eor = false)
  {
    OwnedBuf buf;
    buf.data.assign(sv.begin(), sv.end());
    buf.eor = eor;
    return buf;
  }
};

// ===================== Forward decl ===================================
struct PerClientStorage;

// ===================== RequestContext (streaming parser) ==============
struct RequestContext
{
  PerClientStorage* owner{nullptr};
  enum class PS
  {
    StartLine,
    Headers,
    Body
  } state{PS::StartLine};
  std::string acc; // accumulates across segments
  size_t body_bytes_needed{0};
  bool keep_alive{true};

  void reset_parser()
  {
    state = PS::StartLine;
    acc.clear();
    body_bytes_needed = 0;
    keep_alive = true;
  }

  static bool find_double_crlf(const std::string& s, size_t& pos)
  {
    auto i = s.find("\r\n\r\n");
    if (i == std::string::npos)
      return false;
    pos = i;
    return true;
  }
  static void parse_request_line(const std::string& line, Request& req)
  {
    auto p1 = line.find(' ');
    auto p2 = (p1 == std::string::npos) ? std::string::npos : line.find(' ', p1 + 1);
    req.method = (p1 == std::string::npos) ? line : line.substr(0, p1);
    req.path = (p1 == std::string::npos || p2 == std::string::npos) ? "/" : line.substr(p1 + 1, p2 - p1 - 1);
    req.protocol = (p2 == std::string::npos) ? "HTTP/1.1" : line.substr(p2 + 1);
  }
  static void parse_headers(const std::string& block, Request& req, bool& keep_alive, size_t& content_len)
  {
    keep_alive = true;
    content_len = 0;
    size_t start = 0;
    while (start < block.size())
    {
      auto end = block.find("\r\n", start);
      if (end == std::string::npos)
        end = block.size();
      if (end == start)
        break;
      auto colon = block.find(':', start);
      if (colon != std::string::npos && colon < end)
      {
        std::string k = block.substr(start, colon - start);
        size_t vbeg = colon + 1;
        while (vbeg < end && (block[vbeg] == ' ' || block[vbeg] == '\t'))
          ++vbeg;
        std::string v = block.substr(vbeg, end - vbeg);
        req.headers.emplace(std::move(k), std::move(v));
      }
      start = end + 2;
    }
    auto it = req.headers.find("Connection");
    if (it != req.headers.end())
    {
      std::string v = it->second;
      std::transform(v.begin(), v.end(), v.begin(), ::tolower);
      keep_alive = (v.find("close") == std::string::npos);
    }
    auto it2 = req.headers.find("Content-Length");
    if (it2 != req.headers.end())
    {
      content_len = (size_t) std::strtoull(it2->second.c_str(), nullptr, 10);
    }
  }

  void on_segment(const char* p, size_t n);
};

// ===================== Connection object ==============================
struct PerClientStorage
{
  // ---- sockets & ops ----
  int socket{-1};
  IoEvent recv_event{}, send_event{}, process_event{}, respond_event{};

  // ---- policy ----
  steady_clock::time_point last_active{steady_clock::now()};
  uint32_t served{0};
  bool closing{false};

  // ---- app state ----
  RequestContext http;
  std::shared_ptr<Request> req{std::make_shared<Request>()};
  std::shared_ptr<Response> res{std::make_shared<Response>()};

  // ============ RX side (mutex-protected) ============
  std::shared_ptr<std::vector<char>> rx_hold; // in-flight recv buffer
  std::mutex rx_mtx;
  std::deque<OwnedBuf> rxq;
  bool parse_inflight{false};

  // ============ Respond queue ============
  std::mutex resp_mtx;
  std::deque<std::shared_ptr<Request>> respq;
  bool respond_inflight{false};

  // ============ TX side (mutex-protected) ============
  std::mutex tx_mtx;
  std::deque<OwnedBuf> txq; // queue of chunks (front -> send)
  bool send_inflight{false};
  int inflight_count{0};
  bool inflight_eor{false};

  // cross-refs
  int epoll_fd{-1};

  void clear()
  {
    socket = -1;
    recv_event = IoEvent{};
    send_event = IoEvent{};
    process_event = IoEvent{};
    respond_event = IoEvent{};

    last_active = steady_clock::now();
    served = 0;
    closing = false;

    req = std::make_shared<Request>();
    res = std::make_shared<Response>();
    http.owner = this;
    http.reset_parser();

    rx_hold.reset();
    {
      std::scoped_lock lr(rx_mtx, resp_mtx, tx_mtx);
      rxq.clear();
      respq.clear();
      txq.clear();
      parse_inflight = false;
      respond_inflight = false;
      send_inflight = false;
      inflight_count = 0;
      inflight_eor = false;
    }

    epoll_fd = -1;
  }

  void init(int sock, int epfd)
  {
    clear();
    socket = sock;
    epoll_fd = epfd;

    recv_event.op = Op::Recv;
    recv_event.socket = socket;
    recv_event.user_data = this;

    send_event.op = Op::Send;
    send_event.socket = socket;
    send_event.user_data = this;

    process_event.op = Op::Process;
    process_event.socket = socket;
    process_event.user_data = this;

    respond_event.op = Op::Respond;
    respond_event.socket = socket;
    respond_event.user_data = this;

    last_active = steady_clock::now();
  }

  void mark_activity() { last_active = steady_clock::now(); }

  // ==== TX publishing (thread-safe) ====
  void tx_enqueue_literal(const char* p, size_t n, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::literal(p, n, eor));
    if (!send_inflight)
    {
      post_send_event();
    }
  }
  void tx_enqueue_copy(std::string_view sv, bool eor = false)
  {
    std::lock_guard lk(tx_mtx);
    txq.emplace_back(OwnedBuf::copy(sv, eor));
    if (!send_inflight)
    {
      post_send_event();
    }
  }

  void post_send_event()
  {
    epoll_event ev{};
    ev.events = EPOLLOUT | EPOLLET;
    ev.data.ptr = &send_event;
    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, socket, &ev);
  }

  // ==== Respond scheduling (thread-safe) ====
  void enqueue_request_for_response(std::shared_ptr<Request> r)
  {
    std::lock_guard lr(resp_mtx);
    respq.emplace_back(std::move(r));
    if (!respond_inflight)
    {
      respond_inflight = true;
      post_respond_event();
    }
  }

  void post_respond_event()
  {
    eventfd_t value = 1;
    int event_fd = *static_cast<int*>(respond_event.user_data);
    eventfd_write(event_fd, value);
  }
};

// ===================== Parser implementation ==========================
void RequestContext::on_segment(const char* p, size_t n)
{
  acc.append(p, n);

  for (;;)
  {
    if (state == PS::StartLine)
    {
      auto eol = acc.find("\r\n");
      if (eol == std::string::npos)
        return;
      parse_request_line(acc.substr(0, eol), *owner->req);
      acc.erase(0, eol + 2);
      state = PS::Headers;
    }
    if (state == PS::Headers)
    {
      size_t hdr_end = 0;
      if (!find_double_crlf(acc, hdr_end))
        return;
      size_t content_len = 0;
      parse_headers(acc.substr(0, hdr_end + 2), *owner->req, keep_alive, content_len);
      acc.erase(0, hdr_end + 4);
      owner->req->body.clear();
      owner->req->body.reserve(content_len);
      body_bytes_needed = content_len;
      state = PS::Body;
    }
    if (state == PS::Body)
    {
      size_t take = std::min<size_t>(body_bytes_needed, acc.size());
      if (take > 0)
      {
        owner->req->body.append(acc.data(), take);
        acc.erase(0, take);
        body_bytes_needed -= take;
      }
      if (body_bytes_needed > 0)
        return;

      owner->enqueue_request_for_response(owner->req);
      owner->res = std::make_shared<Response>();
      owner->req = std::make_shared<Request>();
      state = PS::StartLine;
    }
  }
}

// ===================== Epoll helpers ==================================
static inline void start_recv(PerClientStorage* c)
{
  auto hold = std::make_shared<std::vector<char>>(RX_CAP);

  {
    std::lock_guard lk(c->rx_mtx);
    c->rx_hold = hold;
  }

  // Set socket to non-blocking
  int flags = fcntl(c->socket, F_GETFL, 0);
  fcntl(c->socket, F_SETFL, flags | O_NONBLOCK);

  // Add to epoll for reading
  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = &c->recv_event;
  epoll_ctl(c->epoll_fd, EPOLL_CTL_ADD, c->socket, &ev);
}

static inline bool try_recv(PerClientStorage* c)
{
  auto hold = c->rx_hold;
  if (!hold) return false;

  ssize_t recvd = recv(c->socket, hold->data(), hold->size(), 0);
  if (recvd > 0)
  {
    std::shared_ptr<std::vector<char>> hold_copy;
    {
      std::lock_guard lk(c->rx_mtx);
      hold_copy = c->rx_hold;
      c->rx_hold.reset();
      if (hold_copy)
        c->rxq.emplace_back(OwnedBuf{std::vector<char>(hold_copy->begin(), hold_copy->begin() + recvd), false});
      if (!c->parse_inflight)
      {
        c->parse_inflight = true;
        // Post process event
        eventfd_t value = 1;
        int event_fd = *static_cast<int*>(c->process_event.user_data);
        eventfd_write(event_fd, value);
      }
    }
    return true;
  }
  else if (recvd == 0 || (recvd == -1 && errno != EAGAIN && errno != EWOULDBLOCK))
  {
    // Connection closed or error
    return false;
  }
  return true; // EAGAIN - no data available
}

static inline bool try_send(PerClientStorage* c)
{
  std::vector<OwnedBuf> to_send;
  {
    std::lock_guard lk(c->tx_mtx);
    if (c->send_inflight || c->txq.empty())
      return false;

    for (auto it = c->txq.begin(); it != c->txq.end() && to_send.size() < MAX_WSABUF; ++it)
    {
      to_send.push_back(std::move(*it));
      if (it->eor)
      {
        to_send.resize(to_send.size());
        break;
      }
    }
    if (to_send.empty())
      return false;

    c->send_inflight = true;
  }

  size_t total_sent = 0;
  bool success = true;

  for (auto& buf : to_send)
  {
    ssize_t sent = send(c->socket, buf.data.data(), buf.data.size(), 0);
    if (sent == -1)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        // Would block - stop here
        success = false;
        break;
      }
      else
      {
        // Error
        success = false;
        break;
      }
    }
    else if (sent == 0)
    {
      // Connection closed
      success = false;
      break;
    }
    else
    {
      total_sent += sent;
      if ((size_t)sent < buf.data.size())
      {
        // Partial send - adjust buffer and stop
        buf.data.erase(buf.data.begin(), buf.data.begin() + sent);
        success = false;
        break;
      }
    }
  }

  {
    std::lock_guard lk(c->tx_mtx);
    // Remove sent buffers
    for (size_t i = 0; i < to_send.size(); ++i)
    {
      if (!to_send[i].data.empty())
      {
        // Partial send - put back the remainder
        c->txq.push_front(std::move(to_send[i]));
        break;
      }
      if (!c->txq.empty())
        c->txq.pop_front();
    }

    c->send_inflight = false;

    if (success && to_send.back().eor)
    {
      c->served++;
      if (c->served >= 1000) // Close after 1000 requests for demo
      {
        c->closing = true;
      }
    }

    if (!c->txq.empty())
    {
      // More to send
      c->post_send_event();
    }
  }

  return success;
}

// ===================== Connection Table ==============================
class ConnTable
{
public:
  explicit ConnTable(size_t cap) : slots_(cap) {}

  PerClientStorage* allocate(int socket)
  {
    for (;;)
    {
      for (uint32_t i = 0; i < slots_.size(); ++i)
      {
        bool expected = false;
        if (slots_[i].in_use.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
          auto& slot = slots_[i];
          slot.client.clear();
          slot.client.init(socket, -1); // epoll_fd will be set later
          return &slot.client;
        }
      }
      ::usleep(1000); // Sleep for 1ms
    }
  }

  void close_and_recycle(int socket)
  {
    // Find by socket and close
    for (auto& slot : slots_)
    {
      if (slot.in_use.load(std::memory_order_acquire) && slot.client.socket == socket)
      {
        if (slot.in_use.exchange(false, std::memory_order_acq_rel))
        {
          auto& c = slot.client;
          if (c.socket != -1)
          {
            ::close(c.socket);
            c.socket = -1;
          }
        }
        break;
      }
    }
  }

private:
  struct ConnSlot
  {
    std::atomic<uint32_t> generation{1};
    std::atomic<bool> in_use{false};
    PerClientStorage client{};
  };

  std::vector<ConnSlot> slots_;
};

// ===================== Accept Context =================================
struct AcceptCtx
{
  IoEvent event;
  int accept_sock{-1};
  int listen_sock{-1};
  int epoll_fd{-1};
  ConnTable* table{nullptr};
};

// ===================== Worker =========================================
class Worker
{
public:
  Worker(int epfd, int listen, ConnTable& table, int keepalive_limit)
    : epoll_fd_(epfd)
    , listen_(listen)
    , table_(table)
    , keepalive_limit_(keepalive_limit)
  {
    // Create eventfd for custom events
    event_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = event_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, event_fd_, &ev);

    // Set up process and respond events
    process_event_.op = Op::Process;
    process_event_.user_data = &event_fd_;

    respond_event_.op = Op::Respond;
    respond_event_.user_data = &event_fd_;
  }

  ~Worker()
  {
    if (event_fd_ != -1)
      ::close(event_fd_);
  }

  void post_accept()
  {
    AcceptCtx* ac = new AcceptCtx();
    ac->listen_sock = listen_;
    ac->epoll_fd = epoll_fd_;
    ac->table = &table_;
    ac->event.op = Op::Accept;
    ac->event.user_data = ac;

    ac->accept_sock = accept4(listen_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (ac->accept_sock != -1)
    {
      // Accept succeeded immediately
      handle_accept(ac);
    }
    else if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
      // Add listen socket to epoll for accept
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLET;
      ev.data.ptr = ac;
      epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_, &ev);
    }
    else
    {
      delete ac;
    }
  }

  void run()
  {
    const int MAX_EVENTS = 1024;
    epoll_event events[MAX_EVENTS];

    post_accept(); // Start first accept

    while (!stop_flag_.load(std::memory_order_acquire))
    {
      int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);

      for (int i = 0; i < num_events; ++i)
      {
        const epoll_event& ev = events[i];

        if (ev.data.fd == event_fd_)
        {
          // Handle custom event
          eventfd_t value;
          eventfd_read(event_fd_, &value);
          continue;
        }

        IoEvent* io_event = static_cast<IoEvent*>(ev.data.ptr);
        if (!io_event) continue;

        switch (io_event->op)
        {
        case Op::Accept:
          handle_accept(static_cast<AcceptCtx*>(io_event->user_data));
          break;
        case Op::Recv:
          handle_recv(static_cast<PerClientStorage*>(io_event->user_data));
          break;
        case Op::Send:
          handle_send(static_cast<PerClientStorage*>(io_event->user_data));
          break;
        case Op::Process:
          handle_process(static_cast<PerClientStorage*>(io_event->user_data));
          break;
        case Op::Respond:
          handle_respond(static_cast<PerClientStorage*>(io_event->user_data));
          break;
        }
      }
    }
  }

private:
  int epoll_fd_;
  int listen_;
  int event_fd_{-1};
  ConnTable& table_;
  int keepalive_limit_;
  std::atomic<bool> stop_flag_{false};
  IoEvent process_event_, respond_event_;

  void handle_accept(AcceptCtx* ac)
  {
    int client_sock = ac->accept_sock;
    if (client_sock == -1)
    {
      // Try accept again
      client_sock = accept4(ac->listen_sock, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    }

    if (client_sock != -1)
    {
      // Set TCP_NODELAY
      int flag = 1;
      setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

      PerClientStorage* client = table_.allocate(client_sock);
      client->epoll_fd = ac->epoll_fd;
      client->process_event.user_data = &event_fd_;
      client->respond_event.user_data = &event_fd_;

      start_recv(client);
    }

    // Post next accept
    delete ac;
    post_accept();
  }

  void handle_recv(PerClientStorage* c)
  {
    if (!try_recv(c))
    {
      table_.close_and_recycle(c->socket);
    }
  }

  void handle_send(PerClientStorage* c)
  {
    if (!try_send(c))
    {
      table_.close_and_recycle(c->socket);
    }
  }

  void handle_process(PerClientStorage* c)
  {
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
        c->http.on_segment(seg.data.data(), seg.data.size());
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
    {
      eventfd_t value = 1;
      eventfd_write(event_fd_, value);
    }
  }

  void handle_respond(PerClientStorage* c)
  {
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
      (void)rq; // Suppress unused variable warning
      bool keep = c->http.keep_alive && (static_cast<int>(c->served) < keepalive_limit_ - 1);
      const char* hdr = keep ? kHdrKeep : kHdrClose;
      c->tx_enqueue_literal(hdr, std::strlen(hdr), false);
      c->tx_enqueue_literal(kBody, sizeof(kBody) - 1, true);
    }

    {
      std::lock_guard lr(c->resp_mtx);
      if (!c->respq.empty() && !c->respond_inflight)
      {
        c->respond_inflight = true;
        eventfd_t value = 1;
        eventfd_write(event_fd_, value);
      }
    }
  }
};

// ===================== Server scaffolding =============================
struct ServerConfig
{
  uint16_t port = DEFAULT_PORT;
  uint16_t threads = 1; // Start with single thread for simplicity
  uint16_t max_keepalive_requests = 1000;
  uint32_t table_capacity = 1u << 15; // 32768 slots
};

class Server
{
public:
  explicit Server(const ServerConfig& cfg) : cfg_(cfg), table_(cfg.table_capacity)
  {
    // Create epoll
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1)
    {
      perror("epoll_create1");
      exit(1);
    }
  }

  ~Server()
  {
    if (epoll_fd_ != -1)
      ::close(epoll_fd_);
    if (listen_ != -1)
      ::close(listen_);
  }

  void run()
  {
    // Create listen socket
    listen_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_ == -1)
    {
      perror("socket");
      exit(1);
    }

    int reuse = 1;
    if (setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1)
    {
      perror("setsockopt SO_REUSEADDR");
      exit(1);
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(cfg_.port);

    if (bind(listen_, (struct sockaddr*)&addr, sizeof(addr)) == -1)
    {
      perror("bind");
      exit(1);
    }

    if (listen(listen_, SOMAXCONN) == -1)
    {
      perror("listen");
      exit(1);
    }

    std::cout << "Server listening on port " << cfg_.port << std::endl;

    // Start worker
    Worker worker(epoll_fd_, listen_, table_, cfg_.max_keepalive_requests);
    worker.run();
  }

private:
  ServerConfig cfg_;
  int epoll_fd_{-1};
  int listen_{-1};
  ConnTable table_;
};

// ===================== main ==========================================
int main()
{
  ServerConfig cfg{};
  Server srv(cfg);
  srv.run();
  return 0;
}