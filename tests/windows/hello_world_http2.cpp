#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <vector>
#include <memory>
#include <chrono>

using ssize_t = int64_t;
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <nghttp2/nghttp2.h>

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "Mswsock.lib")
#pragma comment(lib, "libssl.lib")
#pragma comment(lib, "libcrypto.lib")

using namespace std::chrono;

constexpr int PORT = 8443;
constexpr int ENC_BUF_SIZE = 8192;
constexpr int APP_BUF_SIZE = 8192;

enum class ConnType
{
  Unknown,
  Http1,
  Http2
};
enum class IoOp
{
  Read,
  Write
};

struct Connection;
struct PerIoContext
{
  OVERLAPPED overlapped{};
  WSABUF wsaBuf{};
  IoOp op;
  Connection* conn;
};

struct Connection
{
  SOCKET fd = INVALID_SOCKET;
  SSL* ssl = nullptr;
  BIO* rbio = nullptr;
  BIO* wbio = nullptr;
  nghttp2_session* h2session = nullptr;
  ConnType type = ConnType::Unknown;
  bool handshake_done = false;
  bool closed = false;

  std::vector<char> enc_read_buf;
  std::vector<char> enc_write_buf;
  std::vector<char> app_read_buf;

  PerIoContext readCtx{};
  PerIoContext writeCtx{};

  Connection(SOCKET s, SSL_CTX* ctx)
    : fd(s)
    , enc_read_buf(ENC_BUF_SIZE)
    , enc_write_buf(ENC_BUF_SIZE)
    , app_read_buf(APP_BUF_SIZE)
  {
    ssl = SSL_new(ctx);
    rbio = BIO_new(BIO_s_mem());
    wbio = BIO_new(BIO_s_mem());
    SSL_set_bio(ssl, rbio, wbio);
    SSL_set_accept_state(ssl);

    ZeroMemory(&readCtx.overlapped, sizeof(readCtx.overlapped));
    ZeroMemory(&writeCtx.overlapped, sizeof(writeCtx.overlapped));
    readCtx.conn = this;
    writeCtx.conn = this;
    readCtx.op = IoOp::Read;
    writeCtx.op = IoOp::Write;
  }

  ~Connection()
  {
    if (h2session)
      nghttp2_session_del(h2session);
    if (ssl)
      SSL_free(ssl);
    if (fd != INVALID_SOCKET)
      closesocket(fd);
  }
};

// ---- nghttp2 callbacks ----
static ssize_t send_callback(nghttp2_session*, const uint8_t* data, size_t length, int, void* user_data)
{
  Connection* conn = (Connection*) user_data;
  int n = SSL_write(conn->ssl, data, (int) length);
  if (n <= 0)
    return NGHTTP2_ERR_WOULDBLOCK;
  return n;
}

static int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame, void* user_data)
{
  if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST)
  {
    int stream_id = frame->hd.stream_id;

    std::vector<nghttp2_nv> headers;
    auto nv = [](const char* k, const char* v) {
      return nghttp2_nv{(uint8_t*) k, (uint8_t*) v, (uint16_t) strlen(k), (uint16_t) strlen(v), NGHTTP2_NV_FLAG_NONE};
    };
    headers.push_back(nv(":status", "200"));
    headers.push_back(nv("content-type", "text/plain"));

    nghttp2_submit_headers(session, NGHTTP2_FLAG_END_HEADERS, stream_id, nullptr, headers.data(), headers.size(),
                           nullptr);

    static const char body[] = "Hello, HTTP/2!\n";
    nghttp2_data_provider provider;
    provider.source.ptr = (void*) body;
    provider.read_callback = [](nghttp2_session*, int32_t, uint8_t* buf, size_t len, uint32_t* data_flags,
                                nghttp2_data_source* source, void*) -> ssize_t {
      const char* data = (const char*) source->ptr;
      size_t datalen = strlen(data);
      if (datalen > len)
        datalen = len;
      memcpy(buf, data, datalen);
      *data_flags = NGHTTP2_DATA_FLAG_EOF;
      return (ssize_t) datalen;
    };
    nghttp2_submit_data(session, NGHTTP2_FLAG_END_STREAM, stream_id, &provider);
  }
  return 0;
}

// ---- IOCP Server ----
class IocpServer
{
public:
  IocpServer() { WSAStartup(MAKEWORD(2, 2), &wsaData); }
  ~IocpServer() { WSACleanup(); }

  bool start()
  {
    init_ssl();

    listen_fd = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (listen_fd == INVALID_SOCKET)
      return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, (char*) &opt, sizeof(opt));
    bind(listen_fd, (sockaddr*) &addr, sizeof(addr));
    listen(listen_fd, SOMAXCONN);

    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    CreateIoCompletionPort((HANDLE) listen_fd, iocp, 0, 0);

    int threads = std::thread::hardware_concurrency();
    if (threads <= 0)
      threads = 4;
    for (int i = 0; i < threads; i++)
      workers.emplace_back(&IocpServer::worker_loop, this);

    std::cout << "Server listening on https://127.0.0.1:" << PORT << "\n";
    accept_loop();

    for (auto& t : workers)
      t.join();
    return true;
  }

private:
  WSADATA wsaData{};
  SOCKET listen_fd = INVALID_SOCKET;
  HANDLE iocp = NULL;
  std::vector<std::thread> workers;
  std::vector<std::unique_ptr<Connection>> conns;
  SSL_CTX* ssl_ctx = nullptr;

  void init_ssl()
  {
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
    ssl_ctx = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(ssl_ctx, "cert.pem", SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(ssl_ctx, "key.pem", SSL_FILETYPE_PEM);

    // const unsigned char alpn[] = "\x02h2\x08http/1.1";
    SSL_CTX_set_alpn_select_cb(
      ssl_ctx,
      [](SSL*, const unsigned char** out, unsigned char* outlen, const unsigned char* in, unsigned int inlen,
         void*) -> int {
        // Parse client's offered protocols
        // in[] is a sequence of length-prefixed ALPN strings
        for (unsigned int i = 0; i < inlen;)
        {
          unsigned int len = in[i];
          const unsigned char* proto = &in[i + 1];

          if (len == 2 && memcmp(proto, "h2", 2) == 0)
          {
            *out = proto;
            *outlen = len;
            return SSL_TLSEXT_ERR_OK;
          }
          if (len == 8 && memcmp(proto, "http/1.1", 8) == 0)
          {
            *out = proto;
            *outlen = len;
            return SSL_TLSEXT_ERR_OK;
          }

          i += 1 + len;
        }

        // Fallback: refuse ALPN (curl will fallback to 1.0 if allowed)
        return SSL_TLSEXT_ERR_NOACK;
      },
      nullptr);
  }

  void accept_loop()
  {
    while (true)
    {
      SOCKET client = WSAAccept(listen_fd, NULL, NULL, NULL, 0);
      if (client == INVALID_SOCKET)
        continue;

      u_long mode = 1;
      ioctlsocket(client, FIONBIO, &mode);

      auto conn = std::make_unique<Connection>(client, ssl_ctx);
      CreateIoCompletionPort((HANDLE) client, iocp, (ULONG_PTR) conn.get(), 0);

      start_read(conn.get());
      conns.push_back(std::move(conn));
    }
  }

  void start_read(Connection* conn)
  {
    DWORD flags = 0;
    conn->readCtx.wsaBuf.buf = conn->enc_read_buf.data();
    conn->readCtx.wsaBuf.len = (ULONG) conn->enc_read_buf.size();
    int rc = WSARecv(conn->fd, &conn->readCtx.wsaBuf, 1, NULL, &flags, &conn->readCtx.overlapped, NULL);
    if (rc == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
      conn->closed = true;
  }

  void flush_ssl_write(Connection* conn)
  {
    while (BIO_pending(conn->wbio) > 0)
    {
      int pending = (int) BIO_pending(conn->wbio);
      conn->enc_write_buf.resize(pending);

      int n = BIO_read(conn->wbio, conn->enc_write_buf.data(), pending);
      if (n > 0)
      {
        WSABUF buf;
        buf.buf = conn->enc_write_buf.data();
        buf.len = n;
        DWORD sent;
        int rc = WSASend(conn->fd, &buf, 1, &sent, 0, nullptr, nullptr);
        if (rc == SOCKET_ERROR)
        {
          int err = WSAGetLastError();
          if (err != WSA_IO_PENDING)
          {
            conn->closed = true;
            break;
          }
        }
      }
      else
        break;
    }
  }

  void worker_loop()
  {
    DWORD bytesTransferred;
    ULONG_PTR key;
    LPOVERLAPPED overlapped;
    while (true)
    {
      BOOL ok = GetQueuedCompletionStatus(iocp, &bytesTransferred, &key, &overlapped, INFINITE);
      auto conn = reinterpret_cast<Connection*>(key);
      auto ctx = reinterpret_cast<PerIoContext*>(overlapped);
      if (!ok || !conn || !ctx || conn->closed)
        continue;

      if (ctx->op == IoOp::Read)
      {
        if (bytesTransferred == 0)
        {
          conn->closed = true;
          continue;
        }

        // Feed encrypted data
        BIO_write(conn->rbio, conn->enc_read_buf.data(), bytesTransferred);

        // 1️⃣ TLS Handshake
        if (!conn->handshake_done)
        {
          int ret = SSL_accept(conn->ssl);
          flush_ssl_write(conn); // Always flush handshake

          if (ret == 1)
          {
            conn->handshake_done = true;
            const unsigned char* alpn = nullptr;
            unsigned alpn_len = 0;
            SSL_get0_alpn_selected(conn->ssl, &alpn, &alpn_len);

            if (alpn_len == 2 && memcmp(alpn, "h2", 2) == 0)
            {
              conn->type = ConnType::Http2;
              nghttp2_session_callbacks* cb;
              nghttp2_session_callbacks_new(&cb);
              nghttp2_session_callbacks_set_send_callback(cb, send_callback);
              nghttp2_session_callbacks_set_on_frame_recv_callback(cb, on_frame_recv);
              nghttp2_session_server_new(&conn->h2session, cb, conn);
              nghttp2_session_callbacks_del(cb);
              nghttp2_settings_entry iv[1] = {{NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100}};
              nghttp2_submit_settings(conn->h2session, NGHTTP2_FLAG_NONE, iv, 1);
              nghttp2_session_send(conn->h2session);
              flush_ssl_write(conn);
            }
            else
              conn->type = ConnType::Http1;
          }
        }

        // 2️⃣ Post-handshake data processing
        if (conn->handshake_done)
        {
          int n;
          while ((n = SSL_read(conn->ssl, conn->app_read_buf.data(), (int) conn->app_read_buf.size())) > 0)
          {
            if (conn->type == ConnType::Http2)
            {
              nghttp2_session_mem_recv(conn->h2session, (uint8_t*) conn->app_read_buf.data(), n);
              nghttp2_session_send(conn->h2session);
              flush_ssl_write(conn);
            }
            else
            {
              std::string req(conn->app_read_buf.data(), n);
              if (req.find("\r\n\r\n") != std::string::npos)
              {
                const char resp[] = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                                    "Content-Length: 16\r\nConnection: close\r\n\r\nHello, HTTP/1.1!";
                SSL_write(conn->ssl, resp, sizeof(resp) - 1);
                flush_ssl_write(conn);
                conn->closed = true;
                break;
              }
            }
          }
        }

        if (!conn->closed)
          start_read(conn);
      }
    }
  }
};

int main()
{
  IocpServer server;
  if (!server.start())
  {
    std::cerr << "Failed to start\n";
    return 1;
  }
  return 0;
}
