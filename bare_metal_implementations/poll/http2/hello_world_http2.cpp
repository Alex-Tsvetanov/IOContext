#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <nghttp2/nghttp2.h>

#include <cstring>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <algorithm>

constexpr int PORT = 8443;
constexpr int MAX_EVENTS = 1024;
constexpr int READ_BUF_SIZE = 8192;
constexpr int MAX_KEEPALIVE_REQUESTS = 100;

enum class ConnType { Unknown, Http1, Http2 };

struct Connection {
    int fd{-1};
    SSL* ssl{nullptr};
    nghttp2_session* h2session{nullptr};
    ConnType type{ConnType::Unknown};
    bool handshake_done{false};
    bool closed{false};

    int requests_served = 0;
    std::vector<uint8_t> buffer;

    ~Connection() {
        if (h2session) nghttp2_session_del(h2session);
        if (ssl) { SSL_shutdown(ssl); SSL_free(ssl); }
        if (fd >= 0) close(fd);
    }
};

static SSL_CTX* ssl_ctx = nullptr;

int make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    return fd;
}

#define MAKE_NV(K, V) \
    nghttp2_nv{(uint8_t*)K, (uint8_t*)V, sizeof(K) - 1, sizeof(V) - 1, NGHTTP2_NV_FLAG_NONE}

// ---- nghttp2 Callbacks ----

static ssize_t send_callback([[maybe_unused]] nghttp2_session* session,
                             const uint8_t* data, size_t length,
                             int /*flags*/, void* user_data) {
    Connection* conn = (Connection*)user_data;
    int n = SSL_write(conn->ssl, data, (int)length);
    if (n <= 0) return NGHTTP2_ERR_WOULDBLOCK;
    return n;
}

static int on_frame_recv(nghttp2_session* session,
                         const nghttp2_frame* frame, void* user_data) {
    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        [[maybe_unused]] Connection* conn = (Connection*)user_data;
        int stream_id = frame->hd.stream_id;

        std::vector<nghttp2_nv> headers;
        headers.push_back(MAKE_NV(":status", "200"));
        headers.push_back(MAKE_NV("content-type", "text/plain"));

        nghttp2_submit_headers(session, NGHTTP2_FLAG_END_HEADERS, stream_id,
                               nullptr, headers.data(), headers.size(), nullptr);

        static const char body[] = "Hello, HTTP/2!\n";
        nghttp2_data_provider provider;
        provider.source.ptr = (void*)body;
        provider.read_callback = [](nghttp2_session*, int32_t,
                                    uint8_t* buf, size_t len,
                                    uint32_t* data_flags,
                                    nghttp2_data_source* source,
                                    void*) -> ssize_t {
            const char* data = (const char*)source->ptr;
            size_t datalen = strlen(data);
            if (len < datalen) datalen = len;
            memcpy(buf, data, datalen);
            *data_flags = NGHTTP2_DATA_FLAG_EOF;
            return datalen;
        };

        nghttp2_submit_data(session, NGHTTP2_FLAG_END_STREAM, stream_id, &provider);
    }
    return 0;
}

// ---- TLS ALPN Callback ----

static int alpn_select_cb([[maybe_unused]] SSL* ssl, const unsigned char** out, unsigned char* outlen, const unsigned char* in, unsigned int inlen, void*)
{
    // Prioritize h2 if offered
    const unsigned char h2[] = {0x02,'h','2'};
    if (SSL_select_next_proto((unsigned char**)out, outlen,
                            h2, sizeof(h2),
                            in, inlen) == OPENSSL_NPN_NEGOTIATED) {
        return SSL_TLSEXT_ERR_OK;
    }
    // fallback to http/1.1
    const unsigned char http11[] = {0x08,'h','t','t','p','/','1','.','1'};
    if (SSL_select_next_proto((unsigned char**)out, outlen,
                            http11, sizeof(http11),
                            in, inlen) == OPENSSL_NPN_NEGOTIATED) {
        return SSL_TLSEXT_ERR_OK;
    }
    return SSL_TLSEXT_ERR_NOACK;
}

void init_ssl() {
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();

    ssl_ctx = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(ssl_ctx, "/mnt/c/users/alext/Documents/GitHub/IOContext/tests/unix/http2/cert.pem", SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(ssl_ctx, "/mnt/c/users/alext/Documents/GitHub/IOContext/tests/unix/http2/key.pem", SSL_FILETYPE_PEM);
    SSL_CTX_set_alpn_select_cb(ssl_ctx, alpn_select_cb, nullptr);
}

// ---- Worker ----

struct Worker {
    int listen_fd;
    int epfd;
    std::vector<std::unique_ptr<Connection>> conns;

    Worker(int lfd) : listen_fd(lfd) {
        epfd = epoll_create1(0);
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd;
        epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);
    }

    void run() {
        std::vector<epoll_event> events(MAX_EVENTS);

        while (true) {
            int nfds = epoll_wait(epfd, events.data(), MAX_EVENTS, 1000);

            for (int i = 0; i < nfds; i++) {
                int fd = events[i].data.fd;

                if (fd == listen_fd) {
                    // Accept new client
                    sockaddr_in caddr{};
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept4(listen_fd, (sockaddr*)&caddr, &clen, SOCK_NONBLOCK);
                    if (cfd < 0) continue;

                    auto conn = std::make_unique<Connection>();
                    conn->fd = cfd;
                    conn->ssl = SSL_new(ssl_ctx);
                    SSL_set_fd(conn->ssl, cfd);
                    SSL_set_accept_state(conn->ssl);

                    epoll_event cev{};
                    cev.events = EPOLLIN | EPOLLET;
                    cev.data.fd = cfd;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev);

                    conns.push_back(std::move(conn));
                } else {
                    Connection* conn = nullptr;
                    for (auto& c : conns) {
                        if (c->fd == fd) { conn = c.get(); break; }
                    }
                    if (!conn || conn->closed) continue;

                    if (!conn->handshake_done) {
                        int ret = SSL_accept(conn->ssl);
                        if (ret == 1) {
                            conn->handshake_done = true;

                            const unsigned char* alpn = nullptr;
                            unsigned alpn_len = 0;
                            SSL_get0_alpn_selected(conn->ssl, &alpn, &alpn_len);

                            if (alpn_len == 2 && memcmp(alpn, "h2", 2) == 0) {
                                conn->type = ConnType::Http2;

                                nghttp2_session_callbacks* callbacks;
                                nghttp2_session_callbacks_new(&callbacks);
                                nghttp2_session_callbacks_set_send_callback(callbacks, send_callback);
                                nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, on_frame_recv);
                                nghttp2_session_server_new(&conn->h2session, callbacks, conn);
                                nghttp2_session_callbacks_del(callbacks);

                                nghttp2_settings_entry iv[1] = {
                                    {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100}
                                };
                                nghttp2_submit_settings(conn->h2session, NGHTTP2_FLAG_NONE, iv, 1);
                                nghttp2_session_send(conn->h2session);
                            } else {
                                conn->type = ConnType::Http1;
                            }
                        } else {
                            int err = SSL_get_error(conn->ssl, ret);
                            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
                                conn->closed = true;
                        }
                    } else if (conn->type == ConnType::Http2) {
                        uint8_t buf[READ_BUF_SIZE];
                        int n = SSL_read(conn->ssl, buf, sizeof(buf));
                        if (n > 0) {
                            nghttp2_session_mem_recv(conn->h2session, buf, n);
                            nghttp2_session_send(conn->h2session);
                        } else {
                            int err = SSL_get_error(conn->ssl, n);
                            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
                                conn->closed = true;
                        }
                    } else if (conn->type == ConnType::Http1) {
                        uint8_t buf[READ_BUF_SIZE];
                        int n = SSL_read(conn->ssl, buf, sizeof(buf));
                        if (n > 0) {
                            // Naive HTTP/1.1
                            if (memmem(buf, n, "\r\n\r\n", 4)) {
                                const char resp[] =
                                    "HTTP/1.1 200 OK\r\n"
                                    "Content-Type: text/plain\r\n"
                                    "Content-Length: 16\r\n"
                                    "Connection: close\r\n\r\n"
                                    "Hello, HTTP/1.1!";
                                SSL_write(conn->ssl, resp, sizeof(resp) - 1);
                                conn->closed = true;
                            }
                        } else {
                            int err = SSL_get_error(conn->ssl, n);
                            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
                                conn->closed = true;
                        }
                    }
                }
            }

            // Cleanup closed
            conns.erase(std::remove_if(conns.begin(), conns.end(),
                                       [](auto& c){ return c->closed; }),
                        conns.end());
        }
    }
};

// ---- Main ----

int main() {
    init_ssl();

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
    make_nonblocking(listen_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, SOMAXCONN);

    std::cout << "Server listening on https://127.0.0.1:" << PORT << "\n";

    int cores = std::thread::hardware_concurrency();
    if (cores <= 0) cores = 4;

    std::vector<std::thread> workers;
    workers.reserve(cores);

    for (int i = 0; i < cores; i++) {
        workers.emplace_back([listen_fd] {
            Worker w(listen_fd);
            w.run();
        });
    }

    for (auto& t : workers) t.join();
}
