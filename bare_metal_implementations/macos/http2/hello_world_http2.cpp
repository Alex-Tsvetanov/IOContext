#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/event.h>
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
#include <deque>

constexpr int PORT = 8443;
constexpr int MAX_EVENTS = 1024;
constexpr int READ_BUF_SIZE = 8192;

enum class ConnType { Unknown, Http1, Http2 };

struct Connection {
    int fd{-1};
    int kq{-1};
    SSL* ssl{nullptr};
    nghttp2_session* h2session{nullptr};
    ConnType type{ConnType::Unknown};
    bool handshake_done{false};
    bool closed{false};
    bool want_write{false};

    std::deque<std::vector<uint8_t>> write_queue;

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

// ---- Enqueue Write & Flush ----

void arm_write(Connection& conn) {
    struct kevent kev;
    EV_SET(&kev, conn.fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr); // Level-triggered
    kevent(conn.kq, &kev, 1, nullptr, 0, nullptr);
    conn.want_write = true;
}

void disarm_write(Connection& conn) {
    struct kevent kev;
    EV_SET(&kev, conn.fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(conn.kq, &kev, 1, nullptr, 0, nullptr);
    conn.want_write = false;
}

void enqueue_write(Connection& conn, const void* data, size_t len) {
    conn.write_queue.emplace_back((const uint8_t*)data, (const uint8_t*)data + len);
    if (!conn.want_write)
        arm_write(conn);
}

bool try_flush(Connection& conn) {
    while (!conn.write_queue.empty()) {
        auto& front = conn.write_queue.front();
        int n = SSL_write(conn.ssl, front.data(), (int)front.size());
        if (n > 0) {
            if ((size_t)n < front.size()) {
                front.erase(front.begin(), front.begin() + n);
                conn.want_write = true;
                return false;
            }
            conn.write_queue.pop_front();
        } else {
            int err = SSL_get_error(conn.ssl, n);
            if (err == SSL_ERROR_WANT_WRITE) {
                conn.want_write = true;
                return false;
            } else {
                conn.closed = true;
                return false;
            }
        }
    }

    conn.want_write = !conn.write_queue.empty();

    // Keep write armed if HTTP/2 or pending TLS
    if (conn.type == ConnType::Http2 && conn.h2session &&
        (conn.want_write || nghttp2_session_want_write(conn.h2session))) {
        arm_write(conn);
        return true;
    }

    disarm_write(conn);
    return true;
}

// ---- ALPN Callback ----

static int alpn_select_cb(SSL*, const unsigned char** out,
                          unsigned char* outlen,
                          const unsigned char* in, unsigned int inlen,
                          void*) {
    static const unsigned char h2[] = {'h','2'};
    static const unsigned char http11[] = {'h','t','t','p','/','1','.','1'};

    const unsigned char* ptr = in;
    const unsigned char* end = in + inlen;
    while (ptr < end) {
        unsigned char len = *ptr++;
        if (ptr + len > end) break;
        if (len == 2 && memcmp(ptr, "h2", 2) == 0) {
            *out = h2; *outlen = 2;
            return SSL_TLSEXT_ERR_OK;
        }
        if (len == 8 && memcmp(ptr, "http/1.1", 8) == 0) {
            *out = http11; *outlen = 8;
            return SSL_TLSEXT_ERR_OK;
        }
        ptr += len;
    }
    return SSL_TLSEXT_ERR_NOACK;
}

void init_ssl() {
    SSL_library_init();
    SSL_load_error_strings();
    OpenSSL_add_all_algorithms();

    ssl_ctx = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(ssl_ctx, "cert.pem", SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(ssl_ctx, "key.pem", SSL_FILETYPE_PEM);
    SSL_CTX_set_alpn_select_cb(ssl_ctx, alpn_select_cb, nullptr);
}

// ---- nghttp2 Callbacks ----

static ssize_t send_callback(nghttp2_session*,
                             const uint8_t* data, size_t length,
                             int, void* user_data) {
    auto* conn = (Connection*)user_data;
    enqueue_write(*conn, data, length);
    return (ssize_t)length;
}

static int on_frame_recv(nghttp2_session* session,
                         const nghttp2_frame* frame, void* user_data) {
    auto* conn = (Connection*)user_data;

    if (frame->hd.type == NGHTTP2_HEADERS &&
        frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        int stream_id = frame->hd.stream_id;

        std::vector<nghttp2_nv> headers = {
            {(uint8_t*)":status",(uint8_t*)"200",7,3,NGHTTP2_NV_FLAG_NONE},
            {(uint8_t*)"content-type",(uint8_t*)"text/plain",12,10,NGHTTP2_NV_FLAG_NONE}
        };

        nghttp2_submit_headers(session, NGHTTP2_FLAG_END_HEADERS,
                               stream_id, nullptr, headers.data(), headers.size(), nullptr);

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

        // 🔹 Force connection close after this response for benchmark
        conn->closed = true;
    }

    return 0;
}

// ---- Worker using kqueue ----

struct Worker {
    int listen_fd;
    int kq;
    std::vector<std::unique_ptr<Connection>> conns;

    Worker(int lfd) : listen_fd(lfd) {
        kq = kqueue();
        struct kevent kev;
        EV_SET(&kev, listen_fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        kevent(kq, &kev, 1, nullptr, 0, nullptr);
    }

    void run() {
        std::vector<struct kevent> events(MAX_EVENTS);

        while (true) {
            // Use short timeout to periodically flush pending writes
            struct timespec timeout;
            timeout.tv_sec = 0;
            timeout.tv_nsec = 50 * 1000 * 1000; // 50ms

            int nfds = kevent(kq, nullptr, 0, events.data(), MAX_EVENTS, &timeout);

            for (int i = 0; i < nfds; i++) {
                int fd = (int)events[i].ident;

                if (fd == listen_fd && events[i].filter == EVFILT_READ) {
                    // ---- Accept new client ----
                    sockaddr_in caddr{};
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept(listen_fd, (sockaddr*)&caddr, &clen);
                    if (cfd < 0) continue;
                    make_nonblocking(cfd);

                    auto conn = std::make_unique<Connection>();
                    conn->fd = cfd;
                    conn->kq = kq;
                    conn->ssl = SSL_new(ssl_ctx);
                    SSL_set_fd(conn->ssl, cfd);
                    SSL_set_accept_state(conn->ssl);

                    struct kevent kev;
                    EV_SET(&kev, cfd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
                    kevent(kq, &kev, 1, nullptr, 0, nullptr);

                    conns.push_back(std::move(conn));
                    continue;
                }

                Connection* conn = nullptr;
                for (auto& c : conns) {
                    if (c->fd == fd) { conn = c.get(); break; }
                }
                if (!conn || conn->closed) continue;

                if (events[i].flags & EV_EOF) { conn->closed = true; continue; }

                if (events[i].filter == EVFILT_READ) {
                    // ---- Handle handshake or app data ----
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
                                try_flush(*conn);
                            } else {
                                conn->type = ConnType::Http1;
                            }
                        } else {
                            int err = SSL_get_error(conn->ssl, ret);
                            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
                                conn->closed = true;
                        }
                    } else {
                        uint8_t buf[READ_BUF_SIZE];
                        int n = SSL_read(conn->ssl, buf, sizeof(buf));
                        if (n > 0) {
                            if (conn->type == ConnType::Http2) {
                                nghttp2_session_mem_recv(conn->h2session, buf, n);
                                nghttp2_session_send(conn->h2session);
                                try_flush(*conn);
                            } else {
                                static constexpr char header_ending[] = "\r\n\r\n";
                                auto end_headers = std::search(buf, buf+n, header_ending, header_ending+4);
                                if (end_headers != buf+n) {
                                    const char resp[] =
                                        "HTTP/1.1 200 OK\r\n"
                                        "Content-Type: text/plain\r\n"
                                        "Content-Length: 16\r\n"
                                        "Connection: close\r\n\r\n"
                                        "Hello, HTTP/1.1!";
                                    enqueue_write(*conn, resp, sizeof(resp)-1);
                                    conn->closed = true;
                                }
                            }
                        } else {
                            int err = SSL_get_error(conn->ssl, n);
                            if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
                                conn->closed = true;
                        }
                    }
                } else if (events[i].filter == EVFILT_WRITE) {
                    // ---- Handle pending writes ----
                    if (conn->type == ConnType::Http2 && conn->h2session) {
                        nghttp2_session_send(conn->h2session);
                    }
                    try_flush(*conn);
                }
            }

            // ---- Idle HTTP/2 flush pump ----
            for (auto& c : conns) {
                if (!c->closed && c->type == ConnType::Http2 && c->h2session) {
                    if (nghttp2_session_want_write(c->h2session) || !c->write_queue.empty()) {
                        nghttp2_session_send(c->h2session);
                        try_flush(*c);
                    }
                }
            }

            // ---- Cleanup closed ----
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
