#include <iostream>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <errno.h>

int main() {
    // Create listen socket
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd == -1) {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1) {
        perror("setsockopt");
        close(listen_fd);
        return 1;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8080);

    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, SOMAXCONN) == -1) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    std::cout << "Server listening on port 8080" << std::endl;

    // Create epoll
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        close(listen_fd);
        return 1;
    }

    // Add listen socket to epoll
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) == -1) {
        perror("epoll_ctl");
        close(epoll_fd);
        close(listen_fd);
        return 1;
    }

    const int MAX_EVENTS = 10;
    struct epoll_event events[MAX_EVENTS];

    for (;;) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000);
        if (num_events == -1) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            if (events[i].data.fd == listen_fd) {
                // Accept connection
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept4(listen_fd, (struct sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
                if (client_fd == -1) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("accept4");
                    }
                    continue;
                }

                std::cout << "Accepted connection from " << inet_ntoa(client_addr.sin_addr) << std::endl;

                // Add client to epoll
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
                    perror("epoll_ctl add client");
                    close(client_fd);
                    continue;
                }

            } else {
                // Handle client data
                int client_fd = events[i].data.fd;
                char buffer[1024];
                ssize_t n = recv(client_fd, buffer, sizeof(buffer), 0);
                if (n > 0) {
                    std::cout << "Received " << n << " bytes" << std::endl;

                    // Simple HTTP response
                    const char* response =
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/plain\r\n"
                        "Content-Length: 14\r\n"
                        "Connection: close\r\n"
                        "\r\n"
                        "Hello, World!\n";

                    send(client_fd, response, strlen(response), 0);
                    close(client_fd);
                } else if (n == 0 || (n == -1 && errno != EAGAIN)) {
                    std::cout << "Connection closed or error" << std::endl;
                    close(client_fd);
                }
            }
        }
    }

    close(epoll_fd);
    close(listen_fd);
    return 0;
}