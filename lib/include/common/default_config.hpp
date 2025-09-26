#ifndef DEFAULT_CONFIG_H
#define DEFAULT_CONFIG_H

#include <chrono>
#include <sys/socket.h>
// static constexpr int RX_CAP = 8192;
// static constexpr int MAX_IOV = 8;
static constexpr int DEFAULT_PORT = 8080;
static constexpr int ACCEPTS_PER_WORKER = 1;

// static constexpr int PROC_MAX_SEGMENTS = 32;
static constexpr std::chrono::microseconds PROC_TIME_BUDGET(200);
static constexpr int BACKLOG = SOMAXCONN;

#endif // DEFAULT_CONFIG_H