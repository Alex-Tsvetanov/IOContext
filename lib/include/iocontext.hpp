// Main IOContext library header
// This includes all platform-independent components

#ifndef IOCONTEXT_H
#define IOCONTEXT_H

// Platform-independent headers
#include "http.hpp"
#include "connection_handle.hpp"
#include "request_context.hpp"
#include "server_config.hpp"
#include "server.hpp"

// Platform-specific includes
#ifdef _WIN32
#include "iocp/iocp_server.hpp"
#include "iocp/iocp_worker.hpp"
#include "iocp/winsock_wrapper.hpp"
#include "iocp/iocp_helpers.hpp"
#else
// Future EPOLL includes will go here
#endif

#endif // IOCONTEXT_H