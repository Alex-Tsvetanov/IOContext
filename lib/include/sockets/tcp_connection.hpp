#pragma once

namespace io
{
    class TCPConnection
    {
    public:
        TCPConnection() = default;
        ~TCPConnection() = default;
        TCPConnection(const TCPConnection&) = delete;
        TCPConnection& operator=(const TCPConnection&) = delete;
        TCPConnection(TCPConnection&&) = default;
        TCPConnection& operator=(TCPConnection&&) = default;

    private:
        // Add private members and methods as needed for TCP connection handling
        // For example, socket file descriptor, connection state, etc.
        int socket_fd_{-1}; // Example member for socket file descriptor
        bool is_connected_{false}; // Example member for connection state
        // Additional members can be added here for managing the TCP connection

    };
} // namespace io
