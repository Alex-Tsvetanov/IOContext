#pragma once

namespace io
{
    enum class ConnectionState
    {
        Accepting,
        ReadingHeaders,
        ReadingBody,
        Writing,
        Closing,
    };
} // namespace io
