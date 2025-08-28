# Add unit tests for the ProcessingMachine
add_subdirectory(unit)

# Add the integration test executable
add_executable(hello_world_http_cross MARSHMELLOW.cpp)

if(WIN32)
    add_executable(hello_world windows/initial_work_on_drawio_scheme.cpp) #hello_world_http_windows.cpp)
    target_link_libraries(hello_world ws2_32)
    target_link_libraries(hello_world_http_cross ws2_32)

    # Add the refactored IOCP server example
    add_executable(iocp_server_example iocp_server_example.cpp)
    target_link_libraries(iocp_server_example PRIVATE iocontext)

    add_executable(hello_world_http2 windows/hello_world_http2.cpp)
    target_link_libraries(hello_world_http2 PRIVATE
        nghttp2::nghttp2
        OpenSSL::SSL
        OpenSSL::Crypto
        ws2_32
    )
elseif(APPLE)
    add_executable(hello_world macos/hello_world_http_macos.cpp)

    add_executable(hello_world_http2 macos/http2/hello_world_http2.cpp)
    target_link_libraries(hello_world_http2 PRIVATE
        nghttp2::nghttp2
        OpenSSL::SSL
        OpenSSL::Crypto
    )
else()
    add_executable(iocontext_http_server unix/hello_world_iocontext_linux.cpp)
    target_link_libraries(iocontext_http_server PRIVATE iocontext)

    add_executable(hello_world unix/hello_world_http_linux.cpp)

    add_executable(hello_world_http2 unix/http2/hello_world_http2.cpp)
    target_link_libraries(hello_world_http2 PRIVATE
        nghttp2::nghttp2
        OpenSSL::SSL
        OpenSSL::Crypto
    )
endif()

if(UNIX)
    add_executable(hello_world_http_unix poll/hello_world_http.cpp)

    # add_executable(hello_world_http2_unix poll/http2/hello_world_http2.cpp)
    # target_link_libraries(hello_world_http2_unix PRIVATE
    #     nghttp2::nghttp2
    #     OpenSSL::SSL
    #     OpenSSL::Crypto
    # )
endif()
