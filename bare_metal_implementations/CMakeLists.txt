if(WIN32)
    add_executable(hello_world windows/hello_world_http_windows.cpp)
    target_link_libraries(hello_world ws2_32)

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
    find_library(LIBURING_LIBRARY NAMES uring REQUIRED)
    if(LIBURING_LIBRARY)
        message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
    else()
        message(FATAL_ERROR "liburing not found. Please install liburing.")
    endif()

    add_executable(hello_world unix/hello_world_http_linux.cpp)
    target_link_libraries(hello_world PRIVATE ${LIBURING_LIBRARY})

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
