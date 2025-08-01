# Add the integration test executable
if(WIN32)
    add_executable(hello_world windows/hello_world_http_chatgpt.cpp)
    target_link_libraries(hello_world ws2_32)
elseif(UNIX)
    add_executable(hello_world unix/hello_world_http.cpp)
    
    find_package(OpenSSL REQUIRED)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(NGHTTP2 REQUIRED libnghttp2)
    add_executable(hello_world_http2 unix/http2/hello_world_http2.cpp)
    target_include_directories(hello_world_http2 PRIVATE
        ${NGHTTP2_INCLUDE_DIRS}
    )

    target_link_libraries(hello_world_http2 PRIVATE
        ${NGHTTP2_LIBRARIES}
        OpenSSL::SSL
        OpenSSL::Crypto
    )
elseif(APPLE)
    add_executable(hello_world MACOSX_BUNDLE/hello_world_http.cpp)
endif()