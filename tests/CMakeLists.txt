# Add the integration test executable
if(WIN32)
    add_executable(hello_world windows/hello_world_http_chatgpt.cpp)
    target_link_libraries(hello_world ws2_32)
elseif(UNIX)
    add_executable(hello_world unix/hello_world_http.cpp)
    
    add_executable(hello_world_http2 unix/http2/hello_world_http2.cpp)
    target_link_libraries(hello_world_http2 PRIVATE
        nghttp2::nghttp2
        OpenSSL::SSL
        OpenSSL::Crypto
    )
elseif(APPLE)
    add_executable(hello_world MACOSX_BUNDLE/hello_world_http.cpp)
endif()