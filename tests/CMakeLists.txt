# Add the integration test executable
if(WIN32)
    add_executable(hello_world windows/hello_world_http_chatgpt.cpp)
    target_link_libraries(hello_world ws2_32)
elseif(UNIX)
    add_executable(hello_world unix/hello_world_http.cpp)
elseif(APPLE)
    add_executable(hello_world MACOSX_BUNDLE/hello_world_http.cpp)
endif()