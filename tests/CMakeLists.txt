# Add unit tests for the ProcessingMachine
add_subdirectory(unit)

add_executable(iocontext_http_server server/hello_world_and_home_server.cpp)
target_link_libraries(iocontext_http_server PRIVATE iocontext)
