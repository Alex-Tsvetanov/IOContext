add_library(iocontext
    src/io_context.cpp
    src/epoll_poller.cpp
    src/net.cpp
)

target_include_directories(iocontext PUBLIC
    include
)

if(WIN32)
    target_link_libraries(iocontext ws2_32)
endif()
