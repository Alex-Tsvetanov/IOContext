#add_library(iocontext)

set(XHTTP_COMMON
  src/server.cpp
  src/timer_heap.cpp
  src/buffer.cpp
  src/socket_utils.cpp
)

if(WIN32)
  list(APPEND XHTTP_COMMON src/backends/poller_iocp.cpp)
  add_definitions(-DXHTTP_PLATFORM_WINDOWS)
elseif(APPLE)
  list(APPEND XHTTP_COMMON src/backends/poller_kqueue.cpp)
  add_definitions(-DXHTTP_PLATFORM_MACOS)
else()
  list(APPEND XHTTP_COMMON src/backends/poller_epoll.cpp)
  add_definitions(-DXHTTP_PLATFORM_LINUX)
endif()

add_library(xhttp STATIC ${XHTTP_COMMON})
target_include_directories(xhttp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

if(WIN32)
  target_link_libraries(xhttp PUBLIC ws2_32 mswsock)
endif()
