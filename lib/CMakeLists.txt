# IOContext Library CMakeLists.txt

# Create the main library
add_library(iocontext STATIC
  # Platform-independent sources
  src/request_context.cpp
  src/server_config.cpp

  # Windows-specific sources
  src/iocp_connection_table.cpp
  src/iocp_client_storage.cpp
  src/iocp_helpers.cpp
  src/winsock_wrapper.cpp
  src/iocp_server.cpp
  src/iocp_worker.cpp
)

# Set include directories
target_include_directories(iocontext PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Platform-specific configuration
if(WIN32)
  target_include_directories(iocontext PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/iocp
  )
  target_compile_definitions(iocontext PRIVATE
    _WIN32_WINNT=0x0600
    WIN32_LEAN_AND_MEAN
  )
endif()

# Link libraries
if(WIN32)
  target_link_libraries(iocontext PRIVATE
    Ws2_32.lib
    Mswsock.lib
  )
endif()
