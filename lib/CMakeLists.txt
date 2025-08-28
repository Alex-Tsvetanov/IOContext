add_library(owned_buffer STATIC src/common/owned_buf.cpp)
target_include_directories(owned_buffer PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(processing_machine STATIC src/common/processing_machine.cpp)
target_include_directories(processing_machine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(WIN32)
  add_library(iocontext
    src/common/per_client_storage.cpp
    src/common/socket.cpp
    src/server/windows/worker.cpp
  )
elseif(APPLE)
else() # Linux
  find_library(LIBURING_LIBRARY NAMES uring REQUIRED)
  if(LIBURING_LIBRARY)
    message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
  else()
    message(FATAL_ERROR "liburing not found. Please install liburing.")
  endif()

  add_library(iocontext
    src/common/per_client_storage.cpp
    src/common/socket.cpp
    src/server/linux/worker.cpp
  )
  target_link_libraries(iocontext PRIVATE ${LIBURING_LIBRARY})
endif()

target_link_libraries(iocontext PUBLIC owned_buffer processing_machine)

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
