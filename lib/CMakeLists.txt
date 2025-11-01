add_library(owned_buffer STATIC src/common/owned_buf.cpp)
target_include_directories(owned_buffer PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(processing_machine STATIC src/common/processing_machine.cpp)
target_include_directories(processing_machine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(per_client_storage STATIC src/common/per_client_storage.cpp)
target_include_directories(per_client_storage PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(socket STATIC src/common/socket.cpp)
target_include_directories(socket PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

add_library(iocontext STATIC)

target_link_libraries(iocontext PUBLIC processing_machine per_client_storage socket Threads::Threads) # Boost::system)

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
  target_link_libraries(iocontext PRIVATE
    Ws2_32.lib
    Mswsock.lib
  )
  target_sources(iocontext PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/server/win32/worker.cpp)
elseif(APPLE)
  target_sources(iocontext PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/server/macos/worker.cpp)
else() # Linux
  find_library(LIBURING_LIBRARY NAMES uring REQUIRED)
  if(LIBURING_LIBRARY)
    message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
  else()
    message(FATAL_ERROR "liburing not found. Please install liburing.")
  endif()
  target_sources(iocontext PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/server/linux/worker.cpp)
  target_link_libraries(iocontext PRIVATE ${LIBURING_LIBRARY})
endif()
