add_library(owned_buffer STATIC src/common/owned_buf.cpp)
target_include_directories(owned_buffer PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

find_package(Boost REQUIRED COMPONENTS system)

add_library(processing_machine STATIC src/common/processing_machine.cpp)
target_include_directories(processing_machine PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
# add_compile_definitions(DEBUG)
add_library(iocontext STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/common/per_client_storage.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/common/socket.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/server/linux/worker.cpp
)

target_link_libraries(iocontext PUBLIC processing_machine)

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
elseif(APPLE)
else() # Linux
  find_library(LIBURING_LIBRARY NAMES uring REQUIRED)
  if(LIBURING_LIBRARY)
    message(STATUS "Found liburing: ${LIBURING_LIBRARY}")
  else()
    message(FATAL_ERROR "liburing not found. Please install liburing.")
  endif()
  target_link_libraries(iocontext PRIVATE ${LIBURING_LIBRARY} Boost::system)
endif()
