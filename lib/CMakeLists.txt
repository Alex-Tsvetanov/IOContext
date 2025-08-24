add_library(owned_buffer STATIC src/common/owned_buf.cpp)
target_include_directories(owned_buffer PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)


add_library(iocontext src/common/owned_buf.cpp)
# target_link_libraries(iocontext PUBLIC owned_buffer)

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
