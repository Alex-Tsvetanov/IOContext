add_executable(playground main.cpp)
target_link_libraries(playground PRIVATE iocontext)
target_include_directories(playground PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../lib/include
)
