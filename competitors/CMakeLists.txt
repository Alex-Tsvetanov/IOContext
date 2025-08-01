# Add the integration test executable

find_package(Boost REQUIRED COMPONENTS system)
find_package(Threads REQUIRED)
cmake_policy(SET CMP0079 NEW)

add_executable(hello_world_boost_1.1 hello_world_boost.cpp)
target_link_libraries(hello_world_boost_1.1 PRIVATE Boost::system Threads::Threads)

add_executable(hello_world_boost_1.0 hello_world_boost_nokeepalive.cpp)
target_link_libraries(hello_world_boost_1.0 PRIVATE Boost::system Threads::Threads)
