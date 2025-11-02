cmake_minimum_required(VERSION 3.10)

set(PROJECT_N iocontext)
project(${PROJECT_N} VERSION 1.0)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

if(MSVC)
    add_compile_options(/W4 /permissive- /Zc:__cplusplus /Zc:preprocessor /Zc:forScope)
else()
    message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        message(STATUS "Compiling in Release mode")
        add_compile_options(-Wall -Wextra -pedantic -O3 -march=native)
    else()
        message(STATUS "Compiling in Debug mode")
        add_compile_options(-Wall -Wextra -pedantic -O0)
    endif()
endif()

# Set up OpenSSL
find_package(OpenSSL REQUIRED)
# Set up nghttp2
set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ENABLE_HPACK_TOOLS OFF CACHE BOOL "" FORCE)
set(ENABLE_APP OFF CACHE BOOL "" FORCE)
set(ENABLE_STATIC_LIB ON CACHE BOOL "" FORCE)
set(ENABLE_SHARED_LIB OFF CACHE BOOL "" FORCE)
add_subdirectory(external/nghttp2)

add_subdirectory(lib)

add_subdirectory(${CMAKE_SOURCE_DIR}/external/catch2)
add_subdirectory(tests)

add_subdirectory(competitors)

add_subdirectory(playground)

# add_subdirectory(bare_metal_implementations)
