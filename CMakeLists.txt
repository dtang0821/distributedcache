cmake_minimum_required(VERSION 3.14)

project(distributed_cache VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(distributed_cache
    src/cache/cachestore.cpp
    src/cache/lruevictionpolicy.cpp
    src/cache/ttlmanager.cpp
)

target_include_directories(distributed_cache
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(distributed_cache PRIVATE /W4 /permissive-)
else()
    target_compile_options(distributed_cache PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(distributed_cache_app src/main.cpp)
target_link_libraries(distributed_cache_app PRIVATE distributed_cache)

add_executable(cache_tests tests/CacheTests.cpp)
target_link_libraries(cache_tests PRIVATE distributed_cache Threads::Threads)

add_executable(cache_benchmark benchmarks/CacheBenchmark.cpp)
target_link_libraries(cache_benchmark PRIVATE distributed_cache)

enable_testing()
add_test(NAME CacheTests COMMAND cache_tests)
