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
    src/net/client_session.cpp
    src/net/commandparser.cpp
    src/net/replication_server.cpp
    src/net/replication_session.cpp
    src/net/tcpserver.cpp
    src/persistence/append_only_log.cpp
    src/persistence/log_entry.cpp
    src/persistence/persistence_manager.cpp
    src/persistence/recovery_manager.cpp
    src/persistence/snapshot_manager.cpp
    src/replication/replication_log.cpp
    src/replication/replication_manager.cpp
    src/replication/replication_message.cpp
    src/replication/replication_peer.cpp
    src/sharding/consistent_hash_ring.cpp
    src/sharding/key_router.cpp
    src/sharding/rebalance_manager.cpp
    src/sharding/shard_manager.cpp
    src/utils/threadpool.cpp
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

target_link_libraries(distributed_cache PUBLIC Threads::Threads)

if(WIN32)
    target_link_libraries(distributed_cache PUBLIC ws2_32)
endif()

add_executable(distributed_cache_app src/main.cpp)
target_link_libraries(distributed_cache_app PRIVATE distributed_cache)

add_executable(cache_tests tests/CacheTests.cpp)
target_link_libraries(cache_tests PRIVATE distributed_cache)

add_executable(network_tests tests/NetworkTests.cpp)
target_link_libraries(network_tests PRIVATE distributed_cache)

add_executable(replication_tests tests/ReplicationTests.cpp)
target_link_libraries(replication_tests PRIVATE distributed_cache)

add_executable(sharding_tests tests/ShardingTests.cpp)
target_link_libraries(sharding_tests PRIVATE distributed_cache)

add_executable(persistence_tests tests/PersistenceTests.cpp)
target_link_libraries(persistence_tests PRIVATE distributed_cache)

add_executable(cache_benchmark benchmarks/CacheBenchmark.cpp)
target_link_libraries(cache_benchmark PRIVATE distributed_cache)

add_executable(replication_benchmark benchmarks/ReplicationBenchmark.cpp)
target_link_libraries(replication_benchmark PRIVATE distributed_cache)

add_executable(sharding_benchmark benchmarks/ShardingBenchmark.cpp)
target_link_libraries(sharding_benchmark PRIVATE distributed_cache)

add_executable(persistence_benchmark benchmarks/PersistenceBenchmark.cpp)
target_link_libraries(persistence_benchmark PRIVATE distributed_cache)

enable_testing()
add_test(NAME CacheTests COMMAND cache_tests)
add_test(NAME NetworkTests COMMAND network_tests)
add_test(NAME ReplicationTests COMMAND replication_tests)
add_test(NAME ShardingTests COMMAND sharding_tests)
add_test(NAME PersistenceTests COMMAND persistence_tests)
