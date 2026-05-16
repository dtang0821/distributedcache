#include "cache/cachestore.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using distributed_cache::CacheStore;

namespace {

void testBasicPutGet()
{
    CacheStore cache(10);
    assert(cache.put("key", "value"));
    assert(cache.get("key").value() == "value");
}

void testOverwriteExistingKey()
{
    CacheStore cache(10);
    assert(cache.put("key", "old"));
    assert(cache.put("key", "new"));
    assert(cache.get("key").value() == "new");
    assert(cache.size() == 1);
}

void testDelete()
{
    CacheStore cache(10);
    assert(cache.put("key", "value"));
    assert(cache.remove("key"));
    assert(!cache.get("key").has_value());
    assert(!cache.remove("key"));
}

void testTtlExpiration()
{
    using namespace std::chrono_literals;

    CacheStore cache(10);
    assert(cache.put("short", "lived", 25ms));
    assert(cache.get("short").has_value());

    std::this_thread::sleep_for(60ms);
    assert(!cache.get("short").has_value());
    assert(cache.size() == 0);
}

void testLruEviction()
{
    CacheStore cache(2);
    assert(cache.put("a", "1"));
    assert(cache.put("b", "2"));
    assert(cache.get("a").value() == "1");
    assert(cache.put("c", "3"));

    assert(cache.get("a").value() == "1");
    assert(!cache.get("b").has_value());
    assert(cache.get("c").value() == "3");
}

void testConcurrentAccessCorrectness()
{
    constexpr int threadCount = 8;
    constexpr int operationsPerThread = 500;

    CacheStore cache(threadCount * operationsPerThread);
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (int threadId = 0; threadId < threadCount; ++threadId) {
        threads.emplace_back([threadId, &cache] {
            for (int i = 0; i < operationsPerThread; ++i) {
                const std::string key = "t" + std::to_string(threadId) + ":" + std::to_string(i);
                const std::string value = std::to_string(i);
                assert(cache.put(key, value));
                const auto fetched = cache.get(key);
                assert(fetched.has_value());
                assert(fetched.value() == value);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    assert(cache.size() == threadCount * operationsPerThread);
}

void testCapacityEnforcement()
{
    CacheStore cache(3);
    assert(cache.put("a", "1"));
    assert(cache.put("b", "2"));
    assert(cache.put("c", "3"));
    assert(cache.put("d", "4"));

    assert(cache.size() == 3);
    assert(!cache.get("a").has_value());
}

void testExpiredEntryCleanupOnPut()
{
    using namespace std::chrono_literals;

    CacheStore cache(10);
    assert(cache.put("expired", "value", 20ms));
    std::this_thread::sleep_for(50ms);

    assert(cache.put("fresh", "value"));
    assert(cache.size() == 1);
    assert(!cache.exists("expired"));
    assert(cache.exists("fresh"));
}

void testZeroCapacity()
{
    CacheStore cache(0);
    assert(!cache.put("key", "value"));
    assert(!cache.get("key").has_value());
    assert(cache.size() == 0);
}

} // namespace

int main()
{
    testBasicPutGet();
    testOverwriteExistingKey();
    testDelete();
    testTtlExpiration();
    testLruEviction();
    testConcurrentAccessCorrectness();
    testCapacityEnforcement();
    testExpiredEntryCleanupOnPut();
    testZeroCapacity();

    std::cout << "All cache tests passed." << '\n';
    return 0;
}
