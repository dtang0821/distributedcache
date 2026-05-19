#include "net/commandparser.hpp"
#include "utils/threadpool.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>

using distributed_cache::CommandParser;
using distributed_cache::CommandType;
using distributed_cache::ThreadPool;

namespace {

void testPutParsing()
{
    CommandParser parser;
    const auto parsed = parser.parse("PUT greeting hello world\r");
    assert(parsed.ok);
    assert(parsed.command.type == CommandType::Put);
    assert(parsed.command.key == "greeting");
    assert(parsed.command.value == "hello world");
}

void testGetParsing()
{
    CommandParser parser;
    const auto parsed = parser.parse("get alpha");
    assert(parsed.ok);
    assert(parsed.command.type == CommandType::Get);
    assert(parsed.command.key == "alpha");
}

void testDeleteParsing()
{
    CommandParser parser;
    const auto parsed = parser.parse("DELETE alpha");
    assert(parsed.ok);
    assert(parsed.command.type == CommandType::Delete);
    assert(parsed.command.key == "alpha");
}

void testInvalidParsing()
{
    CommandParser parser;
    assert(!parser.parse("PUT only-key").ok);
    assert(!parser.parse("GET key extra").ok);
    assert(!parser.parse("UNKNOWN key").ok);
}

void testThreadPoolExecutesTasks()
{
    constexpr int taskCount = 128;

    ThreadPool pool(4);
    std::atomic<int> completed{0};
    std::mutex mutex;
    std::condition_variable condition;

    for (int i = 0; i < taskCount; ++i) {
        pool.submit([&] {
            const int value = completed.fetch_add(1) + 1;
            if (value == taskCount) {
                std::lock_guard<std::mutex> lock(mutex);
                condition.notify_one();
            }
        });
    }

    std::unique_lock<std::mutex> lock(mutex);
    condition.wait_for(lock, std::chrono::seconds(5), [&] {
        return completed.load() == taskCount;
    });

    assert(completed.load() == taskCount);
    pool.shutdown();
}

} // namespace

int main()
{
    testPutParsing();
    testGetParsing();
    testDeleteParsing();
    testInvalidParsing();
    testThreadPoolExecutesTasks();

    std::cout << "All network tests passed." << '\n';
    return 0;
}
