#include "cache/cachestore.hpp"
#include "persistence/persistence_manager.hpp"
#include "utils/timer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

std::size_t parseSizeArg(char* value, std::size_t fallback)
{
    if (value == nullptr) {
        return fallback;
    }
    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return fallback;
    }
    return static_cast<std::size_t>(parsed);
}

double percentileMicros(std::vector<std::chrono::nanoseconds>& latencies, double percentile)
{
    std::sort(latencies.begin(), latencies.end());
    const auto index = static_cast<std::size_t>((percentile / 100.0) * (latencies.size() - 1));
    return std::chrono::duration<double, std::micro>(latencies[index]).count();
}

} // namespace

int main(int argc, char** argv)
{
    const auto operationCount = parseSizeArg(argc > 1 ? argv[1] : nullptr, 50000);
    const auto capacity = parseSizeArg(argc > 2 ? argv[2] : nullptr, operationCount + 100);
    const auto dir = std::filesystem::current_path() / "build_manual" / "persistence_benchmark";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    distributed_cache::PersistenceConfig config;
    config.directory = dir.string();
    config.fsyncPolicy = distributed_cache::FsyncPolicy::Never;
    config.backgroundSnapshots = false;

    auto cache = std::make_shared<distributed_cache::CacheStore>(capacity);
    distributed_cache::PersistenceManager persistence(cache, config);

    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(operationCount);
    distributed_cache::Timer totalTimer;

    for (std::size_t i = 0; i < operationCount; ++i) {
        const std::string key = "key:" + std::to_string(i);
        const std::string value = "value:" + std::to_string(i);
        distributed_cache::Timer operationTimer;
        persistence.put(key, value);
        latencies.push_back(operationTimer.elapsedNanoseconds());
    }
    persistence.appendOnlyLog().flush();

    const double elapsedSeconds = totalTimer.elapsedSeconds();
    const double throughput = static_cast<double>(operationCount) / elapsedSeconds;

    distributed_cache::Timer snapshotTimer;
    persistence.createSnapshot();
    const auto snapshotDuration = snapshotTimer.elapsedNanoseconds();

    auto recoveredCache = std::make_shared<distributed_cache::CacheStore>(capacity);
    distributed_cache::PersistenceManager recovered(recoveredCache, config);
    distributed_cache::Timer recoveryTimer;
    const auto recoveryMetrics = recovered.recover();
    const auto recoveryDuration = recoveryTimer.elapsedNanoseconds();

    const double p50 = percentileMicros(latencies, 50.0);
    const double p99 = percentileMicros(latencies, 99.0);
    const auto metrics = persistence.metrics();

    std::cout << "Persistence benchmark summary" << '\n';
    std::cout << "-----------------------------" << '\n';
    std::cout << "Operations:        " << operationCount << '\n';
    std::cout << "Recovered entries: " << recoveredCache->size() << '\n';
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Throughput:        " << throughput << " writes/sec" << '\n';
    std::cout << "p50 latency:       " << p50 << " us" << '\n';
    std::cout << "p99 latency:       " << p99 << " us" << '\n';
    std::cout << "fsync latency:     "
              << std::chrono::duration<double, std::micro>(metrics.fsyncLatency).count() << " us" << '\n';
    std::cout << "replay duration:   "
              << std::chrono::duration<double, std::milli>(recoveryMetrics.lastReplayLatency).count()
              << " ms" << '\n';
    std::cout << "recovery startup:  "
              << std::chrono::duration<double, std::milli>(recoveryDuration).count() << " ms" << '\n';
    std::cout << "snapshot duration: "
              << std::chrono::duration<double, std::milli>(snapshotDuration).count() << " ms" << '\n';

    return 0;
}
