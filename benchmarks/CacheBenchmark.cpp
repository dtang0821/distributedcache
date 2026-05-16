#include "cache/cachestore.hpp"
#include "utils/timer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

struct BenchmarkConfig {
    std::size_t cacheSize = 100000;
    std::size_t operationCount = 1000000;
    double putRatio = 0.40;
};

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

BenchmarkConfig parseConfig(int argc, char** argv)
{
    BenchmarkConfig config;
    if (argc > 1) {
        config.cacheSize = parseSizeArg(argv[1], config.cacheSize);
    }
    if (argc > 2) {
        config.operationCount = parseSizeArg(argv[2], config.operationCount);
    }
    return config;
}

double percentileMicros(std::vector<std::chrono::nanoseconds>& latencies, double percentile)
{
    if (latencies.empty()) {
        return 0.0;
    }

    std::sort(latencies.begin(), latencies.end());
    const auto index = static_cast<std::size_t>((percentile / 100.0) * (latencies.size() - 1));
    return std::chrono::duration<double, std::micro>(latencies[index]).count();
}

} // namespace

int main(int argc, char** argv)
{
    const auto config = parseConfig(argc, argv);

    distributed_cache::CacheStore cache(config.cacheSize);
    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(config.operationCount);

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<std::size_t> keyDistribution(0, config.cacheSize * 2 + 1);
    std::bernoulli_distribution operationDistribution(config.putRatio);

    distributed_cache::Timer totalTimer;

    for (std::size_t i = 0; i < config.operationCount; ++i) {
        const auto key = "key:" + std::to_string(keyDistribution(rng));
        const auto value = "value:" + std::to_string(i);

        distributed_cache::Timer operationTimer;
        if (operationDistribution(rng)) {
            cache.put(key, value);
        } else {
            (void)cache.get(key);
        }
        latencies.push_back(operationTimer.elapsedNanoseconds());
    }

    const double elapsedSeconds = totalTimer.elapsedSeconds();
    const double throughput = static_cast<double>(config.operationCount) / elapsedSeconds;
    const double p50 = percentileMicros(latencies, 50.0);
    const double p99 = percentileMicros(latencies, 99.0);

    std::cout << "Cache benchmark summary" << '\n';
    std::cout << "-----------------------" << '\n';
    std::cout << "Cache capacity:   " << config.cacheSize << '\n';
    std::cout << "Operations:       " << config.operationCount << '\n';
    std::cout << "Workload:         " << static_cast<int>(config.putRatio * 100)
              << "% PUT / " << static_cast<int>((1.0 - config.putRatio) * 100) << "% GET" << '\n';
    std::cout << "Final size:       " << cache.size() << '\n';
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Elapsed:          " << elapsedSeconds << " sec" << '\n';
    std::cout << "Throughput:       " << throughput << " ops/sec" << '\n';
    std::cout << "p50 latency:      " << p50 << " us" << '\n';
    std::cout << "p99 latency:      " << p99 << " us" << '\n';

    return 0;
}
