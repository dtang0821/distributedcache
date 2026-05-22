#include "cache/cachestore.hpp"
#include "sharding/key_router.hpp"
#include "sharding/rebalance_manager.hpp"
#include "sharding/shard_manager.hpp"
#include "utils/timer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
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

std::shared_ptr<distributed_cache::ShardNode> makeShard(const std::string& nodeId,
                                                        std::size_t capacity)
{
    auto node = std::make_shared<distributed_cache::ShardNode>();
    node->nodeId = nodeId;
    node->leader = true;
    node->cache = std::make_shared<distributed_cache::CacheStore>(capacity);
    return node;
}

} // namespace

int main(int argc, char** argv)
{
    const auto operationCount = parseSizeArg(argc > 1 ? argv[1] : nullptr, 100000);
    const auto shardCount = parseSizeArg(argc > 2 ? argv[2] : nullptr, 4);
    const auto virtualNodes = parseSizeArg(argc > 3 ? argv[3] : nullptr, 64);
    const auto capacity = operationCount + 1000;

    auto manager = std::make_shared<distributed_cache::ShardManager>(virtualNodes);
    for (std::size_t i = 0; i < shardCount; ++i) {
        manager->addNode(makeShard("Shard" + std::to_string(i), capacity));
    }
    distributed_cache::KeyRouter router(manager);

    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(operationCount);
    distributed_cache::Timer totalTimer;

    for (std::size_t i = 0; i < operationCount; ++i) {
        const std::string key = "key:" + std::to_string(i);
        const std::string value = "value:" + std::to_string(i);

        distributed_cache::Timer operationTimer;
        router.put(key, value);
        latencies.push_back(operationTimer.elapsedNanoseconds());
    }

    const double elapsedSeconds = totalTimer.elapsedSeconds();
    const double throughput = static_cast<double>(operationCount) / elapsedSeconds;
    const double p50 = percentileMicros(latencies, 50.0);
    const double p99 = percentileMicros(latencies, 99.0);

    std::unordered_map<std::string, std::size_t> distribution;
    for (const auto& node : manager->nodes()) {
        distribution[node->nodeId] = node->cache->size();
    }

    manager->addNode(makeShard("ShardNew", capacity));
    distributed_cache::RebalanceManager rebalancer(manager);
    distributed_cache::Timer rebalanceTimer;
    const auto rebalanceStats = rebalancer.rebalanceAll();
    const double rebalanceSeconds = rebalanceTimer.elapsedSeconds();

    std::size_t minKeys = operationCount;
    std::size_t maxKeys = 0;
    for (const auto& node : manager->nodes()) {
        const auto size = node->cache->size();
        minKeys = std::min(minKeys, size);
        maxKeys = std::max(maxKeys, size);
    }

    std::cout << "Sharding benchmark summary" << '\n';
    std::cout << "--------------------------" << '\n';
    std::cout << "Operations:        " << operationCount << '\n';
    std::cout << "Initial shards:    " << shardCount << '\n';
    std::cout << "Virtual nodes:     " << virtualNodes << '\n';
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Routing throughput:" << throughput << " writes/sec" << '\n';
    std::cout << "p50 latency:       " << p50 << " us" << '\n';
    std::cout << "p99 latency:       " << p99 << " us" << '\n';
    std::cout << "Rebalance moved:   " << rebalanceStats.keysMoved << " keys" << '\n';
    std::cout << "Rebalance speed:   "
              << (rebalanceSeconds > 0.0 ? rebalanceStats.keysMoved / rebalanceSeconds : 0.0)
              << " keys/sec" << '\n';
    std::cout << "Distribution min:  " << minKeys << '\n';
    std::cout << "Distribution max:  " << maxKeys << '\n';

    return 0;
}
