#include "cache/cachestore.hpp"
#include "replication/replication_manager.hpp"
#include "replication/replication_peer.hpp"
#include "utils/timer.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

class LoopbackPeer final : public distributed_cache::ReplicationPeer {
public:
    explicit LoopbackPeer(std::shared_ptr<distributed_cache::ReplicationManager> target)
        : ReplicationPeer("loopback", 0),
          target_(std::move(target))
    {
    }

    bool send(const distributed_cache::ReplicationMessage& message) override
    {
        target_->applyReplicaMessage(message);
        markAcked(message.sequence);
        return true;
    }

private:
    std::shared_ptr<distributed_cache::ReplicationManager> target_;
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

double percentileMicros(std::vector<std::chrono::nanoseconds>& latencies, double percentile)
{
    std::sort(latencies.begin(), latencies.end());
    const auto index = static_cast<std::size_t>((percentile / 100.0) * (latencies.size() - 1));
    return std::chrono::duration<double, std::micro>(latencies[index]).count();
}

} // namespace

int main(int argc, char** argv)
{
    const auto operationCount = parseSizeArg(argc > 1 ? argv[1] : nullptr, 100000);
    const auto followerCount = parseSizeArg(argc > 2 ? argv[2] : nullptr, 2);
    const auto capacity = parseSizeArg(argc > 3 ? argv[3] : nullptr, operationCount + 10);

    auto leaderCache = std::make_shared<distributed_cache::CacheStore>(capacity);
    auto leader = std::make_shared<distributed_cache::ReplicationManager>(
        distributed_cache::NodeRole::Leader, leaderCache, 4);

    std::vector<std::shared_ptr<distributed_cache::CacheStore>> followerCaches;
    std::vector<std::shared_ptr<distributed_cache::ReplicationManager>> followers;
    for (std::size_t i = 0; i < followerCount; ++i) {
        auto cache = std::make_shared<distributed_cache::CacheStore>(capacity);
        auto follower = std::make_shared<distributed_cache::ReplicationManager>(
            distributed_cache::NodeRole::Follower, cache, 1);
        followerCaches.push_back(cache);
        followers.push_back(follower);
        leader->addPeer(std::make_shared<LoopbackPeer>(follower));
    }

    std::vector<std::chrono::nanoseconds> latencies;
    latencies.reserve(operationCount);

    distributed_cache::Timer totalTimer;
    for (std::size_t i = 0; i < operationCount; ++i) {
        const std::string key = "key:" + std::to_string(i);
        const std::string value = "value:" + std::to_string(i);

        distributed_cache::Timer operationTimer;
        leaderCache->put(key, value);
        leader->replicatePut(key, value);
        latencies.push_back(operationTimer.elapsedNanoseconds());
    }

    while (leader->lastSequence() != 0) {
        bool caughtUp = true;
        for (const auto& follower : followers) {
            caughtUp = caughtUp && follower->lastSequence() == leader->lastSequence();
        }
        if (caughtUp) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const double elapsedSeconds = totalTimer.elapsedSeconds();
    const double throughput = static_cast<double>(operationCount) / elapsedSeconds;
    const double p50 = percentileMicros(latencies, 50.0);
    const double p99 = percentileMicros(latencies, 99.0);

    std::size_t caughtUpFollowers = 0;
    for (const auto& follower : followers) {
        if (follower->lastSequence() == leader->lastSequence()) {
            ++caughtUpFollowers;
        }
    }

    std::cout << "Replication benchmark summary" << '\n';
    std::cout << "-----------------------------" << '\n';
    std::cout << "Operations:        " << operationCount << '\n';
    std::cout << "Followers:         " << followerCount << '\n';
    std::cout << "Caught up:         " << caughtUpFollowers << "/" << followerCount << '\n';
    std::cout << "Leader sequence:   " << leader->lastSequence() << '\n';
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Throughput:        " << throughput << " replicated writes/sec" << '\n';
    std::cout << "p50 latency:       " << p50 << " us" << '\n';
    std::cout << "p99 latency:       " << p99 << " us" << '\n';
    std::cout << "Replication lag:   " << (followerCount - caughtUpFollowers) << " followers behind" << '\n';
    std::cout << "Catch-up speed:    " << throughput << " entries/sec in loopback mode" << '\n';

    return 0;
}
