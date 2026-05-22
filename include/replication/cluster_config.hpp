#pragma once

#include <cstdint>
#include <chrono>
#include <string>
#include <vector>

namespace distributed_cache {

enum class NodeRole {
    Leader,
    Follower
};

struct PeerEndpoint {
    std::string host;
    std::uint16_t port = 0;
};

struct ClusterConfig {
    NodeRole role = NodeRole::Leader;
    std::string host = "0.0.0.0";
    std::uint16_t clientPort = 6379;
    std::uint16_t replicationPort = 7379;
    std::size_t cacheCapacity = 100000;
    std::size_t clientWorkers = 4;
    std::size_t replicationWorkers = 2;
    bool persistenceEnabled = true;
    std::string persistenceDirectory = "data";
    std::string fsyncPolicy = "every-second";
    std::chrono::seconds snapshotInterval{30};
    PeerEndpoint leader;
    std::vector<PeerEndpoint> followers;
};

} // namespace distributed_cache
