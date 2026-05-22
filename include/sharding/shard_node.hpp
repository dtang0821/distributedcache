#pragma once

#include "cache/cachestore.hpp"
#include "replication/replication_manager.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace distributed_cache {

struct ShardNode {
    std::string nodeId;
    std::string host = "127.0.0.1";
    std::uint16_t clientPort = 0;
    std::uint16_t replicationPort = 0;
    bool leader = true;
    std::shared_ptr<CacheStore> cache;
    std::shared_ptr<ReplicationManager> replicationManager;

    bool canAcceptWrites() const
    {
        return leader;
    }
};

} // namespace distributed_cache
