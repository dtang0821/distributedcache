#pragma once

#include "common/types.hpp"
#include "sharding/consistent_hash_ring.hpp"
#include "sharding/shard_node.hpp"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace distributed_cache {

class ShardManager {
public:
    explicit ShardManager(std::size_t virtualNodesPerShard = 64);

    void addNode(std::shared_ptr<ShardNode> node);
    std::shared_ptr<ShardNode> removeNode(const std::string& nodeId);
    std::shared_ptr<ShardNode> nodeForKey(const Key& key) const;
    std::shared_ptr<ShardNode> nodeById(const std::string& nodeId) const;
    bool ownsKey(const std::string& nodeId, const Key& key) const;

    ConsistentHashRing& ring();
    const ConsistentHashRing& ring() const;
    std::vector<std::shared_ptr<ShardNode>> nodes() const;
    std::size_t nodeCount() const;

private:
    ConsistentHashRing ring_;
    std::unordered_map<std::string, std::shared_ptr<ShardNode>> nodes_;
};

} // namespace distributed_cache
