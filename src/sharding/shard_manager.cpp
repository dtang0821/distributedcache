#include "sharding/shard_manager.hpp"

#include <stdexcept>

namespace distributed_cache {

ShardManager::ShardManager(std::size_t virtualNodesPerShard)
    : ring_(virtualNodesPerShard)
{
}

void ShardManager::addNode(std::shared_ptr<ShardNode> node)
{
    if (!node || node->nodeId.empty()) {
        throw std::invalid_argument("shard node requires an id");
    }
    if (!node->cache) {
        throw std::invalid_argument("shard node requires a cache");
    }

    const auto nodeId = node->nodeId;
    nodes_[nodeId] = std::move(node);
    ring_.addNode(nodeId);
}

std::shared_ptr<ShardNode> ShardManager::removeNode(const std::string& nodeId)
{
    auto iterator = nodes_.find(nodeId);
    if (iterator == nodes_.end()) {
        return nullptr;
    }

    auto removed = iterator->second;
    nodes_.erase(iterator);
    ring_.removeNode(nodeId);
    return removed;
}

std::shared_ptr<ShardNode> ShardManager::nodeForKey(const Key& key) const
{
    const auto owner = ring_.ownerForKey(key);
    if (!owner.has_value()) {
        return nullptr;
    }
    return nodeById(owner.value());
}

std::shared_ptr<ShardNode> ShardManager::nodeById(const std::string& nodeId) const
{
    auto iterator = nodes_.find(nodeId);
    if (iterator == nodes_.end()) {
        return nullptr;
    }
    return iterator->second;
}

bool ShardManager::ownsKey(const std::string& nodeId, const Key& key) const
{
    const auto owner = ring_.ownerForKey(key);
    return owner.has_value() && owner.value() == nodeId;
}

ConsistentHashRing& ShardManager::ring()
{
    return ring_;
}

const ConsistentHashRing& ShardManager::ring() const
{
    return ring_;
}

std::vector<std::shared_ptr<ShardNode>> ShardManager::nodes() const
{
    std::vector<std::shared_ptr<ShardNode>> result;
    result.reserve(nodes_.size());
    for (const auto& pair : nodes_) {
        result.push_back(pair.second);
    }
    return result;
}

std::size_t ShardManager::nodeCount() const
{
    return nodes_.size();
}

} // namespace distributed_cache
