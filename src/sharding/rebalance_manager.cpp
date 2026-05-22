#include "sharding/rebalance_manager.hpp"

#include <stdexcept>

namespace distributed_cache {

RebalanceManager::RebalanceManager(std::shared_ptr<ShardManager> shardManager)
    : shardManager_(std::move(shardManager))
{
    if (!shardManager_) {
        throw std::invalid_argument("rebalance manager requires shard manager");
    }
}

RebalanceStats RebalanceManager::rebalanceAll()
{
    RebalanceStats stats;
    const auto nodes = shardManager_->nodes();
    for (const auto& node : nodes) {
        const auto nodeStats = rebalanceNode(node->nodeId);
        stats.keysMoved += nodeStats.keysMoved;
        stats.keysScanned += nodeStats.keysScanned;
    }
    return stats;
}

RebalanceStats RebalanceManager::rebalanceNode(const std::string& nodeId)
{
    RebalanceStats stats;
    auto source = shardManager_->nodeById(nodeId);
    if (!source) {
        return stats;
    }

    const auto entries = source->cache->entriesSnapshot();
    for (const auto& entry : entries) {
        ++stats.keysScanned;
        auto owner = shardManager_->nodeForKey(entry.first);
        if (!owner || owner->nodeId == source->nodeId) {
            continue;
        }

        if (owner->cache->put(entry.first, entry.second)) {
            source->cache->remove(entry.first);
            if (owner->replicationManager) {
                owner->replicationManager->replicatePut(entry.first, entry.second);
            }
            if (source->replicationManager) {
                source->replicationManager->replicateDelete(entry.first);
            }
            ++stats.keysMoved;
        }
    }

    return stats;
}

RebalanceStats RebalanceManager::rebalanceRemovedNode(const std::shared_ptr<ShardNode>& removedNode)
{
    RebalanceStats stats;
    if (!removedNode) {
        return stats;
    }

    const auto entries = removedNode->cache->entriesSnapshot();
    for (const auto& entry : entries) {
        ++stats.keysScanned;
        auto owner = shardManager_->nodeForKey(entry.first);
        if (!owner) {
            continue;
        }

        if (owner->cache->put(entry.first, entry.second)) {
            removedNode->cache->remove(entry.first);
            if (owner->replicationManager) {
                owner->replicationManager->replicatePut(entry.first, entry.second);
            }
            ++stats.keysMoved;
        }
    }
    return stats;
}

} // namespace distributed_cache
