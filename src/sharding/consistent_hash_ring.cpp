#include "sharding/consistent_hash_ring.hpp"

#include <functional>
#include <stdexcept>

namespace distributed_cache {

ConsistentHashRing::ConsistentHashRing(std::size_t virtualNodesPerShard)
    : virtualNodesPerShard_(virtualNodesPerShard)
{
    if (virtualNodesPerShard_ == 0) {
        throw std::invalid_argument("virtual node count must be greater than zero");
    }
}

void ConsistentHashRing::addNode(const std::string& nodeId)
{
    if (nodeId.empty() || containsNode(nodeId)) {
        return;
    }

    auto& positions = nodePositions_[nodeId];
    positions.reserve(virtualNodesPerShard_);

    for (std::size_t i = 0; i < virtualNodesPerShard_; ++i) {
        std::uint64_t position = hashKey(nodeId + "-vnode-" + std::to_string(i));
        while (ring_.find(position) != ring_.end()) {
            ++position;
        }
        ring_[position] = nodeId;
        positions.push_back(position);
    }
}

void ConsistentHashRing::removeNode(const std::string& nodeId)
{
    auto iterator = nodePositions_.find(nodeId);
    if (iterator == nodePositions_.end()) {
        return;
    }

    for (const auto position : iterator->second) {
        ring_.erase(position);
    }
    nodePositions_.erase(iterator);
}

std::optional<std::string> ConsistentHashRing::ownerForKey(const Key& key) const
{
    return successorForPosition(hashKey(key));
}

std::optional<std::string> ConsistentHashRing::successorForPosition(std::uint64_t position) const
{
    if (ring_.empty()) {
        return std::nullopt;
    }

    auto iterator = ring_.lower_bound(position);
    if (iterator == ring_.end()) {
        iterator = ring_.begin();
    }
    return iterator->second;
}

bool ConsistentHashRing::containsNode(const std::string& nodeId) const
{
    return nodePositions_.find(nodeId) != nodePositions_.end();
}

std::size_t ConsistentHashRing::virtualNodesPerShard() const
{
    return virtualNodesPerShard_;
}

std::size_t ConsistentHashRing::ringSize() const
{
    return ring_.size();
}

std::size_t ConsistentHashRing::physicalNodeCount() const
{
    return nodePositions_.size();
}

std::uint64_t ConsistentHashRing::hashKey(const std::string& value) const
{
    return static_cast<std::uint64_t>(std::hash<std::string>{}(value));
}

std::vector<VirtualNode> ConsistentHashRing::virtualNodesFor(const std::string& nodeId) const
{
    std::vector<VirtualNode> result;
    auto iterator = nodePositions_.find(nodeId);
    if (iterator == nodePositions_.end()) {
        return result;
    }

    result.reserve(iterator->second.size());
    for (std::size_t i = 0; i < iterator->second.size(); ++i) {
        result.push_back({nodeId, i, iterator->second[i]});
    }
    return result;
}

std::vector<std::string> ConsistentHashRing::nodeIds() const
{
    std::vector<std::string> result;
    result.reserve(nodePositions_.size());
    for (const auto& pair : nodePositions_) {
        result.push_back(pair.first);
    }
    return result;
}

} // namespace distributed_cache
