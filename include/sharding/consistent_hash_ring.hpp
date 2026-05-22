#pragma once

#include "common/types.hpp"
#include "sharding/virtual_node.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace distributed_cache {

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(std::size_t virtualNodesPerShard = 64);

    void addNode(const std::string& nodeId);
    void removeNode(const std::string& nodeId);
    std::optional<std::string> ownerForKey(const Key& key) const;
    std::optional<std::string> successorForPosition(std::uint64_t position) const;

    bool containsNode(const std::string& nodeId) const;
    std::size_t virtualNodesPerShard() const;
    std::size_t ringSize() const;
    std::size_t physicalNodeCount() const;
    std::uint64_t hashKey(const std::string& value) const;
    std::vector<VirtualNode> virtualNodesFor(const std::string& nodeId) const;
    std::vector<std::string> nodeIds() const;

private:
    std::size_t virtualNodesPerShard_;
    std::map<std::uint64_t, std::string> ring_;
    std::unordered_map<std::string, std::vector<std::uint64_t>> nodePositions_;
};

} // namespace distributed_cache
