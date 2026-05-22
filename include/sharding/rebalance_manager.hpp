#pragma once

#include "sharding/shard_manager.hpp"

#include <memory>
#include <string>

namespace distributed_cache {

struct RebalanceStats {
    std::size_t keysMoved = 0;
    std::size_t keysScanned = 0;
};

class RebalanceManager {
public:
    explicit RebalanceManager(std::shared_ptr<ShardManager> shardManager);

    RebalanceStats rebalanceAll();
    RebalanceStats rebalanceNode(const std::string& nodeId);
    RebalanceStats rebalanceRemovedNode(const std::shared_ptr<ShardNode>& removedNode);

private:
    std::shared_ptr<ShardManager> shardManager_;
};

} // namespace distributed_cache
