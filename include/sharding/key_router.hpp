#pragma once

#include "common/types.hpp"
#include "sharding/shard_manager.hpp"

#include <memory>
#include <optional>
#include <string>

namespace distributed_cache {

class KeyRouter {
public:
    explicit KeyRouter(std::shared_ptr<ShardManager> shardManager);

    bool put(const Key& key, const Value& value);
    std::optional<Value> get(const Key& key);
    bool remove(const Key& key);
    bool exists(const Key& key);
    std::optional<std::string> ownerForKey(const Key& key) const;

private:
    std::shared_ptr<ShardManager> shardManager_;
};

} // namespace distributed_cache
