#include "sharding/key_router.hpp"

#include <stdexcept>

namespace distributed_cache {

KeyRouter::KeyRouter(std::shared_ptr<ShardManager> shardManager)
    : shardManager_(std::move(shardManager))
{
    if (!shardManager_) {
        throw std::invalid_argument("key router requires shard manager");
    }
}

bool KeyRouter::put(const Key& key, const Value& value)
{
    auto node = shardManager_->nodeForKey(key);
    if (!node || !node->canAcceptWrites()) {
        return false;
    }

    const bool stored = node->cache->put(key, value);
    if (stored && node->replicationManager) {
        node->replicationManager->replicatePut(key, value);
    }
    return stored;
}

std::optional<Value> KeyRouter::get(const Key& key)
{
    auto node = shardManager_->nodeForKey(key);
    if (!node) {
        return std::nullopt;
    }
    return node->cache->get(key);
}

bool KeyRouter::remove(const Key& key)
{
    auto node = shardManager_->nodeForKey(key);
    if (!node || !node->canAcceptWrites()) {
        return false;
    }

    const bool removed = node->cache->remove(key);
    if (removed && node->replicationManager) {
        node->replicationManager->replicateDelete(key);
    }
    return removed;
}

bool KeyRouter::exists(const Key& key)
{
    return get(key).has_value();
}

std::optional<std::string> KeyRouter::ownerForKey(const Key& key) const
{
    return shardManager_->ring().ownerForKey(key);
}

} // namespace distributed_cache
