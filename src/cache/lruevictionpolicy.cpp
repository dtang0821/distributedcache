#include "cache/lruevictionpolicy.hpp"

namespace distributed_cache {

LruIterator LruEvictionPolicy::insert(const Key& key)
{
    usageOrder_.push_front(key);
    return usageOrder_.begin();
}

void LruEvictionPolicy::touch(LruIterator iterator)
{
    usageOrder_.splice(usageOrder_.begin(), usageOrder_, iterator);
}

void LruEvictionPolicy::erase(LruIterator iterator)
{
    usageOrder_.erase(iterator);
}

std::optional<Key> LruEvictionPolicy::victim() const
{
    if (usageOrder_.empty()) {
        return std::nullopt;
    }
    return usageOrder_.back();
}

void LruEvictionPolicy::clear()
{
    usageOrder_.clear();
}

std::size_t LruEvictionPolicy::size() const
{
    return usageOrder_.size();
}

} // namespace distributed_cache
