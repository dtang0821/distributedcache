#pragma once

#include "cache/cacheentry.hpp"
#include "cache/lruevictionpolicy.hpp"
#include "common/types.hpp"

#include <mutex>
#include <optional>
#include <unordered_map>

namespace distributed_cache {

class CacheStore {
public:
    explicit CacheStore(std::size_t maxCapacity);

    bool put(const Key& key, const Value& value, std::optional<Duration> ttl = std::nullopt);
    std::optional<Value> get(const Key& key);
    bool remove(const Key& key);
    bool exists(const Key& key);
    std::size_t size();
    std::size_t capacity() const;
    void clear();

private:
    using EntryMap = std::unordered_map<Key, CacheEntry>;

    void removeEntry(EntryMap::iterator iterator);
    void cleanupExpiredEntries();
    void enforceCapacity();

    const std::size_t maxCapacity_;
    EntryMap entries_;
    LruEvictionPolicy evictionPolicy_;
    std::size_t expiringEntryCount_ = 0;
    mutable std::mutex mutex_;
};

} // namespace distributed_cache
