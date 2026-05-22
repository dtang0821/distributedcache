#include "cache/cachestore.hpp"

#include "cache/ttlmanager.hpp"

#include <vector>

namespace distributed_cache {

CacheStore::CacheStore(std::size_t maxCapacity)
    : maxCapacity_(maxCapacity)
{
}

bool CacheStore::put(const Key& key, const Value& value, std::optional<Duration> ttl)
{
    std::lock_guard<std::mutex> lock(mutex_);
    cleanupExpiredEntries();

    if (maxCapacity_ == 0) {
        return false;
    }

    const auto expiration = TtlManager::expirationFromNow(ttl);
    auto existing = entries_.find(key);
    if (existing != entries_.end()) {
        if (existing->second.expiresAt.has_value()) {
            --expiringEntryCount_;
        }
        existing->second.value = value;
        existing->second.expiresAt = expiration;
        if (existing->second.expiresAt.has_value()) {
            ++expiringEntryCount_;
        }
        evictionPolicy_.touch(existing->second.lruIterator);
        return true;
    }

    auto lruIterator = evictionPolicy_.insert(key);
    entries_.emplace(key, CacheEntry{key, value, expiration, lruIterator});
    if (expiration.has_value()) {
        ++expiringEntryCount_;
    }
    enforceCapacity();
    return entries_.find(key) != entries_.end();
}

std::optional<Value> CacheStore::get(const Key& key)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto iterator = entries_.find(key);
    if (iterator == entries_.end()) {
        return std::nullopt;
    }

    if (TtlManager::isExpired(iterator->second.expiresAt)) {
        removeEntry(iterator);
        return std::nullopt;
    }

    evictionPolicy_.touch(iterator->second.lruIterator);
    return iterator->second.value;
}

bool CacheStore::remove(const Key& key)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto iterator = entries_.find(key);
    if (iterator == entries_.end()) {
        return false;
    }

    removeEntry(iterator);
    return true;
}

bool CacheStore::exists(const Key& key)
{
    return get(key).has_value();
}

std::size_t CacheStore::size()
{
    std::lock_guard<std::mutex> lock(mutex_);
    cleanupExpiredEntries();
    return entries_.size();
}

std::size_t CacheStore::capacity() const
{
    return maxCapacity_;
}

std::vector<std::pair<Key, Value>> CacheStore::entriesSnapshot()
{
    std::lock_guard<std::mutex> lock(mutex_);
    cleanupExpiredEntries();

    std::vector<std::pair<Key, Value>> snapshot;
    snapshot.reserve(entries_.size());
    for (const auto& pair : entries_) {
        snapshot.emplace_back(pair.first, pair.second.value);
    }
    return snapshot;
}

std::vector<CacheSnapshotEntry> CacheStore::snapshotEntries()
{
    std::lock_guard<std::mutex> lock(mutex_);
    cleanupExpiredEntries();

    std::vector<CacheSnapshotEntry> snapshot;
    snapshot.reserve(entries_.size());
    const auto now = Clock::now();
    for (const auto& pair : entries_) {
        std::optional<std::uint64_t> ttlMillis;
        if (pair.second.expiresAt.has_value()) {
            const auto remaining = pair.second.expiresAt.value() - now;
            ttlMillis = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count());
        }
        snapshot.push_back({pair.first, pair.second.value, ttlMillis});
    }
    return snapshot;
}

void CacheStore::clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
    evictionPolicy_.clear();
    expiringEntryCount_ = 0;
}

void CacheStore::removeEntry(EntryMap::iterator iterator)
{
    if (iterator->second.expiresAt.has_value()) {
        --expiringEntryCount_;
    }
    evictionPolicy_.erase(iterator->second.lruIterator);
    entries_.erase(iterator);
}

void CacheStore::cleanupExpiredEntries()
{
    if (expiringEntryCount_ == 0) {
        return;
    }

    std::vector<Key> expiredKeys;
    expiredKeys.reserve(expiringEntryCount_);

    const auto now = Clock::now();
    for (const auto& pair : entries_) {
        if (TtlManager::isExpiredAt(pair.second.expiresAt, now)) {
            expiredKeys.push_back(pair.first);
        }
    }

    for (const auto& key : expiredKeys) {
        auto iterator = entries_.find(key);
        if (iterator != entries_.end()) {
            removeEntry(iterator);
        }
    }
}

void CacheStore::enforceCapacity()
{
    while (entries_.size() > maxCapacity_) {
        auto key = evictionPolicy_.victim();
        if (!key.has_value()) {
            return;
        }

        auto iterator = entries_.find(key.value());
        if (iterator == entries_.end()) {
            return;
        }

        removeEntry(iterator);
    }
}

} // namespace distributed_cache
