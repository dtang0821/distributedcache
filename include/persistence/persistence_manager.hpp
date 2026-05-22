#pragma once

#include "cache/cachestore.hpp"
#include "persistence/append_only_log.hpp"
#include "persistence/persistence_config.hpp"
#include "persistence/recovery_manager.hpp"
#include "persistence/snapshot_manager.hpp"

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace distributed_cache {

struct PersistenceMetrics {
    std::uint64_t logSizeBytes = 0;
    std::chrono::nanoseconds replayLatency{0};
    std::chrono::nanoseconds snapshotLatency{0};
    std::chrono::nanoseconds fsyncLatency{0};
    std::chrono::nanoseconds recoveryLatency{0};
    std::uint64_t entriesReplayed = 0;
};

class PersistenceManager {
public:
    PersistenceManager(std::shared_ptr<CacheStore> cache, PersistenceConfig config);
    ~PersistenceManager();

    bool put(const Key& key, const Value& value, std::optional<Duration> ttl = std::nullopt);
    bool remove(const Key& key);
    void recordPut(const Key& key, const Value& value, std::optional<Duration> ttl = std::nullopt);
    void recordDelete(const Key& key);

    RecoveryMetrics recover();
    SnapshotMetadata createSnapshot();
    void startBackgroundSnapshotting();
    void stopBackgroundSnapshotting();
    PersistenceMetrics metrics() const;

    AppendOnlyLog& appendOnlyLog();
    SnapshotManager& snapshotManager();

private:
    std::optional<std::uint64_t> ttlToMillis(std::optional<Duration> ttl) const;
    void snapshotLoop();

    std::shared_ptr<CacheStore> cache_;
    PersistenceConfig config_;
    AppendOnlyLog appendOnlyLog_;
    SnapshotManager snapshotManager_;
    mutable std::mutex metricsMutex_;
    PersistenceMetrics metrics_;
    std::atomic<bool> snapshotting_{false};
    std::condition_variable snapshotCondition_;
    std::mutex snapshotMutex_;
    std::thread snapshotThread_;
};

} // namespace distributed_cache
