#include "persistence/recovery_manager.hpp"

#include "utils/timer.hpp"

namespace distributed_cache {

RecoveryManager::RecoveryManager(SnapshotManager& snapshotManager, AppendOnlyLog& appendOnlyLog)
    : snapshotManager_(snapshotManager),
      appendOnlyLog_(appendOnlyLog)
{
}

RecoveryMetrics RecoveryManager::recover(CacheStore& cache)
{
    Timer recoveryTimer;
    std::uint64_t replayFrom = 0;

    const auto snapshot = snapshotManager_.loadSnapshot(cache);
    if (snapshot.has_value()) {
        replayFrom = snapshot->lastAppliedSequence;
    } else {
        cache.clear();
    }

    Timer replayTimer;
    const auto entries = appendOnlyLog_.replayAfter(replayFrom);
    const auto now = LogEntry::nowMillis();
    for (const auto& entry : entries) {
        if (entry.operation == PersistenceOperation::Put) {
            std::optional<Duration> ttl;
            if (entry.ttlMillis.has_value()) {
                const auto elapsed = now > entry.timestampMillis ? now - entry.timestampMillis : 0;
                if (entry.ttlMillis.value() <= elapsed) {
                    cache.remove(entry.key);
                    continue;
                }
                ttl = std::chrono::milliseconds(entry.ttlMillis.value() - elapsed);
            }
            cache.put(entry.key, entry.value, ttl);
        } else {
            cache.remove(entry.key);
        }
    }

    RecoveryMetrics metrics;
    metrics.lastReplayLatency = replayTimer.elapsedNanoseconds();
    metrics.lastRecoveryLatency = recoveryTimer.elapsedNanoseconds();
    metrics.entriesReplayed = entries.size();
    return metrics;
}

} // namespace distributed_cache
