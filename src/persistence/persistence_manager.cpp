#include "persistence/persistence_manager.hpp"

#include <filesystem>

namespace distributed_cache {

namespace {

std::string pathJoin(const std::string& directory, const std::string& fileName)
{
    return (std::filesystem::path(directory) / fileName).string();
}

} // namespace

PersistenceManager::PersistenceManager(std::shared_ptr<CacheStore> cache, PersistenceConfig config)
    : cache_(std::move(cache)),
      config_(std::move(config)),
      appendOnlyLog_(pathJoin(config_.directory, config_.appendOnlyFile), config_.fsyncPolicy),
      snapshotManager_(pathJoin(config_.directory, config_.snapshotFile))
{
    if (config_.backgroundSnapshots) {
        startBackgroundSnapshotting();
    }
}

PersistenceManager::~PersistenceManager()
{
    stopBackgroundSnapshotting();
}

bool PersistenceManager::put(const Key& key, const Value& value, std::optional<Duration> ttl)
{
    const bool stored = cache_->put(key, value, ttl);
    if (stored) {
        recordPut(key, value, ttl);
    }
    return stored;
}

bool PersistenceManager::remove(const Key& key)
{
    const bool removed = cache_->remove(key);
    if (removed) {
        recordDelete(key);
    }
    return removed;
}

void PersistenceManager::recordPut(const Key& key, const Value& value, std::optional<Duration> ttl)
{
    appendOnlyLog_.appendPut(key, value, ttlToMillis(ttl));
    auto logMetrics = appendOnlyLog_.metrics();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.logSizeBytes = logMetrics.bytesWritten;
    metrics_.fsyncLatency = logMetrics.lastFsyncLatency;
}

void PersistenceManager::recordDelete(const Key& key)
{
    appendOnlyLog_.appendDelete(key);
    auto logMetrics = appendOnlyLog_.metrics();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.logSizeBytes = logMetrics.bytesWritten;
    metrics_.fsyncLatency = logMetrics.lastFsyncLatency;
}

RecoveryMetrics PersistenceManager::recover()
{
    RecoveryManager recovery(snapshotManager_, appendOnlyLog_);
    const auto recoveryMetrics = recovery.recover(*cache_);
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.replayLatency = recoveryMetrics.lastReplayLatency;
    metrics_.recoveryLatency = recoveryMetrics.lastRecoveryLatency;
    metrics_.entriesReplayed = recoveryMetrics.entriesReplayed;
    metrics_.logSizeBytes = appendOnlyLog_.sizeBytes();
    return recoveryMetrics;
}

SnapshotMetadata PersistenceManager::createSnapshot()
{
    const auto metadata = snapshotManager_.createSnapshot(*cache_, appendOnlyLog_.lastSequence());
    const auto snapshotMetrics = snapshotManager_.metrics();
    std::lock_guard<std::mutex> lock(metricsMutex_);
    metrics_.snapshotLatency = snapshotMetrics.lastSnapshotLatency;
    return metadata;
}

void PersistenceManager::startBackgroundSnapshotting()
{
    bool expected = false;
    if (!snapshotting_.compare_exchange_strong(expected, true)) {
        return;
    }
    snapshotThread_ = std::thread([this] { snapshotLoop(); });
}

void PersistenceManager::stopBackgroundSnapshotting()
{
    snapshotting_ = false;
    snapshotCondition_.notify_all();
    if (snapshotThread_.joinable()) {
        snapshotThread_.join();
    }
}

PersistenceMetrics PersistenceManager::metrics() const
{
    std::lock_guard<std::mutex> lock(metricsMutex_);
    auto copy = metrics_;
    copy.logSizeBytes = appendOnlyLog_.sizeBytes();
    copy.fsyncLatency = appendOnlyLog_.metrics().lastFsyncLatency;
    return copy;
}

AppendOnlyLog& PersistenceManager::appendOnlyLog()
{
    return appendOnlyLog_;
}

SnapshotManager& PersistenceManager::snapshotManager()
{
    return snapshotManager_;
}

std::optional<std::uint64_t> PersistenceManager::ttlToMillis(std::optional<Duration> ttl) const
{
    if (!ttl.has_value()) {
        return std::nullopt;
    }
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(ttl.value()).count());
}

void PersistenceManager::snapshotLoop()
{
    std::unique_lock<std::mutex> lock(snapshotMutex_);
    while (snapshotting_) {
        if (snapshotCondition_.wait_for(lock, config_.snapshotInterval, [this] {
                return !snapshotting_;
            })) {
            break;
        }
        lock.unlock();
        createSnapshot();
        lock.lock();
    }
}

} // namespace distributed_cache
