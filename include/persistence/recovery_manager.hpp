#pragma once

#include "cache/cachestore.hpp"
#include "persistence/append_only_log.hpp"
#include "persistence/snapshot_manager.hpp"

#include <chrono>

namespace distributed_cache {

struct RecoveryMetrics {
    std::chrono::nanoseconds lastReplayLatency{0};
    std::chrono::nanoseconds lastRecoveryLatency{0};
    std::uint64_t entriesReplayed = 0;
};

class RecoveryManager {
public:
    RecoveryManager(SnapshotManager& snapshotManager, AppendOnlyLog& appendOnlyLog);

    RecoveryMetrics recover(CacheStore& cache);

private:
    SnapshotManager& snapshotManager_;
    AppendOnlyLog& appendOnlyLog_;
};

} // namespace distributed_cache
