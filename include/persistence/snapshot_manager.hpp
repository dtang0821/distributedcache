#pragma once

#include "cache/cachestore.hpp"
#include "persistence/snapshot_metadata.hpp"

#include <chrono>
#include <optional>
#include <string>

namespace distributed_cache {

struct SnapshotMetrics {
    std::chrono::nanoseconds lastSnapshotLatency{0};
    std::uint64_t snapshotsWritten = 0;
};

class SnapshotManager {
public:
    explicit SnapshotManager(std::string filePath);

    SnapshotMetadata createSnapshot(CacheStore& cache, std::uint64_t lastAppliedSequence);
    std::optional<SnapshotMetadata> loadSnapshot(CacheStore& cache) const;

    SnapshotMetrics metrics() const;
    const std::string& filePath() const;

private:
    std::string filePath_;
    mutable SnapshotMetrics metrics_;
};

} // namespace distributed_cache
