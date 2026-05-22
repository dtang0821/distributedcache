#include "persistence/snapshot_manager.hpp"

#include "persistence/log_entry.hpp"
#include "utils/timer.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace distributed_cache {

SnapshotManager::SnapshotManager(std::string filePath)
    : filePath_(std::move(filePath))
{
    const auto parent = std::filesystem::path(filePath_).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
}

SnapshotMetadata SnapshotManager::createSnapshot(CacheStore& cache, std::uint64_t lastAppliedSequence)
{
    Timer timer;
    const auto entries = cache.snapshotEntries();
    const auto tempPath = filePath_ + ".tmp";

    std::ofstream output(tempPath, std::ios::out | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("failed to open snapshot file");
    }

    SnapshotMetadata metadata{LogEntry::nowMillis(), lastAppliedSequence, filePath_};
    output << "SNAPSHOT " << metadata.timestampMillis << ' '
           << metadata.lastAppliedSequence << ' ' << entries.size() << '\n';

    for (const auto& entry : entries) {
        output << "ENTRY "
               << (entry.ttlMillisRemaining.has_value()
                       ? std::to_string(entry.ttlMillisRemaining.value())
                       : "-1")
               << ' ' << entry.key.size() << ' ' << entry.value.size() << ' '
               << entry.key << entry.value << '\n';
    }
    output.flush();
    output.close();
    std::filesystem::rename(tempPath, filePath_);

    metrics_.lastSnapshotLatency = timer.elapsedNanoseconds();
    metrics_.snapshotsWritten += 1;
    return metadata;
}

std::optional<SnapshotMetadata> SnapshotManager::loadSnapshot(CacheStore& cache) const
{
    std::ifstream input(filePath_);
    if (!input) {
        return std::nullopt;
    }

    std::string header;
    std::getline(input, header);
    std::istringstream headerStream(header);
    std::string marker;
    std::size_t expectedEntries = 0;
    SnapshotMetadata metadata;
    metadata.filePath = filePath_;
    headerStream >> marker >> metadata.timestampMillis >> metadata.lastAppliedSequence >> expectedEntries;
    if (marker != "SNAPSHOT" || !headerStream) {
        return std::nullopt;
    }

    cache.clear();
    std::string line;
    std::size_t loadedEntries = 0;
    while (std::getline(input, line)) {
        std::istringstream stream(line);
        std::string entryMarker;
        long long ttl = -1;
        std::size_t keyLength = 0;
        std::size_t valueLength = 0;
        stream >> entryMarker >> ttl >> keyLength >> valueLength;
        if (entryMarker != "ENTRY" || !stream || stream.get() != ' ') {
            continue;
        }

        std::string payload;
        std::getline(stream, payload);
        if (payload.size() != keyLength + valueLength) {
            continue;
        }

        const auto key = payload.substr(0, keyLength);
        const auto value = payload.substr(keyLength, valueLength);
        std::optional<Duration> ttlDuration;
        if (ttl >= 0) {
            const auto elapsed = LogEntry::nowMillis() > metadata.timestampMillis
                                     ? LogEntry::nowMillis() - metadata.timestampMillis
                                     : 0;
            if (static_cast<std::uint64_t>(ttl) <= elapsed) {
                continue;
            }
            ttlDuration = std::chrono::milliseconds(static_cast<std::uint64_t>(ttl) - elapsed);
        }
        cache.put(key, value, ttlDuration);
        ++loadedEntries;
    }

    if (loadedEntries > expectedEntries) {
        return std::nullopt;
    }
    return metadata;
}

SnapshotMetrics SnapshotManager::metrics() const
{
    return metrics_;
}

const std::string& SnapshotManager::filePath() const
{
    return filePath_;
}

} // namespace distributed_cache
