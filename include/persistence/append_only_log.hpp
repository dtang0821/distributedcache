#pragma once

#include "persistence/log_entry.hpp"
#include "persistence/persistence_config.hpp"

#include <chrono>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace distributed_cache {

struct AppendOnlyLogMetrics {
    std::uint64_t entriesWritten = 0;
    std::uint64_t bytesWritten = 0;
    std::chrono::nanoseconds lastFsyncLatency{0};
};

class AppendOnlyLog {
public:
    AppendOnlyLog(std::string filePath, FsyncPolicy policy);
    ~AppendOnlyLog();

    LogEntry appendPut(const Key& key, const Value& value, std::optional<std::uint64_t> ttlMillis);
    LogEntry appendDelete(const Key& key);
    void append(const LogEntry& entry);
    void flush();

    std::vector<LogEntry> replayAfter(std::uint64_t sequence) const;
    std::uint64_t lastSequence() const;
    std::uint64_t sizeBytes() const;
    AppendOnlyLogMetrics metrics() const;
    const std::string& filePath() const;

private:
    void open();
    void maybeFlush();
    void initializeSequenceFromDisk();

    std::string filePath_;
    FsyncPolicy policy_;
    mutable std::mutex mutex_;
    mutable std::ofstream stream_;
    std::uint64_t nextSequence_ = 1;
    AppendOnlyLogMetrics metrics_;
    std::chrono::steady_clock::time_point lastFlush_;
};

} // namespace distributed_cache
