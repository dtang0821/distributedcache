#include "persistence/append_only_log.hpp"

#include "utils/timer.hpp"

#include <filesystem>
#include <stdexcept>

namespace distributed_cache {

AppendOnlyLog::AppendOnlyLog(std::string filePath, FsyncPolicy policy)
    : filePath_(std::move(filePath)),
      policy_(policy),
      lastFlush_(std::chrono::steady_clock::now())
{
    const auto parent = std::filesystem::path(filePath_).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
    initializeSequenceFromDisk();
    open();
}

AppendOnlyLog::~AppendOnlyLog()
{
    flush();
}

LogEntry AppendOnlyLog::appendPut(const Key& key,
                                  const Value& value,
                                  std::optional<std::uint64_t> ttlMillis)
{
    std::lock_guard<std::mutex> lock(mutex_);
    LogEntry entry{PersistenceOperation::Put, nextSequence_++, LogEntry::nowMillis(), key, value, ttlMillis};
    append(entry);
    return entry;
}

LogEntry AppendOnlyLog::appendDelete(const Key& key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    LogEntry entry{PersistenceOperation::Delete, nextSequence_++, LogEntry::nowMillis(), key, {}, std::nullopt};
    append(entry);
    return entry;
}

void AppendOnlyLog::append(const LogEntry& entry)
{
    const auto line = entry.serialize();
    stream_ << line << '\n';
    if (!stream_) {
        throw std::runtime_error("failed to append persistence log entry");
    }
    metrics_.entriesWritten += 1;
    metrics_.bytesWritten += line.size() + 1;
    maybeFlush();
}

void AppendOnlyLog::flush()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!stream_.is_open()) {
        return;
    }
    Timer timer;
    stream_.flush();
    metrics_.lastFsyncLatency = timer.elapsedNanoseconds();
    lastFlush_ = std::chrono::steady_clock::now();
}

std::vector<LogEntry> AppendOnlyLog::replayAfter(std::uint64_t sequence) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    stream_.flush();

    std::vector<LogEntry> entries;
    std::ifstream input(filePath_);
    std::string line;
    while (std::getline(input, line)) {
        const auto parsed = LogEntry::parse(line);
        if (parsed.has_value() && parsed->sequence > sequence) {
            entries.push_back(parsed.value());
        }
    }
    return entries;
}

std::uint64_t AppendOnlyLog::lastSequence() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return nextSequence_ == 1 ? 0 : nextSequence_ - 1;
}

std::uint64_t AppendOnlyLog::sizeBytes() const
{
    std::error_code error;
    const auto size = std::filesystem::file_size(filePath_, error);
    return error ? 0 : static_cast<std::uint64_t>(size);
}

AppendOnlyLogMetrics AppendOnlyLog::metrics() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto copy = metrics_;
    copy.bytesWritten = sizeBytes();
    return copy;
}

const std::string& AppendOnlyLog::filePath() const
{
    return filePath_;
}

void AppendOnlyLog::open()
{
    stream_.open(filePath_, std::ios::out | std::ios::app);
    if (!stream_) {
        throw std::runtime_error("failed to open append-only log");
    }
}

void AppendOnlyLog::maybeFlush()
{
    if (policy_ == FsyncPolicy::Never) {
        return;
    }
    if (policy_ == FsyncPolicy::EverySecond &&
        std::chrono::steady_clock::now() - lastFlush_ < std::chrono::seconds(1)) {
        return;
    }

    Timer timer;
    stream_.flush();
    metrics_.lastFsyncLatency = timer.elapsedNanoseconds();
    lastFlush_ = std::chrono::steady_clock::now();
}

void AppendOnlyLog::initializeSequenceFromDisk()
{
    std::ifstream input(filePath_);
    std::string line;
    while (std::getline(input, line)) {
        const auto parsed = LogEntry::parse(line);
        if (parsed.has_value() && parsed->sequence >= nextSequence_) {
            nextSequence_ = parsed->sequence + 1;
        }
    }
}

} // namespace distributed_cache
