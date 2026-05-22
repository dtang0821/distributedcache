#include "persistence/log_entry.hpp"

#include <chrono>
#include <sstream>

namespace distributed_cache {

std::string LogEntry::serialize() const
{
    std::ostringstream stream;
    if (operation == PersistenceOperation::Put) {
        stream << "PUT " << sequence << ' ' << timestampMillis << ' '
               << (ttlMillis.has_value() ? std::to_string(ttlMillis.value()) : "-1") << ' '
               << key.size() << ' ' << value.size() << ' ' << key << value;
    } else {
        stream << "DELETE " << sequence << ' ' << timestampMillis << ' '
               << key.size() << ' ' << key;
    }
    return stream.str();
}

std::optional<LogEntry> LogEntry::parse(const std::string& line)
{
    std::istringstream stream(line);
    std::string operation;
    stream >> operation;

    if (operation == "PUT") {
        LogEntry entry;
        entry.operation = PersistenceOperation::Put;
        long long ttl = -1;
        std::size_t keyLength = 0;
        std::size_t valueLength = 0;
        stream >> entry.sequence >> entry.timestampMillis >> ttl >> keyLength >> valueLength;
        if (!stream || stream.get() != ' ') {
            return std::nullopt;
        }
        std::string payload;
        std::getline(stream, payload);
        if (payload.size() != keyLength + valueLength) {
            return std::nullopt;
        }
        entry.key = payload.substr(0, keyLength);
        entry.value = payload.substr(keyLength, valueLength);
        if (ttl >= 0) {
            entry.ttlMillis = static_cast<std::uint64_t>(ttl);
        }
        return entry;
    }

    if (operation == "DELETE") {
        LogEntry entry;
        entry.operation = PersistenceOperation::Delete;
        std::size_t keyLength = 0;
        stream >> entry.sequence >> entry.timestampMillis >> keyLength;
        if (!stream || stream.get() != ' ') {
            return std::nullopt;
        }
        std::getline(stream, entry.key);
        if (entry.key.size() != keyLength) {
            return std::nullopt;
        }
        return entry;
    }

    return std::nullopt;
}

std::uint64_t LogEntry::nowMillis()
{
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

} // namespace distributed_cache
