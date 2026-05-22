#pragma once

#include "common/types.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace distributed_cache {

enum class PersistenceOperation {
    Put,
    Delete
};

struct LogEntry {
    PersistenceOperation operation = PersistenceOperation::Put;
    std::uint64_t sequence = 0;
    std::uint64_t timestampMillis = 0;
    Key key;
    Value value;
    std::optional<std::uint64_t> ttlMillis;

    std::string serialize() const;
    static std::optional<LogEntry> parse(const std::string& line);
    static std::uint64_t nowMillis();
};

} // namespace distributed_cache
