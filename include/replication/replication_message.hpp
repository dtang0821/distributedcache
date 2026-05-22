#pragma once

#include "common/types.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace distributed_cache {

enum class ReplicationOperation {
    Put,
    Delete
};

struct ReplicationMessage {
    ReplicationOperation operation = ReplicationOperation::Put;
    std::uint64_t sequence = 0;
    Key key;
    Value value;
    std::uint64_t timestampMillis = 0;

    std::string serialize() const;

    static std::optional<ReplicationMessage> parse(const std::string& line);
    static std::uint64_t nowMillis();
};

} // namespace distributed_cache
