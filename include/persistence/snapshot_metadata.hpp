#pragma once

#include <cstdint>
#include <string>

namespace distributed_cache {

struct SnapshotMetadata {
    std::uint64_t timestampMillis = 0;
    std::uint64_t lastAppliedSequence = 0;
    std::string filePath;
};

} // namespace distributed_cache
