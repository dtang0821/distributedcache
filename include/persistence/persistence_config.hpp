#pragma once

#include <chrono>
#include <string>

namespace distributed_cache {

enum class FsyncPolicy {
    Always,
    EverySecond,
    Never
};

struct PersistenceConfig {
    std::string directory = "data";
    std::string appendOnlyFile = "cache.aof";
    std::string snapshotFile = "cache.snapshot";
    FsyncPolicy fsyncPolicy = FsyncPolicy::EverySecond;
    std::chrono::seconds snapshotInterval{30};
    bool backgroundSnapshots = true;
};

} // namespace distributed_cache
