#pragma once

#include <cstdint>
#include <string>

namespace distributed_cache {

struct VirtualNode {
    std::string nodeId;
    std::size_t index = 0;
    std::uint64_t position = 0;
};

} // namespace distributed_cache
