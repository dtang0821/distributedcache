#pragma once

#include "common/types.hpp"

#include <optional>

namespace distributed_cache {

class TtlManager {
public:
    static std::optional<TimePoint> expirationFromNow(std::optional<Duration> ttl);
    static bool isExpired(const std::optional<TimePoint>& expiresAt);
    static bool isExpiredAt(const std::optional<TimePoint>& expiresAt, TimePoint now);
};

} // namespace distributed_cache
