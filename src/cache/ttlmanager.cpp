#include "cache/ttlmanager.hpp"

namespace distributed_cache {

std::optional<TimePoint> TtlManager::expirationFromNow(std::optional<Duration> ttl)
{
    if (!ttl.has_value()) {
        return std::nullopt;
    }
    return Clock::now() + ttl.value();
}

bool TtlManager::isExpired(const std::optional<TimePoint>& expiresAt)
{
    return isExpiredAt(expiresAt, Clock::now());
}

bool TtlManager::isExpiredAt(const std::optional<TimePoint>& expiresAt, TimePoint now)
{
    return expiresAt.has_value() && expiresAt.value() <= now;
}

} // namespace distributed_cache
