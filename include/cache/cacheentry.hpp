#pragma once

#include "common/types.hpp"

#include <optional>
#include <utility>

namespace distributed_cache {

struct CacheEntry {
    Key key;
    Value value;
    std::optional<TimePoint> expiresAt;
    LruIterator lruIterator;

    CacheEntry(Key entryKey,
               Value entryValue,
               std::optional<TimePoint> expiration,
               LruIterator iterator)
        : key(std::move(entryKey)),
          value(std::move(entryValue)),
          expiresAt(expiration),
          lruIterator(iterator) {}
};

} // namespace distributed_cache
