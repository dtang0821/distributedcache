#pragma once

#include "common/types.hpp"

#include <optional>

namespace distributed_cache {

class EvictionPolicy {
public:
    virtual ~EvictionPolicy() = default;

    virtual LruIterator insert(const Key& key) = 0;
    virtual void touch(LruIterator iterator) = 0;
    virtual void erase(LruIterator iterator) = 0;
    virtual std::optional<Key> victim() const = 0;
    virtual void clear() = 0;
    virtual std::size_t size() const = 0;
};

} // namespace distributed_cache
