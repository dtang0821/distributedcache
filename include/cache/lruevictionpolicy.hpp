#pragma once

#include "cache/evictionpolicy.hpp"

namespace distributed_cache {

class LruEvictionPolicy final : public EvictionPolicy {
public:
    LruIterator insert(const Key& key) override;
    void touch(LruIterator iterator) override;
    void erase(LruIterator iterator) override;
    std::optional<Key> victim() const override;
    void clear() override;
    std::size_t size() const override;

private:
    LruList usageOrder_;
};

} // namespace distributed_cache
