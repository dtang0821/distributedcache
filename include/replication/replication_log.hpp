#pragma once

#include "replication/replication_message.hpp"

#include <cstdint>
#include <mutex>
#include <vector>

namespace distributed_cache {

class ReplicationLog {
public:
    ReplicationMessage appendPut(const Key& key, const Value& value);
    ReplicationMessage appendDelete(const Key& key);
    void appendReplicaMessage(const ReplicationMessage& message);

    std::vector<ReplicationMessage> entriesAfter(std::uint64_t sequence) const;
    std::uint64_t lastSequence() const;
    std::size_t size() const;

private:
    mutable std::mutex mutex_;
    std::vector<ReplicationMessage> entries_;
    std::uint64_t nextSequence_ = 1;
};

} // namespace distributed_cache
