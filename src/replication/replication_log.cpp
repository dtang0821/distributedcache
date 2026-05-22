#include "replication/replication_log.hpp"

#include <algorithm>

namespace distributed_cache {

ReplicationMessage ReplicationLog::appendPut(const Key& key, const Value& value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ReplicationMessage message{ReplicationOperation::Put, nextSequence_++, key, value,
                               ReplicationMessage::nowMillis()};
    entries_.push_back(message);
    return message;
}

ReplicationMessage ReplicationLog::appendDelete(const Key& key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    ReplicationMessage message{ReplicationOperation::Delete, nextSequence_++, key, {},
                               ReplicationMessage::nowMillis()};
    entries_.push_back(message);
    return message;
}

void ReplicationLog::appendReplicaMessage(const ReplicationMessage& message)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!entries_.empty() && entries_.back().sequence >= message.sequence) {
        return;
    }
    entries_.push_back(message);
    nextSequence_ = std::max(nextSequence_, message.sequence + 1);
}

std::vector<ReplicationMessage> ReplicationLog::entriesAfter(std::uint64_t sequence) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ReplicationMessage> result;
    for (const auto& entry : entries_) {
        if (entry.sequence > sequence) {
            result.push_back(entry);
        }
    }
    return result;
}

std::uint64_t ReplicationLog::lastSequence() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (entries_.empty()) {
        return 0;
    }
    return entries_.back().sequence;
}

std::size_t ReplicationLog::size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return entries_.size();
}

} // namespace distributed_cache
