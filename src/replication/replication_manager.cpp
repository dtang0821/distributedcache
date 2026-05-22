#include "replication/replication_manager.hpp"

#include <stdexcept>

namespace distributed_cache {

ReplicationManager::ReplicationManager(NodeRole role,
                                       std::shared_ptr<CacheStore> cache,
                                       std::size_t workerThreads)
    : role_(role),
      cache_(std::move(cache)),
      replicationPool_(workerThreads)
{
    if (!cache_) {
        throw std::invalid_argument("replication manager requires cache");
    }
}

NodeRole ReplicationManager::role() const
{
    return role_;
}

bool ReplicationManager::isLeader() const
{
    return role_ == NodeRole::Leader;
}

ReplicationMessage ReplicationManager::replicatePut(const Key& key, const Value& value)
{
    if (!isLeader()) {
        throw std::logic_error("followers cannot originate replicated writes");
    }

    auto message = log_.appendPut(key, value);
    broadcast(message);
    return message;
}

ReplicationMessage ReplicationManager::replicateDelete(const Key& key)
{
    if (!isLeader()) {
        throw std::logic_error("followers cannot originate replicated deletes");
    }

    auto message = log_.appendDelete(key);
    broadcast(message);
    return message;
}

void ReplicationManager::applyReplicaMessage(const ReplicationMessage& message)
{
    if (message.operation == ReplicationOperation::Put) {
        if (persistenceManager_) {
            persistenceManager_->put(message.key, message.value);
        } else {
            cache_->put(message.key, message.value);
        }
    } else {
        if (persistenceManager_) {
            persistenceManager_->remove(message.key);
        } else {
            cache_->remove(message.key);
        }
    }
    log_.appendReplicaMessage(message);
}

void ReplicationManager::setPersistenceManager(std::shared_ptr<PersistenceManager> persistenceManager)
{
    persistenceManager_ = std::move(persistenceManager);
}

void ReplicationManager::addPeer(std::shared_ptr<ReplicationPeer> peer)
{
    if (!peer) {
        return;
    }
    std::lock_guard<std::mutex> lock(peersMutex_);
    peers_.push_back(std::move(peer));
}

std::vector<ReplicationMessage> ReplicationManager::entriesAfter(std::uint64_t sequence) const
{
    return log_.entriesAfter(sequence);
}

std::uint64_t ReplicationManager::lastSequence() const
{
    return log_.lastSequence();
}

std::size_t ReplicationManager::peerCount() const
{
    std::lock_guard<std::mutex> lock(peersMutex_);
    return peers_.size();
}

void ReplicationManager::broadcast(const ReplicationMessage& message)
{
    std::vector<std::shared_ptr<ReplicationPeer>> peers;
    {
        std::lock_guard<std::mutex> lock(peersMutex_);
        peers = peers_;
    }

    for (auto& peer : peers) {
        replicationPool_.submit([peer, message] {
            peer->send(message);
        });
    }
}

} // namespace distributed_cache
