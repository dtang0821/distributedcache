#pragma once

#include "cache/cachestore.hpp"
#include "persistence/persistence_manager.hpp"
#include "replication/cluster_config.hpp"
#include "replication/replication_log.hpp"
#include "replication/replication_peer.hpp"
#include "utils/threadpool.hpp"

#include <memory>
#include <mutex>
#include <vector>

namespace distributed_cache {

class ReplicationManager {
public:
    ReplicationManager(NodeRole role, std::shared_ptr<CacheStore> cache, std::size_t workerThreads);

    NodeRole role() const;
    bool isLeader() const;

    ReplicationMessage replicatePut(const Key& key, const Value& value);
    ReplicationMessage replicateDelete(const Key& key);
    void applyReplicaMessage(const ReplicationMessage& message);
    void setPersistenceManager(std::shared_ptr<PersistenceManager> persistenceManager);

    void addPeer(std::shared_ptr<ReplicationPeer> peer);
    std::vector<ReplicationMessage> entriesAfter(std::uint64_t sequence) const;
    std::uint64_t lastSequence() const;
    std::size_t peerCount() const;

private:
    void broadcast(const ReplicationMessage& message);

    NodeRole role_;
    std::shared_ptr<CacheStore> cache_;
    std::shared_ptr<PersistenceManager> persistenceManager_;
    mutable std::mutex peersMutex_;
    std::vector<std::shared_ptr<ReplicationPeer>> peers_;
    ReplicationLog log_;
    ThreadPool replicationPool_;
};

} // namespace distributed_cache
