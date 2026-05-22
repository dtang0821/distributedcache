#pragma once

#include "cache/cachestore.hpp"
#include "net/client_session.hpp"
#include "persistence/persistence_manager.hpp"
#include "replication/cluster_config.hpp"
#include "replication/replication_manager.hpp"
#include "utils/threadpool.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace distributed_cache {

class TcpServer {
public:
    TcpServer(std::string host,
              std::uint16_t port,
              std::shared_ptr<CacheStore> cache,
              std::size_t workerThreads,
              NodeRole role = NodeRole::Leader,
              std::shared_ptr<ReplicationManager> replicationManager = nullptr,
              std::shared_ptr<PersistenceManager> persistenceManager = nullptr);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void run();
    void stop();

private:
    void initializeSockets();
    SocketHandle createListenSocket();
    void closeListenSocket();

    std::string host_;
    std::uint16_t port_;
    std::shared_ptr<CacheStore> cache_;
    NodeRole role_;
    std::shared_ptr<ReplicationManager> replicationManager_;
    std::shared_ptr<PersistenceManager> persistenceManager_;
    ThreadPool threadPool_;
    std::atomic<bool> running_{false};
    SocketHandle listenSocket_;
    bool socketsInitialized_ = false;
};

} // namespace distributed_cache
