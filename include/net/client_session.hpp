#pragma once

#include "cache/cachestore.hpp"
#include "persistence/persistence_manager.hpp"
#include "replication/cluster_config.hpp"
#include "replication/replication_manager.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace distributed_cache {

#ifdef _WIN32
using SocketHandle = uintptr_t;
#else
using SocketHandle = int;
#endif

class ClientSession {
public:
    ClientSession(SocketHandle socket,
                  std::shared_ptr<CacheStore> cache,
                  NodeRole role = NodeRole::Leader,
                  std::shared_ptr<ReplicationManager> replicationManager = nullptr,
                  std::shared_ptr<PersistenceManager> persistenceManager = nullptr);

    void run();

private:
    bool readLine(std::string& line);
    bool sendResponse(const std::string& response);
    std::string executeCommand(const std::string& line);
    void closeSocket();

    SocketHandle socket_;
    std::shared_ptr<CacheStore> cache_;
    NodeRole role_;
    std::shared_ptr<ReplicationManager> replicationManager_;
    std::shared_ptr<PersistenceManager> persistenceManager_;
};

} // namespace distributed_cache
