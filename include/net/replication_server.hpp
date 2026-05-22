#pragma once

#include "net/client_session.hpp"
#include "replication/replication_manager.hpp"
#include "utils/threadpool.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace distributed_cache {

class ReplicationServer {
public:
    ReplicationServer(std::string host,
                      std::uint16_t port,
                      std::shared_ptr<ReplicationManager> replicationManager,
                      std::size_t workerThreads);
    ~ReplicationServer();

    ReplicationServer(const ReplicationServer&) = delete;
    ReplicationServer& operator=(const ReplicationServer&) = delete;

    void run();
    void stop();

private:
    void initializeSockets();
    SocketHandle createListenSocket();
    void closeListenSocket();

    std::string host_;
    std::uint16_t port_;
    std::shared_ptr<ReplicationManager> replicationManager_;
    ThreadPool threadPool_;
    std::atomic<bool> running_{false};
    SocketHandle listenSocket_;
    bool socketsInitialized_ = false;
};

} // namespace distributed_cache
