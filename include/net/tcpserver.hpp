#pragma once

#include "cache/cachestore.hpp"
#include "net/client_session.hpp"
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
              std::size_t workerThreads);
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
    ThreadPool threadPool_;
    std::atomic<bool> running_{false};
    SocketHandle listenSocket_;
    bool socketsInitialized_ = false;
};

} // namespace distributed_cache
