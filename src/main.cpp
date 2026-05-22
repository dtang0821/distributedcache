#include "cache/cachestore.hpp"
#include "net/replication_server.hpp"
#include "net/tcpserver.hpp"
#include "persistence/persistence_manager.hpp"
#include "replication/replication_peer.hpp"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::size_t parseSizeArg(char* value, std::size_t fallback)
{
    if (value == nullptr) {
        return fallback;
    }

    char* end = nullptr;
    const auto parsed = std::strtoull(value, &end, 10);
    if (end == value || *end != '\0') {
        return fallback;
    }
    return static_cast<std::size_t>(parsed);
}

distributed_cache::PeerEndpoint parseEndpoint(const std::string& endpoint)
{
    const auto colon = endpoint.find(':');
    if (colon == std::string::npos) {
        throw std::invalid_argument("endpoint must be host:port");
    }

    distributed_cache::PeerEndpoint parsed;
    parsed.host = endpoint.substr(0, colon);
    parsed.port = static_cast<std::uint16_t>(
        parseSizeArg(const_cast<char*>(endpoint.c_str() + colon + 1), 0));
    if (parsed.host.empty() || parsed.port == 0) {
        throw std::invalid_argument("endpoint must be host:port");
    }
    return parsed;
}

distributed_cache::ClusterConfig parseConfig(int argc, char** argv)
{
    distributed_cache::ClusterConfig config;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--leader") {
            config.role = distributed_cache::NodeRole::Leader;
        } else if (arg == "--follower") {
            if (i + 1 >= argc) {
                throw std::invalid_argument("--follower requires leader host:port");
            }
            config.role = distributed_cache::NodeRole::Follower;
            config.leader = parseEndpoint(argv[++i]);
        } else if (arg == "--peer") {
            if (i + 1 >= argc) {
                throw std::invalid_argument("--peer requires follower host:replication_port");
            }
            config.followers.push_back(parseEndpoint(argv[++i]));
        } else if (arg == "--host" && i + 1 < argc) {
            config.host = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            config.clientPort = static_cast<std::uint16_t>(parseSizeArg(argv[++i], config.clientPort));
        } else if (arg == "--replication-port" && i + 1 < argc) {
            config.replicationPort = static_cast<std::uint16_t>(
                parseSizeArg(argv[++i], config.replicationPort));
        } else if (arg == "--capacity" && i + 1 < argc) {
            config.cacheCapacity = parseSizeArg(argv[++i], config.cacheCapacity);
        } else if (arg == "--workers" && i + 1 < argc) {
            config.clientWorkers = parseSizeArg(argv[++i], config.clientWorkers);
        } else if (arg == "--replication-workers" && i + 1 < argc) {
            config.replicationWorkers = parseSizeArg(argv[++i], config.replicationWorkers);
        } else if (arg == "--data-dir" && i + 1 < argc) {
            config.persistenceDirectory = argv[++i];
        } else if (arg == "--fsync" && i + 1 < argc) {
            config.fsyncPolicy = argv[++i];
        } else if (arg == "--snapshot-interval" && i + 1 < argc) {
            config.snapshotInterval = std::chrono::seconds(parseSizeArg(argv[++i], 30));
        } else if (arg == "--no-persistence") {
            config.persistenceEnabled = false;
        } else if (arg.find("--") == 0) {
            throw std::invalid_argument("unknown option: " + arg);
        } else {
            config.host = arg;
            if (i + 1 < argc) {
                config.clientPort = static_cast<std::uint16_t>(parseSizeArg(argv[++i], config.clientPort));
            }
            if (i + 1 < argc) {
                config.cacheCapacity = parseSizeArg(argv[++i], config.cacheCapacity);
            }
            if (i + 1 < argc) {
                config.clientWorkers = parseSizeArg(argv[++i], config.clientWorkers);
            }
        }
    }
    return config;
}

distributed_cache::FsyncPolicy parseFsyncPolicy(const std::string& value)
{
    if (value == "always") {
        return distributed_cache::FsyncPolicy::Always;
    }
    if (value == "never") {
        return distributed_cache::FsyncPolicy::Never;
    }
    return distributed_cache::FsyncPolicy::EverySecond;
}

} // namespace

int main(int argc, char** argv)
{
    try {
        const auto config = parseConfig(argc, argv);
        auto cache = std::make_shared<distributed_cache::CacheStore>(config.cacheCapacity);
        auto replicationManager = std::make_shared<distributed_cache::ReplicationManager>(
            config.role, cache, config.replicationWorkers);
        std::shared_ptr<distributed_cache::PersistenceManager> persistenceManager;

        if (config.persistenceEnabled) {
            distributed_cache::PersistenceConfig persistenceConfig;
            persistenceConfig.directory = config.persistenceDirectory;
            persistenceConfig.fsyncPolicy = parseFsyncPolicy(config.fsyncPolicy);
            persistenceConfig.snapshotInterval = config.snapshotInterval;
            persistenceManager = std::make_shared<distributed_cache::PersistenceManager>(
                cache, persistenceConfig);
            persistenceManager->recover();
            replicationManager->setPersistenceManager(persistenceManager);
        }

        for (const auto& follower : config.followers) {
            replicationManager->addPeer(
                std::make_shared<distributed_cache::ReplicationPeer>(follower.host, follower.port));
        }

        if (config.role == distributed_cache::NodeRole::Follower && config.leader.port != 0) {
            const auto recovered = distributed_cache::ReplicationPeer::fetchFromLeader(
                config.leader.host, config.leader.port, replicationManager->lastSequence());
            for (const auto& message : recovered) {
                replicationManager->applyReplicaMessage(message);
            }
            std::cout << "follower recovered " << recovered.size() << " entries from leader" << '\n';
        }

        distributed_cache::ReplicationServer replicationServer(
            config.host, config.replicationPort, replicationManager, config.replicationWorkers);
        std::thread replicationThread([&replicationServer] { replicationServer.run(); });

        distributed_cache::TcpServer server(config.host,
                                           config.clientPort,
                                           cache,
                                           config.clientWorkers,
                                           config.role,
                                           replicationManager,
                                           persistenceManager);
        server.run();
        replicationServer.stop();
        if (replicationThread.joinable()) {
            replicationThread.join();
        }
    } catch (const std::exception& error) {
        std::cerr << "server failed: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
