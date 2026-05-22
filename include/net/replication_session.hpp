#pragma once

#include "net/client_session.hpp"
#include "replication/replication_manager.hpp"

#include <memory>
#include <string>

namespace distributed_cache {

class ReplicationSession {
public:
    ReplicationSession(SocketHandle socket, std::shared_ptr<ReplicationManager> replicationManager);

    void run();

private:
    bool readLine(std::string& line);
    bool sendLine(const std::string& line);
    void handleLine(const std::string& line);
    void handleSyncFrom(const std::string& line);
    void closeSocket();

    SocketHandle socket_;
    std::shared_ptr<ReplicationManager> replicationManager_;
};

} // namespace distributed_cache
