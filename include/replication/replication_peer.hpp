#pragma once

#include "replication/replication_message.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace distributed_cache {

class ReplicationPeer {
public:
    ReplicationPeer(std::string host, std::uint16_t port);
    virtual ~ReplicationPeer() = default;

    virtual bool send(const ReplicationMessage& message);

    const std::string& host() const;
    std::uint16_t port() const;
    std::uint64_t lastAckedSequence() const;
    void markAcked(std::uint64_t sequence);

    static std::vector<ReplicationMessage> fetchFromLeader(const std::string& host,
                                                           std::uint16_t port,
                                                           std::uint64_t afterSequence);

private:
    std::string host_;
    std::uint16_t port_;
    mutable std::mutex mutex_;
    std::uint64_t lastAckedSequence_ = 0;
};

} // namespace distributed_cache
