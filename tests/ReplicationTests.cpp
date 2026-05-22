#include "cache/cachestore.hpp"
#include "replication/replication_manager.hpp"
#include "replication/replication_peer.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

using distributed_cache::CacheStore;
using distributed_cache::NodeRole;
using distributed_cache::ReplicationManager;
using distributed_cache::ReplicationMessage;
using distributed_cache::ReplicationPeer;

namespace {

class LoopbackPeer final : public ReplicationPeer {
public:
    explicit LoopbackPeer(std::shared_ptr<ReplicationManager> target)
        : ReplicationPeer("loopback", 0),
          target_(std::move(target))
    {
    }

    bool send(const ReplicationMessage& message) override
    {
        target_->applyReplicaMessage(message);
        markAcked(message.sequence);
        return true;
    }

private:
    std::shared_ptr<ReplicationManager> target_;
};

void waitForValue(CacheStore& cache, const std::string& key, const std::string& expected)
{
    for (int i = 0; i < 100; ++i) {
        const auto value = cache.get(key);
        if (value.has_value() && value.value() == expected) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    assert(false && "timed out waiting for replicated value");
}

void waitForMissing(CacheStore& cache, const std::string& key)
{
    for (int i = 0; i < 100; ++i) {
        if (!cache.get(key).has_value()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    assert(false && "timed out waiting for replicated delete");
}

void testLeaderPropagatesPut()
{
    auto leaderCache = std::make_shared<CacheStore>(10);
    auto followerCache = std::make_shared<CacheStore>(10);
    auto leader = std::make_shared<ReplicationManager>(NodeRole::Leader, leaderCache, 2);
    auto follower = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCache, 1);

    leader->addPeer(std::make_shared<LoopbackPeer>(follower));
    assert(leaderCache->put("alpha", "one"));
    leader->replicatePut("alpha", "one");

    waitForValue(*followerCache, "alpha", "one");
}

void testLeaderPropagatesDelete()
{
    auto leaderCache = std::make_shared<CacheStore>(10);
    auto followerCache = std::make_shared<CacheStore>(10);
    auto leader = std::make_shared<ReplicationManager>(NodeRole::Leader, leaderCache, 2);
    auto follower = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCache, 1);

    leader->addPeer(std::make_shared<LoopbackPeer>(follower));
    assert(leaderCache->put("alpha", "one"));
    leader->replicatePut("alpha", "one");
    waitForValue(*followerCache, "alpha", "one");

    assert(leaderCache->remove("alpha"));
    leader->replicateDelete("alpha");
    waitForMissing(*followerCache, "alpha");
}

void testFollowerRejectsOriginatedWrites()
{
    auto followerCache = std::make_shared<CacheStore>(10);
    ReplicationManager follower(NodeRole::Follower, followerCache, 1);

    bool threw = false;
    try {
        follower.replicatePut("alpha", "one");
    } catch (const std::logic_error&) {
        threw = true;
    }
    assert(threw);
}

void testFollowerServesReads()
{
    auto followerCache = std::make_shared<CacheStore>(10);
    ReplicationManager follower(NodeRole::Follower, followerCache, 1);
    follower.applyReplicaMessage({distributed_cache::ReplicationOperation::Put,
                                  1,
                                  "alpha",
                                  "one",
                                  ReplicationMessage::nowMillis()});

    assert(followerCache->get("alpha").value() == "one");
    assert(followerCache->exists("alpha"));
    assert(followerCache->size() == 1);
}

void testReconnectReplayRecovery()
{
    auto leaderCache = std::make_shared<CacheStore>(10);
    auto followerCache = std::make_shared<CacheStore>(10);
    ReplicationManager leader(NodeRole::Leader, leaderCache, 1);
    ReplicationManager follower(NodeRole::Follower, followerCache, 1);

    leaderCache->put("a", "1");
    leader.replicatePut("a", "1");
    leaderCache->put("b", "2");
    leader.replicatePut("b", "2");
    leaderCache->remove("a");
    leader.replicateDelete("a");

    for (const auto& message : leader.entriesAfter(0)) {
        follower.applyReplicaMessage(message);
    }

    assert(!followerCache->get("a").has_value());
    assert(followerCache->get("b").value() == "2");
    assert(follower.lastSequence() == leader.lastSequence());
}

void testReplicationOrdering()
{
    auto cache = std::make_shared<CacheStore>(10);
    ReplicationManager follower(NodeRole::Follower, cache, 1);

    follower.applyReplicaMessage({distributed_cache::ReplicationOperation::Put,
                                  1,
                                  "key",
                                  "first",
                                  ReplicationMessage::nowMillis()});
    follower.applyReplicaMessage({distributed_cache::ReplicationOperation::Put,
                                  2,
                                  "key",
                                  "second",
                                  ReplicationMessage::nowMillis()});
    follower.applyReplicaMessage({distributed_cache::ReplicationOperation::Delete,
                                  3,
                                  "key",
                                  "",
                                  ReplicationMessage::nowMillis()});

    assert(!cache->get("key").has_value());
    assert(follower.lastSequence() == 3);
}

void testMultipleFollowersReceiveUpdates()
{
    auto leaderCache = std::make_shared<CacheStore>(10);
    auto followerCacheA = std::make_shared<CacheStore>(10);
    auto followerCacheB = std::make_shared<CacheStore>(10);
    auto leader = std::make_shared<ReplicationManager>(NodeRole::Leader, leaderCache, 2);
    auto followerA = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCacheA, 1);
    auto followerB = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCacheB, 1);

    leader->addPeer(std::make_shared<LoopbackPeer>(followerA));
    leader->addPeer(std::make_shared<LoopbackPeer>(followerB));
    leaderCache->put("shared", "value");
    leader->replicatePut("shared", "value");

    waitForValue(*followerCacheA, "shared", "value");
    waitForValue(*followerCacheB, "shared", "value");
}

} // namespace

int main()
{
    testLeaderPropagatesPut();
    testLeaderPropagatesDelete();
    testFollowerRejectsOriginatedWrites();
    testFollowerServesReads();
    testReconnectReplayRecovery();
    testReplicationOrdering();
    testMultipleFollowersReceiveUpdates();

    std::cout << "All replication tests passed." << '\n';
    return 0;
}
