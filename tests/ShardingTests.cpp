#include "cache/cachestore.hpp"
#include "replication/replication_manager.hpp"
#include "replication/replication_peer.hpp"
#include "sharding/key_router.hpp"
#include "sharding/rebalance_manager.hpp"
#include "sharding/shard_manager.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using distributed_cache::CacheStore;
using distributed_cache::KeyRouter;
using distributed_cache::NodeRole;
using distributed_cache::RebalanceManager;
using distributed_cache::ReplicationManager;
using distributed_cache::ReplicationMessage;
using distributed_cache::ReplicationPeer;
using distributed_cache::ShardManager;
using distributed_cache::ShardNode;

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

std::shared_ptr<ShardNode> makeShard(const std::string& nodeId, bool leader = true)
{
    auto cache = std::make_shared<CacheStore>(10000);
    auto manager = std::make_shared<ReplicationManager>(
        leader ? NodeRole::Leader : NodeRole::Follower, cache, 2);

    auto node = std::make_shared<ShardNode>();
    node->nodeId = nodeId;
    node->leader = leader;
    node->cache = cache;
    node->replicationManager = manager;
    return node;
}

std::shared_ptr<ShardManager> makeManager(std::size_t virtualNodes = 32)
{
    auto manager = std::make_shared<ShardManager>(virtualNodes);
    manager->addNode(makeShard("ShardA"));
    manager->addNode(makeShard("ShardB"));
    manager->addNode(makeShard("ShardC"));
    return manager;
}

std::vector<std::string> keys(std::size_t count)
{
    std::vector<std::string> result;
    result.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        result.push_back("key:" + std::to_string(i));
    }
    return result;
}

void waitForValue(CacheStore& cache, const std::string& key, const std::string& expected)
{
    for (int i = 0; i < 100; ++i) {
        const auto value = cache.get(key);
        if (value.has_value() && value.value() == expected) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    assert(false && "timed out waiting for replicated shard value");
}

void testConsistentHashLookup()
{
    distributed_cache::ConsistentHashRing ring(8);
    assert(!ring.ownerForKey("alpha").has_value());

    ring.addNode("ShardA");
    ring.addNode("ShardB");
    assert(ring.ringSize() == 16);
    assert(ring.ownerForKey("alpha").has_value());
    assert(ring.ownerForKey("alpha").value() == ring.ownerForKey("alpha").value());
}

void testKeyRoutingCorrectness()
{
    auto manager = makeManager();
    KeyRouter router(manager);

    assert(router.put("user123", "data"));
    const auto owner = router.ownerForKey("user123");
    assert(owner.has_value());
    auto node = manager->nodeById(owner.value());
    assert(node->cache->get("user123").value() == "data");

    for (const auto& other : manager->nodes()) {
        if (other->nodeId != owner.value()) {
            assert(!other->cache->get("user123").has_value());
        }
    }
}

void testVirtualNodeDistribution()
{
    auto manager = makeManager(64);
    std::unordered_map<std::string, std::size_t> counts;
    for (const auto& key : keys(3000)) {
        ++counts[manager->ring().ownerForKey(key).value()];
    }

    assert(counts.size() == 3);
    for (const auto& pair : counts) {
        assert(pair.second > 600);
        assert(pair.second < 1500);
    }
}

void testAddingNodeCausesMinimalMovement()
{
    auto manager = makeManager(64);
    std::unordered_map<std::string, std::string> before;
    const auto sampleKeys = keys(2000);
    for (const auto& key : sampleKeys) {
        before[key] = manager->ring().ownerForKey(key).value();
    }

    manager->addNode(makeShard("ShardD"));

    std::size_t moved = 0;
    for (const auto& key : sampleKeys) {
        if (before[key] != manager->ring().ownerForKey(key).value()) {
            ++moved;
        }
    }

    assert(moved > 0);
    assert(moved < sampleKeys.size() / 2);
}

void testRemovingNodeRebalancesCorrectly()
{
    auto manager = makeManager(32);
    KeyRouter router(manager);
    for (const auto& key : keys(250)) {
        assert(router.put(key, "value:" + key));
    }

    auto removed = manager->removeNode("ShardB");
    assert(removed);

    RebalanceManager rebalancer(manager);
    const auto stats = rebalancer.rebalanceRemovedNode(removed);
    assert(stats.keysScanned > 0);
    assert(removed->cache->size() == 0);

    for (const auto& key : keys(250)) {
        assert(router.get(key).has_value());
        assert(manager->ownsKey(router.ownerForKey(key).value(), key));
    }
}

void testOwnershipEnforcement()
{
    auto manager = makeManager();
    KeyRouter router(manager);
    assert(router.put("owned-key", "value"));

    const auto owner = router.ownerForKey("owned-key").value();
    for (const auto& node : manager->nodes()) {
        if (node->nodeId != owner) {
            assert(!manager->ownsKey(node->nodeId, "owned-key"));
            assert(!node->cache->get("owned-key").has_value());
        }
    }
}

void testShardLeaderWriteRouting()
{
    auto manager = std::make_shared<ShardManager>(16);
    manager->addNode(makeShard("LeaderShard", true));
    auto followerOnly = makeShard("FollowerShard", false);
    manager->addNode(followerOnly);

    KeyRouter router(manager);
    bool sawFollowerOwnedKey = false;
    for (const auto& key : keys(500)) {
        if (router.ownerForKey(key).value() == "FollowerShard") {
            sawFollowerOwnedKey = true;
            assert(!router.put(key, "blocked"));
            assert(!followerOnly->cache->get(key).has_value());
            break;
        }
    }
    assert(sawFollowerOwnedKey);
}

void testMultiShardReplication()
{
    auto manager = std::make_shared<ShardManager>(16);
    auto shardA = makeShard("ShardA", true);
    auto shardB = makeShard("ShardB", true);
    auto followerCacheA = std::make_shared<CacheStore>(1000);
    auto followerA = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCacheA, 1);
    auto followerCacheB = std::make_shared<CacheStore>(1000);
    auto followerB = std::make_shared<ReplicationManager>(NodeRole::Follower, followerCacheB, 1);

    shardA->replicationManager->addPeer(std::make_shared<LoopbackPeer>(followerA));
    shardB->replicationManager->addPeer(std::make_shared<LoopbackPeer>(followerB));
    manager->addNode(shardA);
    manager->addNode(shardB);

    KeyRouter router(manager);
    std::string keyA;
    std::string keyB;
    for (const auto& key : keys(1000)) {
        const auto owner = router.ownerForKey(key).value();
        if (owner == "ShardA" && keyA.empty()) {
            keyA = key;
        }
        if (owner == "ShardB" && keyB.empty()) {
            keyB = key;
        }
        if (!keyA.empty() && !keyB.empty()) {
            break;
        }
    }

    assert(!keyA.empty());
    assert(!keyB.empty());
    assert(router.put(keyA, "value-a"));
    assert(router.put(keyB, "value-b"));

    waitForValue(*followerCacheA, keyA, "value-a");
    waitForValue(*followerCacheB, keyB, "value-b");
}

} // namespace

int main()
{
    testConsistentHashLookup();
    testKeyRoutingCorrectness();
    testVirtualNodeDistribution();
    testAddingNodeCausesMinimalMovement();
    testRemovingNodeRebalancesCorrectly();
    testOwnershipEnforcement();
    testShardLeaderWriteRouting();
    testMultiShardReplication();

    std::cout << "All sharding tests passed." << '\n';
    return 0;
}
