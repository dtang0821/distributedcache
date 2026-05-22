#include "cache/cachestore.hpp"
#include "persistence/persistence_manager.hpp"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

using distributed_cache::CacheStore;
using distributed_cache::FsyncPolicy;
using distributed_cache::PersistenceConfig;
using distributed_cache::PersistenceManager;

namespace {

std::filesystem::path tempDir(const std::string& name)
{
    const auto path = std::filesystem::current_path() / "build_manual" / ("persistence_" + name);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

PersistenceConfig configFor(const std::filesystem::path& dir, FsyncPolicy policy = FsyncPolicy::Never)
{
    PersistenceConfig config;
    config.directory = dir.string();
    config.fsyncPolicy = policy;
    config.backgroundSnapshots = false;
    config.snapshotInterval = std::chrono::seconds(1);
    return config;
}

void testAppendOnlyLogging()
{
    const auto dir = tempDir("aof");
    auto cache = std::make_shared<CacheStore>(10);
    PersistenceManager persistence(cache, configFor(dir));

    assert(persistence.put("alpha", "one"));
    assert(persistence.remove("alpha"));
    persistence.appendOnlyLog().flush();

    const auto entries = persistence.appendOnlyLog().replayAfter(0);
    assert(entries.size() == 2);
    assert(entries[0].key == "alpha");
    assert(entries[1].key == "alpha");
}

void testSnapshotCreation()
{
    const auto dir = tempDir("snapshot");
    auto cache = std::make_shared<CacheStore>(10);
    PersistenceManager persistence(cache, configFor(dir));
    assert(persistence.put("alpha", "one"));

    const auto metadata = persistence.createSnapshot();
    assert(std::filesystem::exists(metadata.filePath));
    assert(metadata.lastAppliedSequence == 1);
}

void testStartupRecovery()
{
    const auto dir = tempDir("startup");
    {
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir));
        assert(persistence.put("alpha", "one"));
        assert(persistence.put("beta", "two"));
        persistence.appendOnlyLog().flush();
    }

    auto recoveredCache = std::make_shared<CacheStore>(10);
    PersistenceManager recovered(recoveredCache, configFor(dir));
    const auto metrics = recovered.recover();
    assert(metrics.entriesReplayed == 2);
    assert(recoveredCache->get("alpha").value() == "one");
    assert(recoveredCache->get("beta").value() == "two");
}

void testReplayReconstruction()
{
    const auto dir = tempDir("replay");
    {
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir));
        assert(persistence.put("alpha", "one"));
        assert(persistence.put("alpha", "two"));
        assert(persistence.remove("alpha"));
        assert(persistence.put("beta", "three"));
        persistence.appendOnlyLog().flush();
    }

    auto recoveredCache = std::make_shared<CacheStore>(10);
    PersistenceManager recovered(recoveredCache, configFor(dir));
    recovered.recover();
    assert(!recoveredCache->get("alpha").has_value());
    assert(recoveredCache->get("beta").value() == "three");
}

void testCrashRecoverySimulation()
{
    const auto dir = tempDir("crash");
    {
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir, FsyncPolicy::Always));
        assert(persistence.put("before-crash", "safe"));
    }

    auto recoveredCache = std::make_shared<CacheStore>(10);
    PersistenceManager recovered(recoveredCache, configFor(dir, FsyncPolicy::Always));
    recovered.recover();
    assert(recoveredCache->get("before-crash").value() == "safe");
}

void testTtlRecoveryCorrectness()
{
    using namespace std::chrono_literals;

    const auto dir = tempDir("ttl");
    {
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir));
        assert(persistence.put("short", "lived", 80ms));
        persistence.appendOnlyLog().flush();
    }

    std::this_thread::sleep_for(120ms);

    auto recoveredCache = std::make_shared<CacheStore>(10);
    PersistenceManager recovered(recoveredCache, configFor(dir));
    recovered.recover();
    assert(!recoveredCache->get("short").has_value());
}

void testPersistenceDurabilityModes()
{
    for (const auto policy : {FsyncPolicy::Always, FsyncPolicy::EverySecond, FsyncPolicy::Never}) {
        const auto dir = tempDir("policy_" + std::to_string(static_cast<int>(policy)));
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir, policy));
        assert(persistence.put("mode", "ok"));
        persistence.appendOnlyLog().flush();
        assert(persistence.metrics().logSizeBytes > 0);
    }
}

void testSnapshotAndReplayCombinedRecovery()
{
    const auto dir = tempDir("combined");
    {
        auto cache = std::make_shared<CacheStore>(10);
        PersistenceManager persistence(cache, configFor(dir));
        assert(persistence.put("snapshotted", "old"));
        persistence.createSnapshot();
        assert(persistence.put("after", "new"));
        assert(persistence.remove("snapshotted"));
        persistence.appendOnlyLog().flush();
    }

    auto recoveredCache = std::make_shared<CacheStore>(10);
    PersistenceManager recovered(recoveredCache, configFor(dir));
    recovered.recover();
    assert(!recoveredCache->get("snapshotted").has_value());
    assert(recoveredCache->get("after").value() == "new");
}

} // namespace

int main()
{
    testAppendOnlyLogging();
    testSnapshotCreation();
    testStartupRecovery();
    testReplayReconstruction();
    testCrashRecoverySimulation();
    testTtlRecoveryCorrectness();
    testPersistenceDurabilityModes();
    testSnapshotAndReplayCombinedRecovery();

    std::cout << "All persistence tests passed." << '\n';
    return 0;
}
