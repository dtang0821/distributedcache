# Performance

This page summarizes local benchmark methodology and the smoke-test results gathered during development. The numbers are useful for comparing subsystem costs, not for making production throughput claims.

## Environment Notes

Benchmarks were run locally through direct `g++ -std=c++17` builds on the development machine. `cmake` was not available on PATH in this environment, so the same source files were compiled manually.

All results should be interpreted as directional:

- No dedicated benchmark host.
- No CPU pinning.
- No release-profile tuning beyond compiler defaults used in the manual command.
- TCP benchmark coverage is limited to local smoke tests; most subsystem benchmarks use in-process loopback where appropriate.

## Methodology

| Benchmark | Purpose | Workload |
| --- | --- | --- |
| Cache | Measure local cache operation cost | Random PUT/GET workload |
| Replication | Measure async replication overhead | Leader writes to loopback followers |
| Sharding | Measure routing and rebalance cost | Writes through `KeyRouter`, then add a shard |
| Persistence | Measure AOF and recovery overhead | Persistent writes, snapshot, recovery replay |

Latency is measured per operation and reported as p50 and p99.

## Results

### Cache Benchmark

Command:

```sh
cache_benchmark 10000 50000
```

| Metric | Result |
| --- | ---: |
| Operations | 50,000 |
| Cache capacity | 10,000 |
| Workload | 40% PUT / 60% GET |
| Throughput | ~1,054,701 ops/sec |
| p50 latency | 0.60 us |
| p99 latency | 1.90 us |

The local cache path is fast because it is in-memory and uses O(1) average lookup plus O(1) LRU updates.

### Replication Benchmark

Command:

```sh
replication_benchmark 5000 2 10000
```

| Metric | Result |
| --- | ---: |
| Operations | 5,000 |
| Followers | 2 |
| Caught up | 2/2 |
| Throughput | ~101,583 replicated writes/sec |
| p50 latency | 2.50 us |
| p99 latency | 65.80 us |
| Replication lag | 0 followers behind |

This benchmark uses loopback peers to exercise the real replication manager and apply path without relying on multi-process scheduling.

### Sharding Benchmark

Command:

```sh
sharding_benchmark 10000 4 64
```

| Metric | Result |
| --- | ---: |
| Operations | 10,000 |
| Initial shards | 4 |
| Virtual nodes per shard | 64 |
| Routing throughput | ~365,752 writes/sec |
| p50 latency | 1.80 us |
| p99 latency | 6.20 us |
| Rebalance moved | 2,058 keys |
| Rebalance speed | ~148,887 keys/sec |
| Distribution min | 1,877 keys |
| Distribution max | 2,169 keys |

Adding a fifth shard moved about 20% of keys in this run, which is the intended behavior for consistent hashing. Modulo hashing would typically remap a much larger fraction.

### Persistence Benchmark

Command:

```sh
persistence_benchmark 5000 10000
```

| Metric | Result |
| --- | ---: |
| Operations | 5,000 |
| Recovered entries | 5,000 |
| Throughput | ~8,806 writes/sec |
| p50 latency | 87.70 us |
| p99 latency | 309.60 us |
| Flush latency | 51.10 us |
| Replay duration | 9.90 ms |
| Recovery startup | 24.04 ms |
| Snapshot duration | 24.51 ms |

Persistence is the most expensive path because every mutation is serialized to disk-oriented structures.

## Optimization Evolution

The project deliberately layers costs:

1. Phase 1 establishes an O(1) in-memory cache.
2. Phase 2 adds parsing and thread scheduling.
3. Phase 3 adds replication log append and follower propagation.
4. Phase 4 adds hash-ring lookup and ownership checks.
5. Phase 5 adds AOF serialization and recovery state.

This makes bottlenecks easier to explain and profile.

## Current Bottlenecks

- `CacheStore` uses a single mutex. Correct and simple, but it limits write-heavy concurrency.
- Client sessions read sockets byte-by-byte for clarity.
- Persistence uses portable C++ streams rather than platform-specific durable fsync calls.
- Rebalancing scans shard snapshots synchronously.
- Replication peer sends are simple TCP operations rather than long-lived multiplexed streams.

## Future Profiling Opportunities

- Replace coarse cache locking with segmented locks.
- Batch AOF writes and flushes more aggressively.
- Add persistent replication connections.
- Add chunked rebalancing with backpressure.
- Use platform-specific file sync APIs for stronger durability measurements.
- Add structured metrics export and long-running load tests.
