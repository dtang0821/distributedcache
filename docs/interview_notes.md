# Interview Notes

This document frames the project in the style of systems-design interview discussion. Each subsystem is described by problem, architecture, tradeoffs, bottlenecks, and future improvements.

## Networking

**Problem:** Accept concurrent client requests over TCP without blocking the whole cache.

**Architecture:** A `TcpServer` accepts sockets and submits `ClientSession` work to a fixed `ThreadPool`. `CommandParser` converts newline-delimited commands into cache operations.

**Tradeoffs:** A thread pool avoids unbounded thread creation. The current parser and session loop are simple rather than highly optimized.

**Bottlenecks:** Socket reads are byte-oriented, and the thread pool can saturate under heavy client load.

**Future improvements:** Buffered reads, connection limits, backpressure, async IO, and protocol pipelining.

## Concurrency

**Problem:** Keep cache operations correct under concurrent access.

**Architecture:** `CacheStore` uses a mutex around operations because `GET` mutates LRU state.

**Tradeoffs:** Coarse locking is safe and easy to audit. It limits write-heavy scalability.

**Bottlenecks:** One hot cache mutex can become the primary bottleneck.

**Future improvements:** Segmented locking, shard-local locks, read/write locks for read paths that do not update LRU, or lock striping.

## Replication

**Problem:** Keep follower nodes updated from a leader.

**Architecture:** The leader appends a replication log entry and asynchronously sends `REPL_PUT` or `REPL_DELETE` to followers. Followers apply messages locally and can request missed entries with `SYNC_FROM`.

**Tradeoffs:** Async replication gives low write latency but only eventual consistency.

**Bottlenecks:** Simple peer sends and follower lag under load.

**Future improvements:** Persistent replication streams, batching, acknowledgements with lag metrics, and failover.

## Sharding

**Problem:** Scale beyond a single cache node by partitioning keys.

**Architecture:** `KeyRouter` asks `ConsistentHashRing` for the owner of each key. Writes go to shard leaders. Rebalancing moves keys whose owner changed.

**Tradeoffs:** Sharding improves horizontal scale but introduces topology management and key movement.

**Bottlenecks:** Synchronous rebalancing and local topology assumptions.

**Future improvements:** Chunked migration, topology epochs, ownership handoff protocols, and read routing to replicas.

## Consistent Hashing

**Problem:** Avoid remapping most keys when shard count changes.

**Architecture:** Keys and virtual nodes share a sorted hash ring. A key belongs to the first vnode clockwise from its hash.

**Tradeoffs:** Greatly reduces redistribution compared with modulo hashing, but distribution is still probabilistic.

**Bottlenecks:** Ring updates and large vnode counts add metadata overhead.

**Future improvements:** Weighted nodes, bounded-load hashing, and better movement analysis.

## Persistence

**Problem:** Recover cache state after process restart or crash.

**Architecture:** `PersistenceManager` records mutations in an append-only log and periodically writes snapshots. Recovery loads the snapshot and replays later AOF entries.

**Tradeoffs:** AOF improves durability. Snapshots reduce replay time. The current implementation is node-local and uses portable C++ streams.

**Bottlenecks:** File serialization and flush cost dominate write latency.

**Future improvements:** Log compaction, binary encoding, platform-specific fsync, snapshot compression, and incremental snapshots.

## Recovery

**Problem:** Rebuild in-memory data structures accurately.

**Architecture:** Recovery clears memory, loads snapshot entries, then replays ordered log entries. TTLs are aged so expired records are not resurrected.

**Tradeoffs:** Replay is simple and deterministic. Recovery time grows with AOF length after the last snapshot.

**Bottlenecks:** Large logs and snapshots.

**Future improvements:** Snapshot indexing, parallel decode, bounded AOF retention, and startup progress metrics.

## Likely Interview Questions

### Why async replication?

It keeps leader write latency low by not waiting for every follower. The cost is eventual consistency: followers can lag and serve stale reads.

### Why consistent hashing instead of modulo hashing?

Modulo hashing remaps a large fraction of keys when nodes are added or removed. Consistent hashing moves only keys in affected ring ranges.

### What happens during shard rebalance?

The topology changes, the ring recomputes ownership, and the rebalancer scans existing keys. Only keys whose owner changed are moved to the new owner.

### Why snapshots and AOF together?

Snapshots make recovery fast by checkpointing full state. AOF preserves writes after the snapshot. Recovery loads the snapshot and replays only later entries.

### What consistency guarantees exist?

The cache is eventually consistent across leader/follower replicas. Within one node, operations are mutex-protected. There is no linearizable distributed write guarantee.

### What are the bottlenecks?

The cache mutex, byte-wise socket reads, simple replication sends, synchronous rebalancing, and persistence serialization/flushes.

### How would you improve durability?

Use platform-specific fsync, checksums, log segmenting, compaction, crash-safe metadata, atomic snapshot manifests, and corruption handling.

### How would you implement failover?

Introduce membership, health checks, leader election, and a consensus protocol such as Raft for agreeing on leadership and committed replication state.

### Why not implement Raft now?

Raft would shift the project focus from cache architecture to consensus. It is a strong future phase, but this repository first demonstrates networking, replication, sharding, and recovery fundamentals.

### How would you handle cross-shard transactions?

They are intentionally excluded. A future implementation would need transaction coordination, two-phase commit or a consensus-backed transaction layer, and clear failure semantics.
