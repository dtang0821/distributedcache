# Design Tradeoffs

This project intentionally favors clear distributed systems architecture over maximal feature count. The goal is to be production-inspired and interview-explainable without hiding complexity behind frameworks.

## Why Thread Pools?

Thread-per-connection is simple but scales poorly when many clients connect. A fixed thread pool gives the server a predictable concurrency ceiling and avoids unbounded thread creation.

Tradeoff: a small pool can become saturated. A production system would add backpressure, queue limits, and richer metrics.

## Why Leader/Follower Replication?

Leader/follower replication is easier to reason about than multi-leader replication. Writes go to one leader per shard, then followers apply ordered updates.

Tradeoff: the leader is a write bottleneck and a failure point until failover is implemented.

## Why Asynchronous Replication?

Asynchronous replication keeps write latency low because the leader does not wait for every follower before responding.

Tradeoff: followers can lag and serve stale reads. The consistency model is eventual consistency, not linearizability.

## Why No Consensus?

Consensus algorithms such as Raft and Paxos solve leader election and replicated agreement, but they would dominate the project. This repository focuses first on cache internals, networking, replication mechanics, sharding, and durability.

Tradeoff: topology changes and leader choice are explicit configuration, not automatically agreed by a cluster.

## Why Consistent Hashing?

`hash(key) % N` remaps many keys when `N` changes. Consistent hashing maps keys and nodes onto a ring so adding or removing a node moves only nearby key ranges.

Tradeoff: distribution is approximate. Virtual nodes improve balance but do not eliminate all skew.

## Why Virtual Nodes?

A physical shard appears at many positions on the hash ring. This smooths key distribution and reduces the chance that one shard owns a disproportionate range.

Tradeoff: more virtual nodes increase ring size and topology bookkeeping.

## Why Append-Only Logs?

Append-only logs are simple, sequential, and replayable. Every mutation becomes a durable record that can rebuild memory after a crash.

Tradeoff: logs grow over time and need compaction or snapshots.

## Why Snapshots Plus AOF?

AOF alone can become slow to replay. Snapshots alone can lose recent writes. Combining them gives a practical recovery model:

1. Load a recent snapshot.
2. Replay only later AOF entries.

Tradeoff: snapshots must safely serialize concurrent state and introduce storage overhead.

## Why Lazy TTL Expiration?

Lazy expiration removes expired entries when they are accessed or during selected cleanup points. It avoids a constantly running expiration scanner in the core cache.

Tradeoff: expired entries can occupy memory until touched or cleaned.

## Why Simplified Rebalancing?

The rebalancer scans keys and moves only those whose ownership changed. This demonstrates the core idea without distributed locks or transactional migration.

Tradeoff: migrations are best-effort and synchronous. A production system would add chunking, retries, ownership epochs, and progress tracking.

## Limitations

- No consensus or automatic leader election.
- No distributed transactions.
- No cross-shard atomic operations.
- Eventual consistency for followers.
- Simplified rebalancing.
- Node-local persistence only.
- No service discovery.
- No advanced failover.

These exclusions are intentional. They keep each subsystem understandable and make future phases easier to discuss.
