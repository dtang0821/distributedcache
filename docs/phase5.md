# Phase 5 Scope

Phase 5 adds node-local durability and crash recovery. It is inspired by Redis AOF plus snapshot persistence: snapshots make startup faster, while append-only replay restores mutations after the latest snapshot.

This phase intentionally does not implement distributed consensus, transactional persistence across shards, cloud storage, filesystem databases, or WAL coordination between replicas.

## Write Path

```text
Client
  -> CacheStore
  -> AppendOnlyLog
  -> optional flush/fsync policy
```

Mutating operations are recorded as ordered AOF entries:

```text
PUT <seq> <timestamp> <ttl_ms> <key_len> <value_len> <key><value>
DELETE <seq> <timestamp> <key_len> <key>
```

The length-prefixed text format keeps the implementation easy to inspect while still allowing spaces inside keys or values.

## Recovery Path

```text
Snapshot
  -> Replay AOF entries after snapshot sequence
  -> Rebuild CacheStore
```

Startup recovery:

1. Load the latest snapshot if present.
2. Read its last applied AOF sequence.
3. Replay append-only log entries after that sequence.
4. Rebuild key/value state and TTL metadata.

Expired TTL entries are not resurrected. Recovery ages TTLs using the original write timestamp or snapshot timestamp.

## Snapshots

Snapshots serialize the full live cache:

```text
SNAPSHOT <timestamp> <last_sequence> <entry_count>
ENTRY <ttl_ms_remaining> <key_len> <value_len> <key><value>
```

Snapshots are compact compared with replaying every historical write. They are written through a temporary file and renamed into place after the write completes.

## Background Snapshotting

`PersistenceManager` can run a background snapshot thread at a configurable interval. It uses `CacheStore::snapshotEntries()` so request handling can continue with ordinary cache locking.

This phase does not implement copy-on-write fork snapshots or incremental snapshots. Those are useful production techniques, but the current goal is understandable crash recovery.

## Durability Policies

Three fsync-style policies are supported:

- `always`: flush every write, safest and slowest.
- `every-second`: flush at most once per second, balanced.
- `never`: rely on OS flushing, fastest and weakest.

The implementation uses standard C++ stream flushing as the portable durability boundary for this educational phase.

## Startup Flags

```sh
distributed_cache_app --data-dir data/node-a --fsync every-second --snapshot-interval 30
```

Persistence is enabled by default. It can be disabled for volatile test nodes:

```sh
distributed_cache_app --no-persistence
```

## Metrics

Persistence metrics track:

- log size
- replay latency
- snapshot latency
- flush latency
- recovery latency
- replayed entry count

## Tradeoffs

This is a node-local durability layer. In a sharded replicated cluster, each node persists its local cache state. Later phases can add durable replication coordination, bounded log compaction, and snapshot shipping, but those are intentionally outside Phase 5.
