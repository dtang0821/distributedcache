# Phase 4 Scope

Phase 4 introduces horizontal scaling through sharding and consistent hashing. The project now has a routing layer that maps keys to shard leaders, while each shard can continue using the Phase 3 leader/follower replication model internally.

This phase intentionally does not implement Raft, Paxos, distributed transactions, cross-shard transactions, Kubernetes discovery, or multi-leader replication.

## Architecture

```text
Client
  -> KeyRouter
  -> ConsistentHashRing
     -> ShardA leader -> ShardA followers
     -> ShardB leader -> ShardB followers
     -> ShardC leader -> ShardC followers
```

Each shard owns a subset of keys. Writes route to the owning shard leader. Reads route to the owning shard in this implementation, with room for later read-replica routing.

## Why Not `hash(key) % N`

Modulo hashing is simple, but when `N` changes almost every key can map to a different shard. That makes adding or removing a node expensive because the cluster must move a large fraction of cached data.

Consistent hashing places both keys and shard nodes on the same ring. A key belongs to the first node clockwise from the key hash. When a node is added or removed, only keys in the neighboring ring ranges move.

## Hash Ring

```text
                 [ShardB vnode]
                       ^
                       |
 [ShardA vnode] <- key hash -> [ShardC vnode]
                       |
                       v
                 [ShardA vnode]
```

Lookup flow:

1. Hash the key to a `uint64_t` position.
2. Binary search the sorted ring for the first node clockwise.
3. Wrap to the beginning if the key is past the final ring position.

The implementation uses `std::map<uint64_t, NodeID>` for sorted ring ownership.

## Virtual Nodes

Each physical shard is inserted into the ring multiple times:

```text
ShardA-vnode-0
ShardA-vnode-1
ShardA-vnode-2
...
```

Virtual nodes smooth distribution and reduce hot partitions compared with placing each shard on the ring once. The virtual node count is configurable when constructing `ShardManager` or `ConsistentHashRing`.

## Ownership

`ShardManager` owns the cluster topology. `KeyRouter` asks the ring for the owner of every key before executing `PUT`, `GET`, or `DELETE`.

Writes require the target shard to be a leader. Follower-only shard nodes reject writes through the router. This keeps Phase 4 compatible with Phase 3's read-only follower model.

## Rebalancing

Rebalancing is synchronous and intentionally simple:

- On node addition, scan existing shard keys and move only keys whose new owner changed.
- On node removal, scan the removed node and move its keys to their new clockwise owners.

This is best-effort in-memory movement, not a transactional migration protocol. Later phases can add durable migration state, backpressure, and chunked movement.

## Node Addition

```text
Before:
  A ---- B ---- C

After adding D:
  A ---- D ---- B ---- C

Only keys in D's new ring ranges move to D.
```

## Node Removal

```text
Before:
  A ---- B ---- C

After removing B:
  A ---------- C

B's key ranges move to their clockwise successors.
```

## Tradeoffs

This phase demonstrates scalable partitioning and routing fundamentals. It favors explainability over perfect production behavior:

- No distributed locking
- No cross-shard transactions
- No automatic service discovery
- No consensus-backed topology changes
- No streaming migration protocol

Those are good later phases, but they would distract from the core consistent hashing model here.
