# Phase 3 Scope

Phase 3 introduces distributed replication for the first time. It uses a simple leader/follower model with asynchronous propagation, an in-memory append-only replication log, and reconnect replay through `SYNC_FROM`.

It intentionally does not implement Raft, Paxos, leader election, quorum writes, sharding, or snapshots.

## Architecture

```text
Client
  -> Leader Cache Node
  -> ReplicationManager
  -> ReplicationLog
  -> Follower ReplicationServer
  -> Follower CacheStore
```

The leader accepts client writes. Followers are read-only for client traffic and apply writes only when received through the replication protocol.

## Replication Flow

```text
PUT key value
  -> leader updates local CacheStore
  -> leader appends REPL_PUT to ReplicationLog
  -> leader asynchronously sends message to followers
  -> followers apply update locally
```

Deletes follow the same path with `REPL_DELETE`.

## Protocol

Replication messages are newline-delimited text:

```text
REPL_PUT <sequence> <key> <value> <timestamp_millis>
REPL_DELETE <sequence> <key> <timestamp_millis>
SYNC_FROM <sequence>
END <last_sequence>
ACK <sequence>
```

`SYNC_FROM` lets a follower request all log entries after the last sequence it has applied.

## Consistency Model

Replication is asynchronous and eventually consistent. A write can be acknowledged by the leader before every follower has applied it. Followers may briefly serve stale reads during propagation delay or while catching up after a disconnect.

This is deliberate for Phase 3: the goal is to understand replication mechanics before adding stronger coordination.

## Reconnect Recovery

The leader keeps an in-memory append-only log. A reconnecting follower calls `SYNC_FROM <last_sequence>` against the leader replication port and applies every returned entry in order.

This design is simple and educational. A later persistence phase can add durable logs and snapshots so recovery can survive process restarts and bounded log retention.

## Startup Examples

Leader with one configured follower replication endpoint:

```sh
distributed_cache_app --leader --port 6379 --replication-port 7379 --peer 127.0.0.1:7380
```

Follower that serves client reads on `6380` and listens for replication on `7380`:

```sh
distributed_cache_app --follower 127.0.0.1:7379 --port 6380 --replication-port 7380
```

## Future Phases

The next natural step is sharding with consistent hashing. Consensus, leader election, and quorum writes should remain separate later phases because they change the project from primary/replica replication into coordinated distributed state management.
