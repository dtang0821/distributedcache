# Phase 2 Scope

Phase 2 turns the local cache into a networked concurrent TCP cache server while still avoiding distributed consensus, replication, clustering, and sharding.

## Added Components

- `TcpServer`: owns the listening socket and accept loop
- `ClientSession`: processes commands from one connected client
- `ThreadPool`: fixed worker pool used for client-session execution
- `CommandParser`: parses the line-oriented TCP protocol

## TCP Protocol

Commands are newline-delimited text:

```text
PUT <key> <value>
GET <key>
DELETE <key>
EXISTS <key>
SIZE
QUIT
```

Responses are also newline-delimited:

```text
OK
VALUE <value>
NOT_FOUND
DELETED
YES
NO
SIZE <count>
ERROR <message>
```

## Architecture

```text
Client
  -> TCP Server accept loop
  -> fixed ThreadPool
  -> ClientSession
  -> shared CacheStore
```

The server intentionally uses a fixed thread pool rather than spawning an unbounded thread per connection.
