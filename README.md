# Distributed Key-Value Store (Leader-Follower Replication)

A minimal distributed key-value store built to apply two things from
coursework to a new problem: thread synchronization (OS labs) and socket
programming (CN labs).

## Architecture

```
            ┌────────────┐
  clients → │   LEADER   │ ── replicates every write ──┐
  (SET/GET) │ (port 9000)│                              │
            └────────────┘                              ▼
                                              ┌────────────────┐
                                              │   FOLLOWER A   │ ← clients (GET only)
                                              │  (port 9100)   │
                                              └────────────────┘
                                              ┌────────────────┐
                                              │   FOLLOWER B   │ ← clients (GET only)
                                              │  (port 9101)   │
                                              └────────────────┘
```

- The **leader** is the only node that accepts writes. After committing a
  write to its own in-memory store, it streams that write to every connected
  follower over a persistent TCP connection.
- **Followers** connect out to the leader on startup and apply every write
  they receive, in order, to their own copy. They serve `GET` directly from
  their local copy (a "read replica") and reject `SET`.
- Concurrency is handled with **one thread per connection** plus a
  `threading.Lock` around the shared dictionary, the same synchronization
  pattern as the producer-consumer / readers-writers OS labs — just applied
  across a network instead of within a single process.

## How to run it

```bash
# terminal 1
python3 leader.py

# terminal 2
python3 follower.py 9100

# terminal 3 (optional second follower)
python3 follower.py 9101

# terminal 4 — talk to the leader
python3 client.py 127.0.0.1 9000
> SET name alice
OK
> GET name
VALUE alice

# terminal 5 — read from a follower
python3 client.py 127.0.0.1 9100
> GET name
VALUE alice
> SET name bob
ERROR writes must go to the leader
```

## Design decisions

**Asynchronous replication.** The leader commits locally and replies `OK`
without waiting for followers to confirm receipt. This keeps write latency
low (it doesn't depend on the slowest follower or the network), at the cost
of a small window where a follower's data can lag behind the leader. A
synchronous alternative — waiting for one or more follower acknowledgments
before confirming the write — trades latency for durability.

**A single lock around the store.** A `threading.Lock` around the shared
dict is the simplest tool that correctly prevents two threads from
reading/writing it at the same time — the same idea as the mutexes and
semaphores used for the OS coursework problems, applied to a different
shared resource.

**TCP over UDP.** Replication needs every write to arrive, in order, exactly
once. TCP provides ordered, reliable delivery; UDP would require building
that guarantee manually.

## Known limitations

- No leader election or automatic failover if the leader goes down.
- No persistence — data lives in memory only and is lost on restart.
- No conflict resolution, since only the leader accepts writes.
