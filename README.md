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

## Design decisions worth being able to explain

**Why asynchronous (fire-and-forget) replication instead of synchronous?**
The leader commits locally and replies `OK` *without* waiting for followers
to confirm receipt. This keeps write latency low (it doesn't depend on the
slowest follower or the network), at the cost of a small window where a
follower's data can lag behind the leader ("replication lag"). The
alternative — synchronous replication, where the leader waits for an
acknowledgment from one or more followers before confirming the write —
trades latency for durability: if the leader crashes right after committing
but before a follower acks, an async system can lose that write; a
synchronous one (with enough acks) generally won't.

**Why a lock instead of something fancier?**
A single `threading.Lock` around the dict is the simplest tool that
correctly prevents two threads from reading/writing the store at the same
time. This is the same idea as the mutexes/semaphores from the OS labs —
just guarding a different resource (a shared dict instead of a shared
buffer).

**Why TCP instead of UDP?**
Replication needs every write to arrive, in order, exactly once. TCP gives
ordered, reliable delivery for free; UDP would require building that
guarantee manually, which isn't worth it for this scope.

## Known limitations (good to volunteer in an interview)

- **No leader election / failover.** If the leader goes down, the system
  doesn't automatically promote a follower — this is what tools like Raft
  or Paxos solve, and it's intentionally out of scope here.
- **No persistence.** Everything lives in memory; a process restart loses
  the data.
- **No conflict resolution.** Since only the leader accepts writes, there's
  no concurrent-write conflict to resolve — a deliberate simplification.

## Likely interview questions (with the short answer)

- *"What happens if a follower disconnects?"* The leader detects the closed
  socket, drops it from its follower list, and keeps serving other clients;
  the follower's replication loop retries the connection every 2 seconds.
- *"What happens if the leader crashes?"* Writes stop being accepted; reads
  on followers still work but may be stale. There's no automatic failover —
  a real system would need leader election here.
- *"How would you make this strongly consistent?"* Make replication
  synchronous: don't reply `OK` to the client until at least one (or a
  quorum of) followers has acknowledged the write.
- *"How would you scale this to many followers?"* The current design is
  fine for a handful; at scale you'd want followers fanning out from each
  other (a tree) instead of all connecting directly to one leader, to avoid
  the leader becoming a bottleneck.
