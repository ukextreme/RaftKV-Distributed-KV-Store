# RaftKV — Distributed Fault-Tolerant Key-Value Store

A replicated, crash-safe key-value store written from scratch in Rust. It implements
the Raft consensus protocol (leader election, log replication, commit safety), a
fsync-durable write-ahead log, a Redis-compatible wire protocol, and consistent-hash
sharding across independent Raft groups.

No consensus crate, no async runtime, no networking framework — the only dependencies
are `serde` and `serde_json` for state serialisation.

---

## What it does

Point a `redis-cli` at any node and it behaves like a small Redis:

```
$ redis-cli -p 6381
127.0.0.1:6381> SET name uday
OK
127.0.0.1:6381> GET name
"uday"
127.0.0.1:6381> DEL name
(integer) 1
```

Behind that, every write goes through Raft consensus, is replicated to a majority of
nodes, and is fsynced to a write-ahead log before it is acknowledged. Kill the leader
and the cluster elects a new one and keeps serving — with no data loss.

---

## Architecture

```
                 redis-cli / any RESP client
                            |
                            v
              +---------------------------+
              |   TCP server (RESP)       |   src/network/
              |   parse -> Command        |
              +---------------------------+
                            |
                            v
              +---------------------------+
              |   Router                  |   src/sharding/
              |   key -> consistent hash  |   64 vnodes per shard
              |   -> shard  (or MOVED)    |   wrong shard => MOVED error
              +---------------------------+
                            |
                            v
              +---------------------------+
              |   ClusterNode             |   src/cluster.rs
              |   writes -> Raft          |
              |   reads  -> state machine |
              +---------------------------+
                     |              |
                     v              v
        +------------------+   +------------------+
        |  Raft node       |   |  Storage engine  |
        |  election        |   |  WAL (fsync)     |
        |  log replication |   |  BTreeMap memtbl |
        |  commit index    |   |  replay recovery |
        |  persistent term |   |  tombstones      |
        +------------------+   +------------------+
                     |
                     v
        +------------------------------+
        |  Peer transport (TCP)        |   src/transport.rs
        |  RequestVote / AppendEntries |
        +------------------------------+
```

### Raft (`src/raft/`)

| File | Responsibility |
|---|---|
| `node.rs` | The consensus state machine — follower/candidate/leader roles, election, replication, commit advance |
| `log.rs` | The replicated log: append, truncate-on-conflict, term lookup by index |
| `state.rs` | Volatile and persistent Raft state (`current_term`, `voted_for`, `commit_index`, `last_applied`) |
| `persist.rs` | Durable term and vote — survives a crash so the node cannot vote twice in one term |
| `message.rs` | `RequestVote` / `AppendEntries` RPCs and their replies |

The Raft node is written as a **pure actor**: it takes messages and a `tick()`, and
returns messages to send. It performs no I/O of its own. That means elections, split
votes, log divergence and commit-safety are all exercised in ordinary unit tests with
zero networking and zero sleeps — the tests drive the clock by calling `tick()`.

Election timeout is randomised over 30–60 ticks; heartbeat interval is 3 ticks. The
randomisation is what breaks symmetry and prevents endless split votes.

### Storage (`src/storage/`)

- **`wal.rs`** — append-only write-ahead log. Every mutation is serialised and
  `fsync`ed **before** the operation is acknowledged. This is the durability boundary:
  if the process dies one instruction after the ack, the data is still on disk.
- **`memtable.rs`** — in-memory `BTreeMap` holding current state. Deletes are written
  as **tombstones** rather than removals, so a delete survives WAL replay and does not
  get resurrected by an earlier `SET` for the same key.
- **`engine.rs`** — ties them together and performs **WAL-replay recovery** on startup:
  read the log from the beginning, apply every entry in order, and the memtable is
  back exactly where it was.

### Networking (`src/network/`, `src/transport.rs`)

- `protocol.rs` speaks **RESP**, the Redis wire protocol — commands arrive as arrays of
  bulk strings (`*3\r\n$3\r\nSET\r\n...`) and replies use RESP simple strings, bulk
  strings, integers, nulls and errors. That is why an unmodified `redis-cli` works.
- `server.rs` accepts client connections and dispatches parsed commands.
- `transport.rs` is a separate peer-to-peer TCP layer for inter-node Raft RPCs, kept
  deliberately apart from the client path.

### Sharding (`src/sharding/`)

`ring.rs` is a consistent-hash ring backed by a `BTreeMap`, giving **64 virtual nodes
per shard** so keys spread evenly and adding a shard only moves a small slice of the
keyspace. Lookup is a single O(log n) range query for "the next point clockwise".

`router.rs` maps a key to its owning shard. If a client sends a key this shard does not
own, the node replies with a Redis-Cluster-style redirect rather than serving stale data:

```
-ERR MOVED - key belongs to shard 2
```

---

## Running it

### Single node

```bash
cargo run
# listens on 127.0.0.1:6381
```

### Three-node cluster, locally

```bash
cargo run -- 1    # client 6381, peer 7001
cargo run -- 2    # client 6382, peer 7002
cargo run -- 3    # client 6383, peer 7003
```

### Three-node cluster with monitoring (Docker)

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| node1 / node2 / node3 | `localhost:6381` / `6382` / `6383` |
| node metrics | `localhost:9101` / `9102` / `9103` |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (anonymous viewer enabled) |

### Tests

```bash
cargo test
```

Covers WAL append/replay, tombstone deletes, memtable behaviour, log truncation on
conflict, leader election including split votes, replication, commit safety, the hash
ring's distribution, and MOVED routing.

---

## Observability

Each node exposes Prometheus text-format metrics on its own port:

| Metric | Meaning |
|---|---|
| `raftkv_current_term` | Current Raft term |
| `raftkv_role` | 0 = follower, 1 = candidate, 2 = leader |
| `raftkv_is_leader` | 1 if this node is leader |
| `raftkv_known_leader` | Node ID of the leader this node believes in |
| `raftkv_commit_index` | Highest committed log index |
| `raftkv_last_applied` | Highest index applied to the state machine |
| `raftkv_log_length` | Entries in the Raft log |

The provisioned Grafana dashboard plots term, role and commit index per node, which
makes a failover visible as a step in term and a role flip.

---

## Chaos test

```bash
docker compose up --build -d
./chaos.sh
```

The script writes data, kills the current leader, waits for a new election, verifies
the data is still readable from the surviving nodes, then restarts the dead node and
confirms it catches up via log replication. Leader failover completes in **under 3
seconds**.

---

## Design notes

**Why an actor-model Raft?** Consensus bugs are timing bugs, and timing bugs found via
`sleep()` in tests are flaky and unreproducible. Making the Raft node a pure
message-in/message-out function means the entire protocol is deterministic and testable
from a single thread. Networking is someone else's problem — `transport.rs`'s.

**Why tombstones?** A WAL is replayed from the start. If a delete were a removal rather
than a record, replay would re-apply the earlier `SET` and the key would come back from
the dead.

**Why fsync before ack?** Without it, "OK" means "in the page cache" — which a power cut
discards. The fsync is the difference between a database and a cache.

**Why MOVED instead of proxying?** Redis Cluster's approach: tell the client where the
data lives and let it connect directly. One less hop, and no node becomes a bottleneck
for another shard's traffic.

---

## Layout

```
src/
  main.rs             entry point, node config, integration tests
  cluster.rs          ClusterNode — glues Raft, storage and routing
  transport.rs        peer-to-peer TCP for Raft RPCs
  metrics.rs          Prometheus exposition
  raft/               consensus
  storage/            WAL, memtable, engine
  network/            RESP protocol and client server
  sharding/           consistent hash ring and router
chaos.sh              leader-kill failover test
docker-compose.yml    3 nodes + Prometheus + Grafana
prometheus.yml        scrape config
grafana/              provisioned datasource and dashboard
```

## Built with

Rust 2021 · `serde` · Docker Compose · Prometheus · Grafana
