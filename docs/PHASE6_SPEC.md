# Phase 6 — Clustering & Replication (binding)

## Goals

Scale beyond one node with durable multi-replica partitions:

- Static cluster membership (config file)
- Kafka-style leader + ISR followers (not Raft-per-partition)
- Controller elects leaders; automatic failover on leader death
- Producer `acks=1` (default) and `acks=all` with `min_insync_replicas`
- 3-node cluster survives leader kill with **no acknowledged data loss** for `acks=all`
- Documented consistency model

## Non-goals (Phase 6)

- Dynamic membership / gossip / full Raft metadata quorum
- Rack-aware placement (stub field only)
- Exactly-once / transactions
- Cross-datacenter async replication
- Online partition reassignment UI
- Kafka wire compatibility

## Design choice (locked)

**Kafka-style controller + leader/follower log replication (ISR).**

| Piece | Choice |
|-------|--------|
| Membership | Static `cluster.toml` |
| Metadata authority | Single **controller** = lowest live broker id |
| Data path | Leader appends; followers `ReplicaFetch` + append locally |
| Commit | High watermark = min LEO among **ISR** |
| Failover | Controller removes dead brokers; elects new leader from ISR |
| Client routing | Metadata returns leader; `NotLeaderForPartition` if wrong node |

Raft-per-partition is deferred (higher ops complexity; not needed for exit criteria).

## Consistency model

See also **[docs/consistency.md](./consistency.md)** (shipped with Phase 6).

| Term | Meaning |
|------|---------|
| **LEO** (log end offset) | Next offset the local log will assign (= `high_watermark` field on single-node today) |
| **HWM** (high watermark) | Largest offset such that **every broker in ISR** has that message |
| **Committed** | Offset `o` with `o < HWM` |
| **acks=1** | Leader appends locally, responds; HWM may lag; **may lose uncommitted data** on leader crash |
| **acks=all** (`acks=255` on wire) | Leader waits until HWM covers the produced batch before responding |
| **min_insync_replicas** | Produce with `acks=all` fails if `\|ISR\| < min_insync_replicas` |
| **Client fetch** | Returns only records with `offset < HWM` (never uncommitted) |

**Guarantees:**

- `acks=all` produce that returns success: data is on all ISR members → survives any single ISR failure including former leader
- `acks=1` produce: only on leader at response time; may be lost if leader dies before followers catch up
- Consumers never see uncommitted data (fetch capped at HWM)

## Cluster config

File: TOML, path via `--cluster-config`.

```toml
# cluster.toml
default_replication_factor = 3
min_insync_replicas = 2
session_timeout_ms = 3000          # broker → controller heartbeat
replica_fetch_max_wait_ms = 500
replica_fetch_max_bytes = 1048576
replica_lag_max_messages = 10000   # out of ISR if LEO lag exceeds this

[[brokers]]
id = 1
host = "127.0.0.1"
port = 9092
# rack = "r1"   # optional; ignored for placement in Phase 6

[[brokers]]
id = 2
host = "127.0.0.1"
port = 9093

[[brokers]]
id = 3
host = "127.0.0.1"
port = 9094
```

Server flags:

```text
--node-id <u32>              # required when --cluster-config set; must match a brokers[].id
--cluster-config <path>      # omit → single-node mode (Phase 1–5 behavior)
--data-dir <path>
--listen <host:port>         # must match this node's config entry (or override advertised)
--advertised-host <host>     # optional override
--advertised-port <port>     # optional override
```

**Single-node mode** (no cluster config): `node_id=0`, RF=1, leader always local, HWM=LEO, `acks=all` ≡ `acks=1`. Existing tests must keep passing.

## On-disk metadata (per broker)

Under `{data_dir}/cluster/`:

| File | Contents |
|------|----------|
| `assignment.json` | Topic → partitions → `{replicas: [ids], leader, isr, epoch}` |
| `controller.json` | Last known controller id + generation (advisory) |

Assignment is **controller-authored**; followers apply updates from `ClusterState` pushes / responses.

## Partition assignment

On `CreateTopic(name, partitions)` (handled only by **controller**):

1. RF = `min(default_replication_factor, N_brokers)`
2. For partition `p`, place replicas starting at broker index `(p + topic_hash) % N`, then next RF-1 brokers (round-robin)
3. Initial leader = `replicas[0]`; initial ISR = all replicas
4. Persist assignment; broadcast `ClusterState` to all live brokers
5. Each broker opens local `PartitionLog` only for partitions where it is in `replicas`

Non-controller receiving CreateTopic: respond `NotController` (error code 14).

## Roles per partition

| Role | Behavior |
|------|----------|
| **Leader** | Accept client Produce/Fetch; append; track follower fetch offsets; advance HWM; respond |
| **Follower** | Reject client Produce with `NotLeaderForPartition` (13); run ReplicaFetch loop; append fetched records preserving offsets |
| **Neither** | Reject with NotFound / NotLeader |

### High watermark advancement (leader)

Maintain `last_caught_up_leo[broker_id]` updated on each ReplicaFetch.

ISR membership:

- Start = all replicas
- Remove from ISR if: broker dead (controller) OR `leader_leo - replica_leo > replica_lag_max_messages`
- Add back when replica_leo catches to within lag and broker is live

`HWM = min(leo of all brokers currently in ISR)` (including leader's LEO).

### acks handling (leader)

| acks | Wire | Behavior |
|------|------|----------|
| 0 | 0 | Append; respond immediately (best-effort) |
| 1 | 1 | Append; respond after local append (default) |
| all | 255 | Append; wait until `HWM >= base_offset + count` (timeout → error) |

If `acks=all` and `|ISR| < min_insync_replicas` → error `NotEnoughReplicas` (15) without appending (or after append but before ack — prefer **before** append for simplicity).

## Inter-broker protocol

Reuse existing TCP framing (`volant-protocol`). New opcodes **20+**:

| Opcode | Name | Direction |
|--------|------|-----------|
| 20 | ReplicaFetch | Follower → Leader |
| 21 | ReplicaFetch | response |
| 22 | HeartbeatBroker | Any → Controller |
| 23 | HeartbeatBroker | response |
| 24 | ClusterState | Controller → Broker (or pull response) |
| 25 | ClusterState | response / apply ack |
| 26 | CreateTopic (cluster-aware; same as 3 but controller-only semantics) |

Prefer **extending existing CreateTopic (3)** with controller check rather than new opcode.

### ReplicaFetch request (20)

```
topic: string
partition: u32
from_offset: u64          # follower LEO
max_bytes: u32
replica_id: u32           # follower node id
```

### ReplicaFetch response (21)

```
error_code: u16           # 0 ok; 13 not leader; ...
topic: string
partition: u32
high_watermark: u64       # leader HWM (for follower awareness)
leader_epoch: u32
record_count: u32
records: repeated {       # same layout as client Fetch records
  offset: u64
  timestamp_ms: i64
  key: optional bytes
  value: bytes
  headers: ...
}
```

Follower **must** append records at the exact `offset` values (storage may need `append_raw` / `append_at_offset` if current append always assigns next — see storage note).

### HeartbeatBroker (22/23)

```
# request
broker_id: u32
controller_id_known: u32  # 0 if unknown
generation: u32

# response
error_code: u16
controller_id: u32
generation: u32
alive_brokers: repeated u32
```

Controller tracks last heartbeat time; brokers missing `session_timeout_ms` are **dead**.

### ClusterState (24/25)

Full snapshot (Phase 6 size is fine):

```
generation: u32
controller_id: u32
topics: repeated {
  name: string
  topic_id: u32
  partitions: repeated {
    partition_id: u32
    leader: u32
    leader_epoch: u32
    replicas: repeated u32
    isr: repeated u32
  }
}
```

Brokers apply: open/close partition logs as needed; start/stop follower loops; update leader epoch.

## Metadata response extension

Extend `PartitionInfo` wire format **compatibly**:

```
partition_id: u32
leader: u32
hwm: u64
# Phase 6 additions (always present; single-node: replicas=[self], isr=[self]):
replica_count: u32
replicas: repeated u32
isr_count: u32
isr: repeated u32
leader_epoch: u32
```

**Wire compatibility:** bump protocol payload understanding; old clients that stop reading after `hwm` still work if they ignore trailing bytes — **our** client/server must encode/decode fully. Document as Phase 6 breaking for external tools that parse partial payloads strictly by length.

Also extend `Metadata` brokers list to include **all cluster brokers**, not only self.

## Error codes (additions)

| Code | Name |
|------|------|
| 13 | NotLeaderForPartition |
| 14 | NotController |
| 15 | NotEnoughReplicas |
| 16 | BrokerNotAvailable |

## Storage changes

`PartitionLog` / segment append today assigns monotonic offsets. Followers need:

```rust
// Prefer:
fn append_records_with_offsets(&mut self, records: &[Record]) -> Result<()>;
// or
fn append_at(&mut self, offset: Offset, msg: Message) -> Result<Record>;
```

Rules:

- Offsets must be contiguous from current LEO
- If `record.offset != leo` → error (gap) or truncate+rebuild (Phase 6: **error / fatal replica**; re-fetch from 0 only on empty log)
- Follower never invents offsets

## Broker internal modules (suggested)

```
volant-broker/
  cluster/
    mod.rs          # ClusterConfig load
    membership.rs   # live set, heartbeats
    controller.rs   # create topic, elect leaders
    assignment.rs   # replica placement
    state.rs        # applied ClusterState
  replica/
    mod.rs
    follower.rs     # ReplicaFetch loop
    leader.rs       # ISR / HWM tracking
  broker.rs         # integrate roles + acks
  net.rs            # dispatch new opcodes
```

Optional crate split is **not** required; keep in `volant-broker` for Phase 6.

## Client / CLI

- Client: honor Metadata leaders for Produce/Fetch; on `NotLeaderForPartition`, refresh metadata and retry once
- Produce `acks`: pass through; document `255` = all
- CLI: `volant topics create` works against any broker only if that broker is controller **or** CLI targets controller (document); prefer Metadata to find controller later — Phase 6: try create, on NotController print controller id from error message / heartbeat

## Server multi-node

```bash
# terminal 1–3
cargo run -p volant-server -- \
  --node-id 1 --cluster-config ./cluster.toml \
  --data-dir ./data1 --listen 127.0.0.1:9092

cargo run -p volant-server -- \
  --node-id 2 --cluster-config ./cluster.toml \
  --data-dir ./data2 --listen 127.0.0.1:9093

cargo run -p volant-server -- \
  --node-id 3 --cluster-config ./cluster.toml \
  --data-dir ./data3 --listen 127.0.0.1:9094
```

## Tests (exit criteria)

1. **Unit:** assignment round-robin; ISR shrink/grow; HWM min logic
2. **Integration (in-process):** 3 fake brokers or 3 `Broker` with shared cluster harness — produce acks=all, kill leader task, new leader serves fetch of committed data
3. **E2E (preferred):** spawn 3 servers on ephemeral ports, produce acks=all, SIGKILL leader process, metadata shows new leader, fetch all messages
4. **Regression:** single-node `cargo test --workspace` green without cluster config
5. **Rolling restart:** stop follower, restart, catches up; produce continues (document if full automation deferred)

Minimum bar for Phase 6 ✅:

- [x] Spec + consistency.md
- [x] Static membership + controller
- [x] ReplicaFetch path + HWM/ISR
- [x] acks=all + min_insync_replicas
- [x] Leader failover elects from ISR
- [x] Test: leader kill → no loss of acks=all data
- [x] Single-node tests still pass

## Implementation workstreams (parallel)

1. **protocol** — opcodes 20–25, error codes 13–16, Metadata partition extension, encode/decode tests
2. **storage-follower** — `append_with_offset` / batch with fixed offsets; durable_log tests
3. **cluster-controller** — config, membership heartbeats, assignment, controller election, ClusterState apply
4. **replica-path** — leader HWM/ISR, follower loop, produce acks=all, NotLeader checks
5. **server-e2e-docs** — CLI flags, consistency.md, ROADMAP/README, 3-node failover test

## Agent rules

Each agent: plan → code → review → test → fix.  
Do not claim Phase 6 complete until e2e/failover proof exists.  
macOS must remain default-build green (no Linux-only deps for clustering).
