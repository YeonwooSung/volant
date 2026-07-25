# Phase 113 — Cluster admin fan-out (MVP)

**Status:** ✅ Shipped (MVP)  
**Implementation:**  
- **PR1** protocol opcodes 70–75 + dispatch stubs + generation atomics — **landed**  
- **PR2** DeleteRecords fan-out + tests — **landed**  
- **PR3** BROKER config fan-out (controller-only Alter, generation push) — **landed**  
- **PR4** ACL snapshot fan-out (controller-only Create/Delete, generation push) — **landed**  
- **PR5** living docs ship record — **landed**  
**Theme:** Cluster correctness — make cluster-scoped admin ops actually reach every
relevant replica, instead of silently applying only on the node that handled the
client request.

## Goals

1. **DeleteRecords fan-out:** When a partition leader truncates its log, every
   other replica for that partition advances its log start to at least the same
   low watermark (best-effort, with clear error / metric honesty).
2. **BROKER config fan-out:** AlterConfigs / IncrementalAlterConfigs for the
   **BROKER** resource applies cluster-wide when running with `--cluster-config`
   (controller-authoritative push), not only on the node that accepted the
   admin RPC.
3. **ACL snapshot fan-out (MVP):** CreateAcls / DeleteAcls are **controller-only**
   in cluster mode; the controller pushes a generationed snapshot so **all** live
   brokers share the same durable ACL set (no silent per-node authZ drift).
4. Integration tests under multi-node in-process cluster harness + living docs
   honesty (no false Raft / multi-broker 2PC claims).
5. Document the PR DAG so implementation can land as a reviewable stack.

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Multi-broker **2PC** / full KIP-890 | Separate control plane; **Phase 114+** |
| Durable multi-broker **fetch sessions** / session affinity | Session table redesign; later |
| Dynamic membership / Raft metadata quorum | Membership redesign; later |
| Full Kafka DynamicBrokerConfig catalog | Only the six Phase 99 knobs |
| Peer-to-peer gossip (controller remains membership SoT) | Unchanged from Phase 110 |
| Exactly-once streams / multi-lang clients / long fuzz campaigns | Orthogonal tracks |
| Rewriting control batches when markers clip | Phase 111 honesty; not this phase |
| Cross-DC async replication | Out of scope |

## Problem (today)

```text
                    client
                      │
                      ▼
              broker that got the RPC
              (may or may not be leader /
               controller)
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
     local only   followers     other brokers
     (applied)    (stale log    (stale knobs /
                   start /       ACLs)
                   ACLs)
```

| Op | Cluster behavior today | Failure mode |
|----|------------------------|--------------|
| `DeleteRecords` | Leader truncates **local** log only (`broker.rs`); followers keep old segments until natural retention / ReplicaFetch cannot un-write | Divergent log starts; wasted disk; soft markers GC on leader only; re-elected leader may expose offsets others already truncated |
| `AlterConfigs` BROKER | Validates name vs **local** `node_id` (Phase 103); sparse file under **that** broker's `data_dir` only | Txn/session/sweep knobs differ across nodes after admin tool hits one broker |
| ACLs | File store `{data_dir}/__acls` per node | AuthZ depends on which broker the client hit |

Replica **data** already fans out via `ReplicaFetch` (Phase 6). Admin /
metadata-side effects do not.

## Design principles

1. **Reuse inter-broker RPC** (`inter_broker_rpc` + shared-token / inter-broker TLS)
   — no new transport.
2. **Controller remains SoT** for cluster-wide admin state (same as assignment /
   membership). Non-controllers apply pushes; they do not invent cluster policy.
3. **Partition-local ops stay leader-driven** (DeleteRecords only from current
   partition leader; followers receive fan-out, clients that hit a non-leader
   still get `NotLeaderForPartition`).
4. **Best-effort + generation**, not Raft: short inconsistency windows during
   controller failover are **honest** (same class as Phase 6 metadata lag).
5. **Single-node mode unchanged** — no `--cluster-config` ⇒ local-only paths
   identical to Phase 112.

---

## Track A — DeleteRecords fan-out

### Contract

| Actor | Behavior |
|-------|----------|
| Client → any broker | Unchanged: only **partition leader** applies; others return `NotLeaderForPartition` |
| Leader after successful local `delete_records` | Best-effort RPC to every **other replica** in the current assignment (or local ISR if assignment lag): `ReplicaDeleteRecords { topic, partition, before_offset, leader_epoch }` |
| Follower receiving RPC | If local epoch ≤ request epoch (or no epoch stored): call storage `delete_records` + soft-marker GC/clip (Phase 104/111); reply low watermark. If epoch too old: reject with `FencedLeaderEpoch` / ignore |
| Response to client | Still the **leader** low watermark after **local** success. Fan-out failure does **not** fail the client response (honest: eventually consistent log starts). Increment metric `volant_delete_records_fanout_errors_total` |

### Why best-effort (not 2PC truncate)

- Truncate-after-ack is already the Kafka-ish model for DeleteRecords.
- Blocking client on slow/dead followers reintroduces rolling-restart pain that
  Phase 108/110 fixed for produce.
- Dead replicas catch up on restart via: (optional) controller generation note
  **or** simply re-applying when they rejoin and receive a later fan-out /
  operator re-issues DeleteRecords. Phase 113 MVP: **retry once** per replica
  synchronously; no durable "pending truncate" queue (stretch → later).

### Wire (native protocol)

New opcodes (suggested; keep contiguous with Phase 6 inter-broker band):

| Opcode | Name | Direction |
|-------:|------|-----------|
| 70 | `ReplicaDeleteRecords` | Leader → replica |
| 71 | `ReplicaDeleteRecords` response | Replica → leader |

Payload (LE):

```text
request:  topic_len u16 | topic | partition u32 | before_offset u64 | leader_epoch i32
response: error_code u16 | low_watermark u64
```

Kafka shim: no new public API key — client DeleteRecords (key 21) still maps to
local `Broker::delete_records`; fan-out is internal.

### Algorithm (leader)

```text
low, err = local_delete_records(topic, partition, before)
if err != 0: return to client
gc_and_persist_aborted_markers(...)   # existing Phase 104/111
for replica_id in assigned_replicas(topic, partition) - {self}:
    rpc ReplicaDeleteRecords(...); on error: metric++, log warn
return low to client
```

Followers that are **not** yet at `before_offset` (LEO < before): still advance
log start as far as storage allows (same as local `PartitionLog::delete_records`
semantics — whole sealed segments only). Document: fan-out cannot invent
segments the follower never had; it only truncates prefix the follower already
replicated.

---

## Track B — BROKER config fan-out

### Contract

| Mode | Behavior |
|------|----------|
| Single-node | Unchanged: sparse `{data_dir}/__broker_config/state.json` |
| Cluster + Alter on **controller** | Apply + persist locally; push `ClusterBrokerConfig { generation, entries }` to every other **live** broker |
| Cluster + Alter on **non-controller** | **Forward** the Alter to the controller (or reject with `NotController` — pick one and stick). **MVP choice: reject with NotController (error 41)** so admin tools always hit the controller. Native + Kafka Describe still work on any node for **local** effective values |
| Peer receiving push | Apply `apply_broker_config_value` for each entry; merge sparse durable file **locally**; ignore if `generation < last_applied_config_gen` |
| Restart | Each node loads **its** sparse file (already applied via push). Controller remains SoT for **future** Alters; no full-cluster config re-sync on boot required if every push succeeded. **Boot repair (MVP):** non-controller may pull config snapshot once after first successful `ClusterState` if `config_generation` in assignment/header advanced |

### Phase 103 name validation

- Controller accepts BROKER resource name empty **or** decimal equal to
  **controller** `node_id` (unchanged local rule on the node handling Alter).
- Pushed applies do **not** re-validate peer names against the controller id
  (peers apply values only).
- Optional later: name = target broker id for per-broker knobs — **out of
  scope**; Phase 113 fan-out is **homogeneous cluster knobs** (all six keys).

### Wire

| Opcode | Name |
|-------:|------|
| 72 | `ClusterBrokerConfig` (push or pull-response) |
| 73 | response |

```text
request:  generation u64 | n u16 | repeated (key_len u16 | key | val_len u16 | val)
          empty val = DELETE / product default (same as IncrementalAlter DELETE)
response: error_code u16 | applied_generation u64
```

### Kafka surface

DescribeConfigs BROKER on any node → that node's **effective** values (after
push). AlterConfigs / IncrementalAlterConfigs BROKER on non-controller →
**NotController** (document in `KAFKA_COMPAT.md`). Tools must target the
controller (lowest live id; Metadata / DescribeCluster already expose brokers).

---

## Track C — ACL snapshot fan-out (MVP)

### Contract

| Op | Behavior |
|----|----------|
| CreateAcls / DeleteAcls | Allowed on **controller only** in cluster mode (else `NotController`). Apply + persist local `__acls`. Bump `acl_generation`. Push full snapshot (or compact diff) to live peers |
| ListAcls / authorize | Local store on each broker (after push) |
| Super-users / env | Unchanged; still process-local / flag-driven |
| Boot | Non-controller pulls ACL snapshot if `acl_generation` advanced (piggyback on ClusterState or dedicated pull after heartbeat) |

### Why full snapshot (not CRDT)

- ACL sets are small; snapshot avoids dual-write OT complexity.
- Matches "controller SoT" model used for assignment.

### Wire

| Opcode | Name |
|-------:|------|
| 74 | `ClusterAclSnapshot` |
| 75 | response |

```text
request:  generation u64 | json_or_binary snapshot (versioned)
response: error_code u16 | applied_generation u64
```

Reuse existing `AclSnapshot` / `AclStore` serialization if present; otherwise
versioned JSON matching `__acls/acls.json`.

### Honest limitation

Brief window after controller failover before new controller has pushed: peers
may still hold last snapshot (good) or empty if a brand-new broker joined
without pull (boot pull required). No multi-master ACL merge.

---

## Shared infrastructure

### Protocol / net

| Piece | Work |
|-------|------|
| `volant-protocol` | New Request/Response variants + encode/decode + opcode map |
| `volant-broker::net` | Dispatch inter-broker requests (auth: existing inter-broker token / TLS) |
| `Broker` | Fan-out helpers; controller gates; generation counters |
| Metrics | `volant_delete_records_fanout_errors_total`, `volant_cluster_config_push_errors_total`, `volant_cluster_acl_push_errors_total`, gauges for `config_generation` / `acl_generation` |

### Generations

Store on controller (and echo in pushes):

```text
cluster_admin {
  config_generation: u64,  // starts 0; ++ on each successful BROKER alter
  acl_generation: u64,     // ++ on each ACL mutate
}
```

Optional: embed both in `ClusterState` response header so observers can pull
without separate RPCs (preferred if `ClusterState` decode remains backward
compatible — bump state version carefully).

### Single-node

All new opcodes may still be registered but fan-out loops are no-ops when
`cluster.is_none()`.

---

## Tests

| Test file | Cases |
|-----------|-------|
| `phase113_delete_records_fanout.rs` | 3-node: produce → DeleteRecords on leader → follower log_start ≥ low; non-leader client gets NotLeader; dead follower does not fail client; metric increments on forced RPC failure |
| `phase113_broker_config_fanout.rs` | Alter on controller changes getters on peer; Alter on non-controller → NotController; restart peer keeps sparse file; DELETE unfreezes to product default cluster-wide |
| `phase113_acl_fanout.rs` | CreateAcls on controller denies produce on peer; Create on non-controller → NotController; peer restart reloads durable ACLs; generation monotonic |
| Regression | `phase104` / `phase111` marker GC; `phase100`–`103` sparse config; `phase8` / `phase108` / `phase110` ISR death |

Harness: reuse `tests/common` multi-broker helpers from cluster failover tests.

---

## Exit criteria

1. `cargo test -p volant-broker --test phase113_delete_records_fanout` green  
2. `cargo test -p volant-broker --test phase113_broker_config_fanout` green  
3. `cargo test -p volant-broker --test phase113_acl_fanout` green  
4. Existing cluster + config + marker GC suites still green  
5. Living docs updated: this spec, `ROADMAP.md`, `PHASE_HISTORY.md`, `INDEX.md`,
   `KAFKA_COMPAT.md` (NotController on BROKER alter / ACL mutate),
   `consistency.md` / `ops.md` (admin targeting controller; DeleteRecords fan-out
   honesty), `features.md` open limitations revised  
6. Single-node path behavior bitwise-identical for admin ops (no NotController)  
7. Commit on `main` (or stacked PRs merged)

---

## Honest limitations (after ship)

- DeleteRecords fan-out is **best-effort**; no durable pending-truncate log  
- No multi-broker 2PC; prepared txns remain per-node store  
- BROKER knobs are **homogeneous** (not per-broker overrides)  
- ACL / config rely on controller liveness; failover can lag one generation  
- Inter-broker admin RPCs are **not** ACL-gated (shared-token / TLS only), same
  as ReplicaFetch  
- Kafka clients that Alter BROKER against a random bootstrap broker may see
  NotController — document and recommend controller address  

---

## PR plan (DAG)

Implement as a **stack** (Graphite or plain stacked branches). Each PR is
independently reviewable and green.

```text
PR1  protocol + dispatch skeleton
 │
 ├─► PR2  DeleteRecords fan-out + tests          ─┐
 │                                                ├─► PR5  docs / ROADMAP / compat honesty
 ├─► PR3  BROKER config fan-out + tests          ─┤
 │                                                │
 └─► PR4  ACL snapshot fan-out + tests           ─┘
```

PR2–PR4 may proceed in parallel after PR1 lands. PR5 last (or land partial docs
per PR and do a final honesty pass).

### PR1 — Inter-broker admin protocol skeleton

**Scope**

- Add opcodes 70–75 request/response encode/decode + unit roundtrips  
- `net` dispatch stubs: unknown → error; known → `todo`/empty apply hooks  
- Generation fields on `Broker` (atomics) + optional ClusterState version bump
  sketch (may land fully in PR3/4)  
- No behavior change for clients  

**Exit:** protocol unit tests; workspace builds.

### PR2 — DeleteRecords fan-out

**Scope**

- Leader fan-out after local success  
- Follower `ReplicaDeleteRecords` handler + Phase 104/111 GC path  
- Metrics + `phase113_delete_records_fanout` tests  
- `ops.md` / `consistency.md` note  

**Exit:** PR2 tests green; single-node DeleteRecords unchanged.

### PR3 — BROKER config fan-out

**Scope**

- Cluster-mode Alter → controller only (`NotController` else)  
- Push `ClusterBrokerConfig`; peer apply + sparse persist  
- Optional pull-on-generation via ClusterState  
- `phase113_broker_config_fanout` + Kafka shim NotController assertion  
- `KAFKA_COMPAT.md` row  

**Exit:** PR3 tests green; Phase 100–103 single-node tests still green.

### PR4 — ACL snapshot fan-out

**Scope**

- Controller-only Create/Delete Acls in cluster mode  
- Snapshot push + boot/generation pull  
- `phase113_acl_fanout`  
- CLI note: ACL admin against controller in multi-node  

**Exit:** PR4 tests green; Phase 20/21 ACL tests still green single-node.

### PR5 — Living docs + phase ship record

**Scope**

- Mark Phase 113 ✅ in `ROADMAP.md` / `PHASE_HISTORY.md` / `INDEX.md`  
- Tighten deferred list (fan-out closed; 2PC still open)  
- README band line if needed  
- Whitepaper honest non-parity bullet update  

**Exit:** docs match code; no remaining "DeleteRecords does not fan out" claims.

---

## Suggested implementation order (within PRs)

1. Protocol types + codec tests  
2. `Broker` hooks that are pure (apply local truncate / config / ACL)  
3. Async fan-out from existing Tokio context (`net` handlers already async)  
4. Controller gate for config + ACL  
5. Metrics  
6. Multi-node tests  
7. Docs  

Avoid expanding `broker.rs` without extraction: prefer
`cluster/admin_fanout.rs` (or `cluster/delete_records_fanout.rs` +
`cluster/config_sync.rs` + `cluster/acl_sync.rs`) and call from `Broker` /
`net`. Phase 113 is a good moment to **stop growing** the 5k-line `broker.rs`
for cluster control-plane code.

---

## Rollout / ops impact

| Item | Note |
|------|------|
| Upgrade order | Rolling restart safe: old peers ignore unknown opcodes → leader fan-out errors increment metrics until all upgraded. Prefer upgrade followers first, then leaders, then controller |
| Config | No new env vars required for MVP |
| CLI | Document `volant` ACL / config admin → controller in cluster mode |
| Metrics | Alert optional on fan-out error counters |

---

## Still deferred after Phase 113

- Multi-broker **2PC** / full KIP-890 abortable surface → **closed by Phase 114 (MVP)**; full KIP-890/939 still deferred
- Multi-broker session affinity / durable fetch sessions  
- Dynamic membership / Raft metadata  
- Durable pending DeleteRecords queue for down replicas → **closed by Phase 116**  

- Per-broker BROKER config overrides  
- Inter-broker RPC ACL gating  
- Long fuzz / chaos-mesh / multi-lang clients  

### Phase 114 sketch (not in scope)

Controller-coordinated prepare/commit for Enable2Pc across partition leaders;
txn log or controller-durable prepared set; fence rules across failover.
Do **not** start 114 until 113 fan-out + generations exist — 2PC will reuse the
same inter-broker admin patterns.

---

## Decision log (locked for this phase)

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| DeleteRecords durability | Best-effort fan-out | Sync 2PC truncate (latency / availability) |
| Non-controller BROKER Alter | `NotController` | Transparent forward (hides topology; harder retries) |
| ACL / config SoT | Controller | Multi-master last-writer-wins |
| ACL payload | Full snapshot | Per-entry gossip |
| Kafka public API | No new keys | Expose internal opcodes |

---

## Getting started (implementation)

```bash
# After PR1+
cargo test -p volant-protocol
cargo test -p volant-broker --test phase113_delete_records_fanout
cargo test -p volant-broker --test phase113_broker_config_fanout
cargo test -p volant-broker --test phase113_acl_fanout

# Regression bands
cargo test -p volant-broker --test phase104_marker_gc
cargo test -p volant-broker --test phase111_straddle_marker_clip
cargo test -p volant-broker --test phase100_broker_config_durable
cargo test -p volant-broker --test phase103_broker_name
cargo test -p volant-broker --test phase110_alive_set_death
cargo test -p volant-broker --test cluster_failover
```

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 113 |
| [consistency.md](./consistency.md) | DeleteRecords + admin SoT notes |
| [ops.md](./ops.md) | Target controller for BROKER/ACL alter |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | NotController honesty |
| [../ROADMAP.md](../ROADMAP.md) | Phase 113 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

