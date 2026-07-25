# Phase 116 — Durable DeleteRecords outbox for offline replicas (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable outbox store under `{data_dir}/__delete_records_outbox` + load/persist — **landed**  
- **PR2** enqueue on fan-out failure + background/live drain + metrics — **landed**  
- **PR3** multi-node offline→online integration tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — when a partition leader truncates via DeleteRecords
and a peer is offline or the best-effort fan-out RPC fails, **remember** the pending
truncate on disk and **retry** so the peer’s log start catches up after it returns.

## Goals

1. **Durable pending truncates:** After a successful **leader** local DeleteRecords,
   if `ReplicaDeleteRecords` to another assigned replica fails (or the peer is not
   reachable), enqueue a durable outbox entry under the leader’s `data_dir`.
2. **At-least-once retry:** Drain the outbox when peers are live (short background
   loop in cluster mode) using the existing Phase 113 opcode — no new transport.
3. **Idempotent peer apply:** Peers still only advance log start (whole sealed
   segments); re-delivery is safe.
4. **Client semantics unchanged:** Client DeleteRecords success remains based on
   **local leader** truncate only (Phase 113 honesty).
5. **Metrics:** outbox depth + enqueue / retry success / retry error counters.
6. Integration test: 3-node cluster, kill follower, DeleteRecords, restart follower,
   assert peer log_start advanced via outbox drain.
7. Living docs honesty (not a consensus truncate log; not multi-DC).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Multi-broker session handoff | Phase 115 local only; later |
| Raft / dynamic membership | Out of scope |
| Sync 2PC truncate (client waits on all replicas) | Latency / availability; rejected in 113 |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Full KIP-890/939 / `__transaction_state` | Orthogonal remainder after 114 |
| Cross-DC async replication | Out of scope |
| Per-broker BROKER config overrides | Orthogonal |

## Problem (today — post Phase 113)

```text
  client DeleteRecords → leader local truncate OK → ReplicaDeleteRecords to peers
                              │
                    live peer: log_start advances
                    dead peer: fanout_errors++ and **forgotten**
                              │
                    peer restarts later with **stale** log start forever
                    (until operator re-issues DeleteRecords or retention)
```

Phase 113 closed live-peer fan-out. Offline / flaky peers kept old segments forever.

## Design principles

1. **Leader-local outbox** — pending targets live on the **partition leader** that
   applied DeleteRecords (`{data_dir}/__delete_records_outbox/state.json`). Not a
   controller SoT; not Raft.
2. **Reuse opcode 70/71** — `ReplicaDeleteRecords` only; no new wire keys.
3. **Merge by max offset** — same `(replica_id, topic, partition)` keeps
   `max(before_offset)` (log start only advances).
4. **Best-effort client path** — enqueue + retry never fail the client response.
5. **Bounded outbox** — soft cap; drop new distinct keys when full (metric++).
6. **Honest gaps** — not a full consensus truncate journal; leader failover may
   leave the old leader’s outbox orphaned (new leader has empty outbox unless
   another DeleteRecords runs); not multi-DC.

---

## Architecture

### On-disk layout

`{data_dir}/__delete_records_outbox/state.json`:

```json
{
  "version": 1,
  "entries": [
    {
      "replica_id": 3,
      "topic": "events",
      "partition": 0,
      "before_offset": 128,
      "leader_epoch": 0
    }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `version` | File format (`1`) |
| `replica_id` | Target broker id |
| `topic` / `partition` | Truncate target |
| `before_offset` | Desired low watermark / delete-before offset (max wins) |
| `leader_epoch` | Epoch stamped on the original fan-out (for fence honesty) |

### Key + merge

```text
key = (replica_id, topic, partition)
on enqueue:
  if key exists: before_offset = max(old, new); prefer epoch from the higher offset
  else if len < MAX: insert
  else: drop + outbox_drops++
persist atomic (tmp + rename + fsync) after mutation
```

Default `MAX` entries: **10_000** (constant; no new env required for MVP).

### When to enqueue

| Path | Action |
|------|--------|
| Leader local DeleteRecords success | Fan-out each assigned peer (Phase 113) |
| Peer RPC success (`error_code=0`) | No outbox change (clear key if present) |
| Peer RPC failure / unexpected response / non-zero error | `fanout_errors++` **and** enqueue |
| Single-node / no peers | No-op (unchanged) |

Non-zero peer errors that are **epoch fence** (`InvalidProducerEpoch`): still
enqueue is wasteful — **drop** that key if present (cannot apply with stale epoch;
new leadership / re-issue will re-create). Other non-zero codes: enqueue for retry.

### Drain algorithm

```text
// cluster mode background loop (~500ms) + optional immediate drain after fan-out
for entry in outbox where peer has addr:
  if membership knows peer dead and we want to save RPC: skip (MVP may still try)
  rpc ReplicaDeleteRecords(topic, partition, before_offset, leader_epoch)
  on success (0): remove entry; retry_success++
  on fence: remove entry (stale); no success counter
  on other error / transport: leave entry; retry_errors++
persist after batch of removals
```

**Peer alive preference:** Prefer draining entries whose `replica_id` is in the
local live set (Phase 110 membership). Offline peers stay queued without spamming
RPC every tick (optional: still attempt if never marked live — addr-based try on a
slower cadence is fine for MVP; **implement live-set filter**).

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_delete_records_fanout_errors_total` | counter | Immediate fan-out failures (Phase 113; retained) |
| `volant_delete_records_outbox_depth` | gauge | Pending entries on this broker |
| `volant_delete_records_outbox_enqueued_total` | counter | Successful enqueues (including max-merge updates) |
| `volant_delete_records_outbox_retry_success_total` | counter | Entries removed after successful peer truncate |
| `volant_delete_records_outbox_retry_errors_total` | counter | Drain RPC failures |
| `volant_delete_records_outbox_drops_total` | counter | Rejected enqueues at capacity |

### Client / Kafka surface

Unchanged: DeleteRecords (key 21) → local leader; fan-out + outbox internal.
No new public API keys.

---

## Tests

| File | Cases |
|------|-------|
| `phase116_delete_records_outbox.rs` | 3-node: produce → kill follower listener → DeleteRecords on leader → outbox depth ≥ 1 → restart follower → drain → follower log_start ≥ leader low; client success independent of offline peer; outbox roundtrip unit; single-node empty |
| Regression | `phase113_delete_records_fanout` |

Harness: same multi-broker pattern as Phase 113.

---

## Exit criteria

1. `cargo test -p volant-broker --test phase116_delete_records_outbox` green  
2. `cargo test -p volant-broker --test phase113_delete_records_fanout` green  
3. Spec + ROADMAP / PHASE_HISTORY / INDEX / consistency / ops / features /
   KAFKA_COMPAT honest updates  
4. Single-node DeleteRecords bitwise-unchanged (no outbox traffic)  
5. Commit on `main`

---

## Honest limitations (after ship)

- **Not** a consensus truncate log; only the leader that handled DeleteRecords
  owns the pending set  
- Leader **failover** does not automatically transfer the old leader’s outbox to
  the new leader (re-issue DeleteRecords or wait for retention on stale peers)  
- Bounded outbox may drop distinct keys under extreme backlog  
- Whole-segment DeleteRecords only (Phase 14 storage rule)  
- Inter-broker RPCs still not ACL-gated (shared-token / TLS)  
- No multi-DC / async replication story  

---

## Still deferred after Phase 116

- Multi-broker session handoff / affinity routing  
- Full KIP-890/939 / `__transaction_state`  
- Raft / dynamic membership  
- Multi-lang clients / chaos-mesh / long fuzz  
- Transparent EndTxn forward  
- Per-broker BROKER config overrides  
- Outbox handoff on leadership change → **closed by Phase 123**

---

## Decision log (locked for this phase)

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| Storage | Leader-local durable JSON outbox | Controller SoT queue (extra hop; SoT churn) |
| Transport | Reuse `ReplicaDeleteRecords` | New opcode |
| Client wait | Still best-effort (async catch-up) | Block client until all replicas ACK |
| Merge | Max `before_offset` per (peer, tp) | Full history of every admin call |
| Drain trigger | Background + live-set aware | Only on heartbeat (harder to test; still ok as add-on) |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 116 |
| [consistency.md](./consistency.md) | Outbox retry honesty |
| [ops.md](./ops.md) | Metrics + offline replica note |
| [features.md](./features.md) | Close “no durable pending” limitation |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | DeleteRecords cluster row |
| [../ROADMAP.md](../ROADMAP.md) | Phase 116 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started (implementation)

```bash
cargo test -p volant-broker --test phase116_delete_records_outbox
cargo test -p volant-broker --test phase113_delete_records_fanout
```
