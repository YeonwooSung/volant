# Phase 117 — Controller failover catch-up for ACL + BROKER config (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable admin generations + heartbeat applied-gen piggyback — **landed**  
- **PR2** controller lag-driven catch-up re-push (config + ACL) + metrics — **landed**  
- **PR3** multi-node offline/rejoin + controller restart tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — after peer rejoin, controller restart, or brief
controller change, brokers **converge** on generationed ACL snapshot + BROKER
knobs instead of silent permanent drift.

## Goals

1. **No permanent silent drift** on ACL / BROKER config when a peer misses a
   Phase 113 push (offline, flaky RPC, or restart mid-fan-out).
2. **Durable generations** under each broker's `data_dir` so controller restart
   does not reset gens to `0` and poison later Alters (peers would ignore
   `generation <= applied`).
3. **Catch-up path** reuse Phase 113 opcodes 72–75 (`ClusterBrokerConfig` /
   `ClusterAclSnapshot`) — controller re-pushes full SoT state when a peer's
   applied generation lags.
4. **Heartbeat piggyback:** non-controllers report `applied_config_generation` +
   `applied_acl_generation` on `HeartbeatBroker`; controller compares and
   re-pushes when lagging (covers rejoin without a new alter).
5. Metrics: catch-up success / error counters (generation gauges already Phase 113).
6. Integration tests: alter on controller → kill/restart peer or controller →
   catch-up restores generationed ACL + BROKER state.
7. Honest lag windows documented (still not Raft).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Raft / dynamic membership | Out of scope |
| Multi-broker session handoff | Phase 115 local only |
| Full KIP-890/939 / `__transaction_state` | Orthogonal remainder after 114 |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| ISR lag shrink redesign | Orthogonal |
| Per-broker BROKER overrides / multi-master ACL | Rejected; controller SoT |
| Transparent EndTxn forward | Orthogonal |

## Problem (today — post Phase 113/116)

```text
  controller Alter → push gen=N to live peers
                         │
              offline peer: fanout_errors++ and **forgotten**
                         │
              peer restarts later with **stale** ACL/config forever
              (until operator re-issues Alter)

  controller process restart:
    gens reset to 0 in memory
    next Alter → gen=1
    peers with applied=N ignore forever (generation <= applied)
```

Phase 113 closed **live-peer** push. Phase 116 closed DeleteRecords offline
retry. ACL/config still had no durable gens and no rejoin catch-up.

## Design principles

1. **Controller remains SoT** — no Raft, no multi-master merge.
2. **Reuse opcodes 72–75** — catch-up is a re-push of current controller state,
   not a new pull-response payload.
3. **Durable generations** on every node (`__cluster_admin/state.json`).
4. **Best-effort + bounded lag** — same class as Phase 6 metadata lag; brief
   windows are honest and tested.
5. **Single-node unchanged** — no cluster config ⇒ no catch-up traffic.

---

## Architecture

### Durable admin generations

`{data_dir}/__cluster_admin/state.json`:

```json
{
  "version": 1,
  "config_generation": 3,
  "applied_config_generation": 3,
  "acl_generation": 2,
  "applied_acl_generation": 2
}
```

| Field | Meaning |
|-------|---------|
| `version` | File format (`1`) |
| `config_generation` | Controller (or last known) BROKER config gen |
| `applied_config_generation` | Last applied BROKER config gen on this node |
| `acl_generation` | Controller (or last known) ACL gen |
| `applied_acl_generation` | Last applied ACL gen on this node |

**Load** on `Broker::new` / `with_cluster` after durable config/ACLs.  
**Persist** (atomic tmp + rename + fsync) whenever any generation atomic changes
(bump on alter/create/delete; apply on peer push; catch-up apply).

Peers store both SoT gens (mirrored from last applied push) and applied gens so
a peer that is later promoted to controller can re-push at the correct
generation without inventing a lower one.

### Heartbeat piggyback (backward compatible)

`HeartbeatBroker` **request** (after existing fields):

```text
… | applied_config_generation u64 | applied_acl_generation u64
```

Decode: if fewer than 16 trailing bytes remain, treat both as `0` (old peers).

Controller path on each heartbeat from `broker_id`:

```text
if self is controller:
  if peer.applied_config < self.config_generation:
    re-push ClusterBrokerConfig(gen, full effective entries)
  if peer.applied_acl < self.acl_generation:
    re-push ClusterAclSnapshot(gen, full snapshot)
  on success: catchup_success++; on failure: catchup_errors++
```

Non-controller continues to send heartbeats; applied gens come from local
atomics (restored from durable file after restart).

### Catch-up payload

| Domain | Payload |
|--------|---------|
| BROKER config | Full **effective** six knobs via `describe_broker_configs()` stamped with controller `config_generation` (ensures DELETE/default convergence, not sparse-only) |
| ACL | Full durable snapshot via `acl_snapshot_wire_bytes()` at `acl_generation` |

Peer apply rules unchanged (Phase 113): ignore `generation <= applied`.

### Controller promote / restart

| Event | Behavior |
|-------|----------|
| Controller process restart | Load gens from `__cluster_admin`; next Alter continues from durable max; lagging peers catch up on heartbeat |
| Lower-id controller death | New lowest live id is controller; its durable gens/state are SoT; lagging peers catch up when they heartbeat to the new controller |
| Peer offline during Alter | Immediate fan-out fails (Phase 113 metrics); on rejoin heartbeat, catch-up re-push restores |

Optional stretch (not required if heartbeat lag covers it): proactive push to
all live peers when `is_controller` flips true. MVP relies on heartbeat lag
detection (peers heartbeat every `session/3`).

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_config_generation` / `volant_applied_config_generation` | gauge | Phase 113 (retained) |
| `volant_acl_generation` / `volant_applied_acl_generation` | gauge | Phase 113 (retained) |
| `volant_cluster_admin_catchup_success_total` | counter | Successful catch-up RPC applies (config and/or ACL unit) |
| `volant_cluster_admin_catchup_errors_total` | counter | Catch-up RPC / apply failures |

---

## Tests

| File | Cases |
|------|-------|
| `phase117_admin_catchup.rs` | (1) Offline peer misses Alter → restart/rejoin → heartbeat catch-up restores BROKER knobs + gen; (2) Offline peer misses CreateAcls → rejoin → ACL authorize + gen; (3) Controller restart preserves durable gens so next Alter is accepted by peers; (4) Unit: durable gens roundtrip; single-node no catch-up traffic |
| Regression | `phase113_broker_config_fanout`, `phase113_acl_fanout` |

Harness: multi-broker in-process listeners (same pattern as Phase 113/116).

---

## Exit criteria

1. `cargo test -p volant-broker --test phase117_admin_catchup` green  
2. `cargo test -p volant-broker --test phase113_broker_config_fanout` green  
3. `cargo test -p volant-broker --test phase113_acl_fanout` green  
4. Spec + ROADMAP / PHASE_HISTORY / INDEX / consistency / ops / features /
   KAFKA_COMPAT honest updates  
5. Single-node admin path unchanged  
6. Commit on `main`

---

## Honest limitations (after ship)

- Still **not Raft** — brief lag until next successful heartbeat + catch-up RPC  
- Catch-up BROKER payload is **full effective** six keys (may expand peer sparse
  overlay beyond keys the operator explicitly altered)  
- New controller that **never received** a prior push (offline during all Alters)
  may re-push **stale** local state at its durable gen; operators should Alter
  again on the new controller if that node was dark for all admin ops  
- Inter-broker admin RPCs still not ACL-gated (shared-token / TLS)  
- Not multi-DC / async multi-region SoT  

---

## Still deferred after Phase 117

- Multi-broker session handoff / affinity routing  
- Full KIP-890/939 / `__transaction_state`  
- Raft / dynamic membership  
- Multi-lang clients / chaos-mesh / long fuzz  
- Transparent EndTxn forward  
- Per-broker BROKER config overrides  
- Outbox handoff on leadership change (DeleteRecords)  

---

## Decision log (locked for this phase)

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| Catch-up transport | Controller re-push (opcodes 72–75) | New pull-response payload shape |
| Lag signal | Heartbeat applied-gen piggyback | Separate generation poll RPC |
| Generation durability | `__cluster_admin/state.json` per node | Memory-only (broken across restart) |
| Config catch-up body | Full effective six knobs | Sparse-only (DELETE drift) |
| Promote path | Heartbeat lag to new controller | Mandatory cluster-wide re-push on flip |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 117 |
| [consistency.md](./consistency.md) | Catch-up + durable gens honesty |
| [ops.md](./ops.md) | Metrics + rejoin note |
| [features.md](./features.md) | Close permanent-drift limitation |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Cluster admin SoT note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 117 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started (implementation)

```bash
cargo test -p volant-broker --test phase117_admin_catchup
cargo test -p volant-broker --test phase113_broker_config_fanout
cargo test -p volant-broker --test phase113_acl_fanout
```
