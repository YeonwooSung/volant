# Phase 124 — Durable txn coordinator registry (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable registry under `{data_dir}/__txn_coordinator` + load/persist — **landed**  
- **PR2** Broker open-path restore + note/update persist + metrics — **landed**  
- **PR3** restart / multi-node forward + unit roundtrip tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — the Phase 120–122 Init-owner / txn coordinator
registry survives **broker restart on the same data_dir**, so transparent EndTxn
/ AddOffsets / TxnOffsetCommit forward and sticky FindCoordinator override keep
working after crash without waiting for re-Init or open fan-out.

## Goals

1. **Durable local registry:** Persist known
   `(transactional_id → coordinator node_id)` and
   `(producer_id → coordinator node_id)` under
   `{data_dir}/__txn_coordinator/state.json`.
2. **Load on open:** `Broker::new` / `with_cluster` restores the maps from disk
   so peers and the Init owner remember prior ownership after restart.
3. **Persist on change:** Every `note_txn_coordinator` (Init registration, open
   fan-out install, re-Init overwrite) writes an atomic snapshot.
4. **Crash/restart honesty:** After restart, a non-coordinator still forwards
   Kafka EndTxn / AddOffsets / TxnOffsetCommit when the registry entry was
   durable; sticky FindCoordinator still prefers the known Init owner when live.
5. Integration / unit tests for restart survival + living-docs honesty.
6. Single-node unchanged (maps may still persist locally; no forward path).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `__transaction_state` / KIP-890/939 | Separate storage design |
| Controller / Raft shared registry | Orthogonal; local table matches Phase 115 |
| Full GC / TTL expiry of completed txn mappings | Optional follow-up; overwrite on re-Init is enough for MVP |
| Producer epoch / open SoT durability redesign | Already partially covered by `__producer_state` / `__txn_prepared` |
| Dynamic membership / Raft | Orthogonal |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Rewrite of Phase 114–123 history | Forbidden |

## Problem (today — post Phase 122/123)

```text
  Init on coordinator A ──► registry on A + fan-out to peers B, C (RAM only)
  Broker B restart
  EndTxn / AddOffsets on B ──► resolve_txn_coordinator miss
  ──► local path / UnknownProducerId (until re-Init or open fan-out)
  FindCoordinator(txn) ──► sticky hash only (Init-owner override lost on B)
```

Phases 120–122 closed misroute **while the registry host stayed up**. Restart
wiped the Init-owner map on that node even when `data_dir` was intact.

## Design principles

1. **Per-broker local durability only** — same class as `__fetch_sessions`,
   `__delete_records_outbox`, `__txn_prepared`. Controller is **not** SoT for
   the coordinator map.
2. **No new wire keys / opcodes** — Kafka client paths and 84/85 unchanged.
3. **At-least-once OK** — stale entries for completed txns may linger until
   re-Init overwrites them; forward to a live Init owner remains correct for
   active/known transactional_ids.
4. **Atomic snapshot** — write `state.json.tmp` + fsync + rename (Phase 115/116).
5. **Honest gaps** — not cluster-wide consensus; wrong data_dir or wiped disk
   still loses the map; Init owner process loss still loses producer SoT
   (pre-existing).

---

## Architecture

### Chosen design: **local durable table (Phase 115 pattern)**

| Piece | Role |
|-------|------|
| `{data_dir}/__txn_coordinator/state.json` | Snapshot of known mappings |
| Load on broker open | Rebuild `by_id` + `by_pid` maps |
| `note_txn_coordinator` | Update RAM + persist snapshot |
| `resolve_txn_coordinator` | Unchanged lookup order (id → prepared index → pid) |
| FindCoordinator override | Still uses restored registry |

### On-disk layout

`{data_dir}/__txn_coordinator/state.json`:

```json
{
  "version": 1,
  "by_id": {
    "orders-txn": 2,
    "payments-txn": 1
  },
  "by_pid": {
    "7": 2,
    "9": 1
  }
}
```

| Field | Meaning |
|-------|---------|
| `version` | File format version (`1`) |
| `by_id` | `transactional_id` → Init-owner `node_id` |
| `by_pid` | `producer_id` (JSON string keys) → Init-owner `node_id` |

Maps mirror the in-memory Phase 120 tables exactly (no ambiguous id↔pid pairing).

### Load algorithm

```text
open path; if missing → empty maps
parse JSON; on error → empty maps (do not crash broker)
by_id = file.by_id (drop empty keys / zero coords)
by_pid = file.by_pid (drop zero coords)
restored = by_id.len() + by_pid.len()
```

### Persist algorithm

```text
on note_txn_coordinator(txn_id, pid, coord):
  if coord == 0: return
  update by_id / by_pid in RAM
  write full snapshot (pretty JSON) via tmp + fsync + rename
  on failure: increment persist_errors; keep RAM view
```

### Clear / GC

MVP does **not** remove entries on EndTxn complete (memory maps already kept
mappings for the process lifetime). Re-Init with a new pid **overwrites** the
transactional_id entry and adds/updates the pid entry. Optional bounded GC is
deferred; document stale completed-txn entries as honest.

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_txn_coordinator_registry_restored` | gauge | Entries restored at last open |
| `volant_txn_coordinator_registry_persist_errors_total` | counter | Failed snapshot writes |

Existing `volant_txn_forward_*` unchanged.

---

## Contract preserved

- Phase 120 EndTxn forward + Phase 122 offset forward resolve order
- Phase 121 sticky FindCoordinator + registry override
- Single-node Phase 18/90 behavior (no cluster ⇒ no forward)
- No dual prepare / dual offset buffer
- Kafka public API keys/versions unchanged

## Tests

`crates/volant-broker/tests/phase124_durable_txn_coordinator.rs`:

1. Unit/store: note → drop → reopen same `data_dir` → resolve by id and pid
2. Multi-node: Init (+ fan-out) on coordinator; restart a **peer** on same
   data_dir → EndTxn (or resolve + forward) via peer still finds owner
3. Idempotent reload: open twice without note → same maps; second open does not
   invent entries
4. Single-node: note/persist still works; no forward required for classic path

Regression band: `phase120_*`, `phase121_*`, `phase122_*`.

## Exit criteria

1. Registry entry survives drop + `Broker::with_cluster` / `new` same `data_dir`  
2. After peer restart, non-coordinator still resolves Init owner and can forward  
3. Sticky FindCoordinator override uses restored map when owner live  
4. Metrics exposed; living docs drop “memory-only registry” honesty gap  
5. `cargo test -p volant-broker --test phase124_durable_txn_coordinator` green  
6. Workspace builds; phase120/121/122 band green  

---

## Honest limitations (after ship)

- **Not** full KIP-890/939 / `__transaction_state`  
- **Not** a replicated / controller-shared coordinator table  
- Stale entries for **completed** txns may remain until overwrite (no GC MVP)  
- Crash mid-rename may lose the last mutation (empty or previous snapshot)  
- Persist is **full snapshot + fsync** on every note (debounce deferred)  
- Init owner **process** loss still loses producer SoT / open state (registry
  alone does not resurrect the coordinator's in-memory producers)  
- Fan-out to a peer that never received the note still needs re-Init/open or
  a future re-broadcast  

---

## PR plan (DAG)

```text
PR1  durable store module + load/persist
 │
 ├─► PR2  Broker open + note_txn_coordinator wire + metrics
 │         │
 │         └─► PR3  phase124 tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| MVP slice | Local durable Init-owner map | Controller SoT / Raft registry |
| Layout | `__txn_coordinator/state.json` | Embed in `__producer_state` |
| Persist cadence | On every note | Debounced sweeper-only |
| GC | None (overwrite on re-Init) | TTL / EndTxn remove this phase |
| Wire | Unchanged 84/85 + FC | New Metadata coordinator-owner field |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 124 |
| [ops.md](./ops.md) | `__txn_coordinator`; restore/persist metrics |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Durable registry honesty |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | Restart + forward note |
| [../ROADMAP.md](../ROADMAP.md) | Phase 124 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase124_durable_txn_coordinator
cargo test -p volant-broker --test phase120_endtxn_forward
cargo test -p volant-broker --test phase121_sticky_find_coordinator
cargo test -p volant-broker --test phase122_txn_offset_forward
```
