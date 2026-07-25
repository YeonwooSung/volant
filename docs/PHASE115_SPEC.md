# Phase 115 — Durable fetch sessions (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable session table under `{data_dir}/__fetch_sessions` + load/persist — **landed**  
- **PR2** Broker open-path restore + metrics — **landed**  
- **PR3** restart / omit-unchanged integration tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Fetch session affinity honesty — session_id → partition fetch state
(omit cache, epoch) survives **broker restart on the same data_dir**, closing the
main “process-local only / lost on restart” gap from Phases 88/91/95.

## Goals

1. **Durable local sessions:** Persist active Fetch sessions under
   `{data_dir}/__fetch_sessions/state.json` so a single-broker (or per-broker)
   restart restores `session_id` → topics/partitions, epoch, activity, and
   Phase 91 `last_hwm`/`last_lso` omit cache (within idle TTL).
2. **TTL-aware restore:** On load, drop sessions already idle past
   `fetch_session_idle_ms` (same clock rules as Phase 95 lazy/sweeper eviction).
3. **Wire compatibility unchanged:** create, forgotten, 70/71, FINAL_EPOCH close,
   omit-unchanged empty-topics incremental — same as Phases 49/54/84/88/91/95.
4. **Metrics:** restore count + persist errors (and existing active/evicted gauges).
5. Integration tests for restart survival + living-docs honesty.
6. Document multi-broker sticky routing as **convention only** this phase
   (no inter-broker session handoff).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Multi-broker session handoff / forward | Client sticky to session-owner broker; later phase |
| Replicated / shared session store across brokers | Storage design; not Kafka consumer-session replication |
| Byte-identical Kafka response cache | Beyond Phase 91 HWM+LSO omit |
| Raft / dynamic membership | Out of scope |
| Multi-lang clients, chaos-mesh / long fuzz | Orthogonal |
| Full KIP-890/939 / `__transaction_state` | Closed partially by 114; remainder orthogonal |
| Debounced / async persist for throughput | Possible follow-up; MVP fsyncs snapshot on mutation |

## Problem (today)

```text
  Fetch create ──► session_id=S on broker A (RAM only)
  Broker A restart
  Fetch incremental session_id=S ──► FETCH_SESSION_ID_NOT_FOUND (70)
  omit-unchanged cache gone; client must full-fetch recreate
```

Sticky load-balancers and rolling restarts break incremental sessions even when
the client reconnects to the **same** node and data_dir. Multi-broker miss is a
separate affinity problem; restart on one node is the highest-value honesty fix.

## Design principles

1. **Per-broker local durability only** — same class as `__txn_prepared`,
   `__producer_state`, `__broker_config`. Controller is **not** SoT for sessions.
2. **No new wire keys / opcodes** — Kafka Fetch session_id/epoch behavior unchanged.
3. **No Raft / shared session table** — multi-broker affinity remains
   sticky-by-convention (client → same broker).
4. **Idle TTL still wins** — restored sessions past idle are dropped at load
   and never reappear as active.
5. **Honest gaps** — not multi-broker sticky; not a Kafka consumer group session
   store; persist is best-effort atomic file replace (crash mid-write may lose
   the last mutation).

---

## Architecture

### On-disk layout

`{data_dir}/__fetch_sessions/state.json`:

```json
{
  "version": 1,
  "next_id": 4,
  "sessions": [
    {
      "id": 1,
      "epoch": 2,
      "last_activity_ms": 1700000001000,
      "topics": [
        {
          "key": "orders",
          "wire_kind": "name",
          "wire_name": "orders",
          "wire_uuid_hex": null,
          "name": "orders",
          "partitions": [
            {
              "id": 0,
              "fetch_offset": 1,
              "current_leader_epoch": -1,
              "last_fetched_epoch": -1,
              "max_bytes": 1000000,
              "last_hwm": 1,
              "last_lso": 1
            }
          ]
        }
      ]
    }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `version` | File format version (`1`) |
| `next_id` | Next positive session id allocator (skip 0) |
| `epoch` | Next expected client `session_epoch` (same as in-memory) |
| `last_hwm` / `last_lso` | Phase 91 omit cache; optional null |

### When to persist

| Path | Persist? |
|------|----------|
| Create | Yes (after insert + any LRU eviction) |
| Valid incremental (`begin_incremental`) | Yes (epoch + activity) |
| `merge_topics` / `forget` | Yes |
| `note_returned` | Yes (omit cache) |
| Close / FINAL | Yes (removal) |
| Idle / LRU eviction | Yes |
| Session error 70/71 (no mutation) | No |
| Load at boot | N/A (read); may rewrite if idle dropped |

### Load algorithm

```text
open path; if missing → empty table, next_id=1
parse JSON; on error → empty table (do not crash broker)
for each session:
  if idle_ttl > 0 and now - last_activity_ms > idle_ttl: drop (count as idle eviction)
  else: insert into RAM map
next_id = max(file.next_id, max(ids)+1, 1)
restored = sessions.len()
optional: rewrite file if any idle dropped
```

### Multi-broker honesty (deferred handoff)

```text
  Session opened on broker N  ──sticky──► client should keep Fetch to N
  Client hits broker M with session_id from N  ──► 70 (not found)
  No inter-broker forward / Metadata “session owner” this phase
```

Ops guidance: pin Fetch TCP connections (or LB stickiness) to the broker that
created the session. Durable restore only helps **same node + same data_dir**
restart.

### Metrics

| Metric | Type | Source |
|--------|------|--------|
| `volant_fetch_sessions_restored` | gauge | Sessions loaded at last open (post idle filter) |
| `volant_fetch_sessions_persist_errors_total` | counter | Failed write/rename of state.json |
| Existing active / evicted / idle_evicted | unchanged | Phase 95/97 |

---

## Tests

| Test file | Cases |
|-----------|-------|
| `phase115_durable_fetch_sessions.rs` | Create session + note omit cache → drop broker → reopen same `data_dir` → incremental epoch OK + omit-unchanged empty-topics; FINAL close not restored; idle-expired not restored (unit or short TTL) |
| Unit (`fetch_session.rs`) | Persist/load roundtrip; next_id continuity; idle filter on load |
| Regression | `phase88_*`, `phase91_*`, `phase95_*` still green |

---

## Exit criteria

1. Session created on broker A, process drop + `Broker::new` same `data_dir` →
   incremental with same id/epoch succeeds within idle TTL  
2. Omit-unchanged still works after restore (HWM+LSO cache restored)  
3. Idle-expired sessions not restored as live  
4. FINAL / close not present after restart  
5. Metrics exposed; living docs updated; no false multi-broker sticky claim  
6. `cargo test -p volant-broker --test phase115_durable_fetch_sessions` green  
7. Workspace builds  

---

## Honest limitations (after ship)

- **Not** multi-broker session affinity / handoff — wrong broker ⇒ **70**  
- **Not** a replicated Kafka-style fetch session cache across the cluster  
- Persist is **full snapshot + fsync** on mutation (may be expensive under high
  Fetch QPS; debounce deferred)  
- Crash mid-rename may lose the last mutation (empty or previous snapshot)  
- `session_id` space is per data_dir / broker process lineage, not cluster-global  
- Wall-clock idle on restore (same as Phase 95)  

---

## PR plan (DAG)

```text
PR1  durable store + FetchSessionManager load/persist
 │
 ├─► PR2  Broker open + metrics
 │         │
 │         └─► PR3  phase115 tests
 │                   │
 └───────────────────┴─► PR4  living docs
```

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| MVP slice | Durable **local** sessions (restart) | Full multi-broker handoff first |
| SoT | Per-broker `data_dir` file | Controller / Raft session table |
| Persist cadence | On every mutating op | Debounced sweeper-only (weaker omit cache) |
| Multi-broker miss | Document sticky + 70 | Invent forward RPC this phase |
| Wire | Unchanged Fetch session_id/epoch | New Metadata session-owner field |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 115 |
| [ops.md](./ops.md) | Sticky Fetch; `__fetch_sessions`; metrics |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | Durable local sessions; not multi-broker sticky |
| [features.md](./features.md) / [INDEX.md](./INDEX.md) | Short honesty line |
| [consistency.md](./consistency.md) | Session locality note if present |
| [../ROADMAP.md](../ROADMAP.md) | Phase 115 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase115_durable_fetch_sessions
cargo test -p volant-broker --test phase91_omit_unchanged_sessions
cargo test -p volant-broker --test phase95_fetch_session_limits
cargo test -p volant-broker --lib kafka::fetch_session
```
