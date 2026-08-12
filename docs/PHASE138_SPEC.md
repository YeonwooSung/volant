# Phase 138 — Shared fetch session mirror + promote (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Best-effort **peer mirror** of fetch sessions so owner death does not
always force client recreate (**70**). Keeps Phase 119 forward as the live
happy path. **Not** Raft / not controller session SoT.

## Goals

1. **Mirror put/delete RPCs** (native inter-broker):
   - Opcode **90** `FetchSessionMirrorPut` — `session_id i32` + snapshot `Bytes`
     (JSON of one durable session entry: id, epoch, last_activity_ms, topics/omit)
   - Opcode **91** response — `error_code u16`
   - Opcode **92** `FetchSessionMirrorDelete` — `session_id i32`
   - Opcode **93** response — `error_code u16`
2. **Foreign mirror table** on each broker (`FetchSessionManager`):
   - `install_mirror` / `remove_mirror` / `mirror_contains` / `mirrored_count`
   - `export_session_bytes` for put payload
   - `promote_from_mirror(session_id) -> bool` moves mirror → primary table
3. **Dirty queue + best-effort fan-out** after primary session mutations
   (create / begin_incremental / merge / forget / note_returned → Put;
   close / FINAL → Delete). Cluster only; fire-and-forget with inter-broker
   timeout; client Fetch must not fail if mirror put fails.
4. **Promote on owner miss:** `maybe_forward_kafka_fetch` on forward failure
   (or missing owner addr): if mirror present → promote → return `None` so
   local `encode_fetch` serves. Else existing empty Fetch **70**.
5. Metrics: `volant_fetch_session_mirror_puts_total` / `_errors_total`,
   `volant_fetch_session_mirror_deletes_total`, `volant_fetch_session_promote_total`,
   gauge `volant_fetch_sessions_mirrored`.
6. Tests `phase138_shared_fetch_sessions` + living docs 0–138.

## Non-goals

| Deferred | Why |
|----------|-----|
| Raft / controller session registry | Hot SoT; larger than MVP |
| Multi-writer serve from mirror without promote | Dual-epoch races |
| Re-encode session_id to new owner | Client recreate; keep id stable |
| Debounced mirror put | Chatty full put OK for MVP |
| Preferred re-home of session owner | Orthogonal |
| Change Kafka public wire | Unchanged |

## Design

```text
  Create/mutate on owner A
       │
       ├─ local primary + __fetch_sessions (Phase 115)
       └─ best-effort MirrorPut (90) → live peers
              peers keep foreign mirror map (not served while A alive)

  Client → B, A alive, session owned by A:
       B miss primary → KafkaFetchForward 82 → A  (Phase 119 unchanged)

  Client → B, A unreachable:
       B forward fails
       B promote_from_mirror(session_id)?
            yes → local encode_fetch (omit cache preserved)
            no  → empty Fetch top-level 70
```

### Promote rules

1. Only after forward fail or missing owner addr (never while local primary hit).
2. Mirror must exist; promote inserts into primary under same `session_id`.
3. Session_id owner bits may still name dead A — routing: local hit first.
4. Dual-promote race → possible later **71**; client recreates (honest).

## Honest limitations

- Best-effort mirror; put lag/fail ⇒ still **70**.
- Not dual-master while owner alive.
- Full snapshot put per mutation can be chatty.
- READ_COMMITTED / preferred residual unchanged.
- Single-node: no fan-out.

## Exit criteria

1. [x] Create on A → B/C `mirror_contains` true  
2. [x] Happy path: B still forwards while A up (primary empty on B)  
3. [x] Kill A → incremental on B promotes → error 0; omit still works if HWM stable  
4. [x] Wrong epoch after promote → **71**  
5. [x] No mirror + owner dead → **70**  
6. [x] phase119/115/91/95 regression green  
7. [x] Living docs 0–138 updated  

## Tests

**Formal Phase 138:**

- `crates/volant-broker/tests/phase138_shared_fetch_sessions.rs`
  - create on owner → peers install foreign mirror
  - happy path still Phase 119 forward while owner alive
  - owner miss + mirror → promote → local encode_fetch
  - wrong epoch after promote → **71**; no mirror → **70**
- Unit: export/install/promote/mirror_contains in `crates/volant-broker/src/kafka/fetch_session.rs`

**Regression:** phase119 / 115 / 91 / 95 fetch-session suites.

## Protocol

| Opcode | Name |
|-------:|------|
| 90 | FetchSessionMirrorPut req |
| 91 | FetchSessionMirrorPut resp |
| 92 | FetchSessionMirrorDelete req |
| 93 | FetchSessionMirrorDelete resp |

No Kafka public wire change (inter-broker native only).

## Implementation notes (shipped)

- Protocol: native inter-broker opcodes **90–93** (`FetchSessionMirrorPut` /
  `FetchSessionMirrorDelete` req/resp); put body = `session_id i32` + snapshot
  `Bytes` (JSON durable session entry: id, epoch, last_activity_ms, topics/omit).
- `FetchSessionManager`: foreign mirror table (`install_mirror` / `remove_mirror`
  / `mirror_contains` / `mirrored_count`); `export_session_bytes`;
  `promote_from_mirror(session_id) -> bool` moves mirror → primary under the
  same `session_id` (no re-encode of owner bits).
- Dirty queue + best-effort fan-out after primary mutations (create /
  begin_incremental / merge / forget / note_returned → Put; close / FINAL →
  Delete). Cluster only; fire-and-forget with inter-broker timeout; client Fetch
  does not fail if mirror put fails.
- Promote on owner miss: `maybe_forward_kafka_fetch` on forward failure (or
  missing owner addr) promotes if mirror present then returns `None` so local
  `encode_fetch` serves; else existing empty Fetch **70**. Happy path still
  Phase 119 forward while owner is alive.
- Metrics (Prometheus): `volant_fetch_session_mirror_puts_total`,
  `volant_fetch_session_mirror_deletes_total`,
  `volant_fetch_session_promote_total`, gauge `volant_fetch_sessions_mirrored`.
- Residual honesty: not Raft / not controller SoT; best-effort (put lag/fail
  still **70**); dual-promote race may yield later **71**; preferred selector
  still orthogonal (Phase 126/133); single-node no fan-out.
