# Phase 95 — Fetch session TTL / max sessions (MVP)

## Goals

1. **Idle TTL**: Evict process-local fetch sessions that have been idle longer
   than a configurable timeout. Update `last_activity` on each successful
   session Fetch (create + valid incremental).
2. **Max concurrent sessions**: Cap the number of live sessions. When creating
   a new session at the cap, **evict the LRU idle session** (Kafka-like cache
   pressure) rather than rejecting the create.
3. **Lazy eviction** on session create and incremental begin (same pattern as
   Phase 92/93 txn timeouts — no required background thread).
4. Preserve Phase 88/91: create, forgotten, 70/71, FINAL, omit-unchanged.
5. Tests (`phase95_*.rs`) + living docs honesty.
6. Optional stretch: cheap counters (`fetch_sessions_active`,
   `fetch_sessions_evicted_total`).

## Non-goals

- Multi-broker session affinity / durable / replicated sessions
- Byte-identical Kafka response cache beyond Phase 91 omit-unchanged
- Background sweeper thread (lazy only is enough)
- Multi-lang clients / cargo-fuzz corpus CI
- Multi-broker 2PC

## Design (honest MVP)

### Config

| Knob | Default | Env | Runtime |
|------|---------|-----|---------|
| Idle TTL | **60_000** ms | `VOLANT_FETCH_SESSION_IDLE_MS` | `Broker::set_fetch_session_idle_ms` |
| Max sessions | **1000** | `VOLANT_FETCH_SESSION_MAX` | `Broker::set_fetch_session_max` |

| Special value | Meaning |
|---------------|---------|
| Idle TTL `0` | Disable idle eviction |
| Max sessions `0` | Unlimited concurrent sessions (no LRU cap) |

Defaults mirror Kafka-ish ops defaults (`max.incremental.fetch.session.cache.slots`
≈ 1000) and Volant's other 60s timeout knobs (prepared/open txn).

### Session fields

Extend `FetchSession` with:

| Field | Meaning |
|-------|---------|
| `last_activity_ms: i64` | Unix epoch ms of last successful session Fetch |

### When activity is refreshed

| Path | Touch `last_activity_ms`? |
|------|---------------------------|
| Create (`session_id==0` or INITIAL epoch) | Yes (set to now on insert) |
| Valid incremental (`begin_incremental` Ok) | Yes |
| Session error 70/71 | No |
| FINAL close | N/A (session removed) |
| `note_returned` / merge / forget | No separate touch (already touched on begin/create) |

### Lazy eviction

On **create** and **begin_incremental**:

1. If idle TTL > 0: remove every session with
   `now_ms - last_activity_ms > idle_timeout_ms` (count as evictions).
2. Then proceed with the requested op.

On **create** only, after idle sweep:

3. If max sessions > 0 and `len == max`: remove the **LRU** session
   (lowest `last_activity_ms`; ties → lowest session id) and count eviction.
4. Insert the new session with `last_activity_ms = now`.

### Client-visible behavior after eviction

| Client action | Result |
|---------------|--------|
| Incremental against idle-expired or LRU-evicted id | Top-level **FETCH_SESSION_ID_NOT_FOUND (70)** |
| Create at capacity | **Succeeds**; another session was LRU-evicted |
| Wrong epoch on still-live session | **INVALID_FETCH_SESSION_EPOCH (71)** unchanged |

**Choice rationale (LRU vs reject):** Kafka's fetch session cache evicts under
slot pressure rather than failing every new full Fetch. Rejecting create would
force clients into session-less full fetches permanently until something else
evicts; LRU keeps create always succeeding and surfaces 70 only to the victim
client on its next incremental — honest and client-recoverable (client recreates).

### Metrics (stretch, shipped if cheap)

| Metric | Type | Source |
|--------|------|--------|
| `volant_fetch_sessions_active` | gauge | `sessions.len()` |
| `volant_fetch_sessions_evicted_total` | counter | idle + LRU removals |

Exposed on the existing Prometheus text endpoint (appended from session manager).

## Exit criteria

1. Session idle beyond TTL → next incremental returns **70**
2. At max sessions, new create succeeds; victim session next incremental → **70**
3. Active session within TTL keeps working (epoch advances, omit-unchanged OK)
4. Phase 88/91 tests still green
5. `cargo test -p volant-broker` green (and workspace)
6. Docs: PHASE95_SPEC + living docs / ROADMAP

## Honest limitations

- Process-local only (lost on restart; not multi-broker sticky)
- Lazy eviction only (no background sweeper; idle sessions may linger until
  the next create/incremental on *any* session path)
- LRU uses `last_activity_ms` only (not byte size / partition count)
- No Kafka `max.incremental.fetch.session.cache.slots` dynamic config API
- Clock is single-node wall time (unix ms)

## Test plan

`crates/volant-broker/tests/phase95_fetch_session_limits.rs`:

1. Short idle TTL → sleep past TTL → empty-topics incremental → **70**
2. `max_sessions=2` → create 3 sessions → first (LRU) incremental → **70**;
   newest still works
3. Regression: omit-unchanged empty-topics still omits when session live

Unit tests in `fetch_session.rs` with explicit timestamps (no sleep).

## Deferred (Phase 96+)

- Background session sweeper
- Multi-broker session affinity / durable sessions
- Byte-level response cache / compressed batch reuse
- Session metrics labels (eviction reason: idle vs lru)
- Multi-lang clients; cargo-fuzz corpus CI
- Background txn sweeper / `transaction.max.timeout.ms` clamp

## Phase 96 ideas

- Background txn + session sweeper (periodic, not only lazy)
- `transaction.max.timeout.ms` broker clamp for InitProducerId
- Multi-broker session affinity / sticky routing hints
- Byte-identical / compressed response cache beyond HWM+LSO omit
- Eviction-reason metric labels; session size-weighted LRU
- Mid-txn abortable signals; full KIP-890 multi-broker surface
