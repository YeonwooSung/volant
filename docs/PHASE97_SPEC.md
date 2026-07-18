# Phase 97 — Background txn + session sweeper with metrics (MVP)

## Goals

1. **Periodic background task** that:
   - Calls open + prepared timeout expiry (same paths as lazy expire)
   - Evicts idle fetch sessions (same as lazy idle eviction)
   - Interval configurable: default **1000 ms**; env
     `VOLANT_SWEEP_INTERVAL_MS`; runtime setter; **`0` disables** background
     sweep (lazy paths remain)
2. **Richer metrics** on the existing Prometheus text endpoint:
   - Counters: open txn expired, prepared txn expired, fetch sessions idle-evicted
   - Gauges: open_txns, prepared_txns, fetch_sessions_active (sessions already)
3. Lazy expire paths **remain** — correctness does not depend on the sweeper.
4. Start sweeper from `start_background_tasks` (server entry); no special
   Drop/join (fire-and-forget tokio task, same as group expiry / retention).
5. Tests (`phase97_*.rs`) + living docs / ROADMAP.

## Non-goals

- Multi-broker coordinated clocks
- Multi-lang / fuzz CI
- Durable sessions / multi-broker 2PC
- Full admin DynamicConfig surface
- Stopping/joining the task on Broker drop (Arc-shared; matches existing bg tasks)
- Separate LRU eviction counter labels beyond existing total (idle is split out)

## Design (honest MVP)

### Default choice

| Knob | Default | Rationale |
|------|---------|-----------|
| `sweep_interval_ms` | **1000** | Sub-second enough for timeout correctness without spinning; 1s matches group-session tick order of magnitude |
| Interval `0` | disabled | Ops/tests can keep lazy-only behavior |

### Config surface

| Source | Behavior |
|--------|----------|
| Default | **1000 ms** |
| Env `VOLANT_SWEEP_INTERVAL_MS` | Read at `Broker::new` / `with_cluster` |
| `Broker::set_sweep_interval_ms(ms)` | Runtime override (tests / ops) |
| `Broker::sweep_interval_ms()` | Current interval |
| **`0`** | **Disable background sweep** (lazy paths still run) |

### Lifecycle

`net::start_background_tasks(Arc<Broker>)` (already called from
`volant-server`) **always** spawns a tokio task (Phase 101; originally only
when `sweep_interval_ms > 0`):

```text
loop {
  ms = re-read Atomic
  if ms == 0 { sleep(200ms); continue }  // pause; observe later enable
  sleep(ms)
  if interval still > 0 { broker.sweep_timeouts() }
}
```

`Broker::sweep_timeouts()` is the single entry used by both the background
task and tests:

1. `expire_timed_out_txns()` → open + prepared (Phase 92/93 paths unchanged)
2. `fetch_sessions().evict_idle_now()` → idle TTL only (not LRU pressure)

Lazy paths on txn/LSO APIs and fetch create/begin **remain**.

**Why not spawn from `Broker::new`:** `Broker` is often constructed under tests
without a tokio runtime / without wanting background work. Server entry via
`start_background_tasks` matches group expiry + retention.

### Metrics

| Metric | Type | Source |
|--------|------|--------|
| `volant_open_txns_expired_total` | counter | open timeout aborts (lazy **and** background) |
| `volant_prepared_txns_expired_total` | counter | prepared timeout aborts (lazy **and** background) |
| `volant_fetch_sessions_idle_evicted_total` | counter | idle TTL removals only |
| `volant_fetch_sessions_evicted_total` | counter | idle + LRU (Phase 95; unchanged) |
| `volant_open_txns` | gauge | `open_txns.len()` |
| `volant_prepared_txns` | gauge | `prepared_txns.len()` |
| `volant_fetch_sessions_active` | gauge | Phase 95; unchanged |

Expired counters increment inside `expire_timed_out_open_txns` /
`expire_timed_out_prepared_txns` so both lazy and background paths count once
per aborted txn.

### Semantics table

| Case | Behavior |
|------|----------|
| Default interval 1s, aged open txn | Background aborts without API touch |
| Default interval 1s, aged prepared | Background aborts without API touch |
| Idle session past TTL | Background idle-evicts; next incremental → **70** |
| Interval `0` | No bg task work; lazy expire still works |
| Lazy expire still called | Same abort semantics; counters still increment |
| Concurrent lazy + bg | parking_lot Mutex; second pass is no-op if first won |

## Exit criteria

1. Default interval 1000 ms; env + setter; `0` disables background
2. Background expires open + prepared + idle sessions without API touch
3. Lazy paths still work; counters count both paths
4. Gauges for open/prepared/session counts on `/metrics`
5. `phase97_*` + prior phases green
6. Docs: PHASE97_SPEC + living docs / ROADMAP

## Honest limitations

- Single-node wall clock; no multi-broker coordinated expiry
- Fire-and-forget tokio task (no join on shutdown)
- Interval re-read each loop; task always spawned (Phase 101 closed the
  boot-with-0 gap where no task existed for later `0→>0`)
- Idle session sweep only (LRU still lazy-on-create)
- No per-reason labels on a single counter (separate idle + total series)
- No DynamicConfig / Admin surface for the interval

## Test plan

`crates/volant-broker/tests/phase97_background_sweeper.rs`:

1. Short interval + aged open txn → expires without calling expire API
2. Short interval + aged prepared → expires without calling expire API
3. Short interval + idle session → evicted without create/begin touch
4. Interval `0` → aged open not expired until lazy/manual expire
5. Lazy path still works and increments counters
6. Metrics text contains new series after expiry

## Phase 98 ideas

- Sweep-run / duration histograms; eviction-reason labels
- Admin/DescribeConfigs for timeout + sweep knobs → **closed by Phase 99**
- Mid-txn abortable signals beyond timeout-only
- Graceful sweeper shutdown / join on server stop
- Multi-broker 2PC / session affinity
- Multi-lang clients / cargo-fuzz corpus CI
