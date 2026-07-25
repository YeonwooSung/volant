# Phase 125 — Time-based ISR lag shrink (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** `replica_lag_max_ms` config + last-caught-up timestamps + time shrink helpers — **landed**  
- **PR2** ReplicaFetch / apply_local_assignment wire-up + metrics — **landed**  
- **PR3** multi-node time-lag shrink + catch-up rejoin + Phase 118 interaction tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — Track B follow-on to Phase 118: offset lag alone
cannot drop a slow-but-alive follower whose LEO stays *just* inside
`replica_lag_max_messages` for a long wall-clock interval (e.g. quiet topics or
a stalled fetch loop that still looks “close enough” on message lag).

## Goals

1. **Time-based ISR shrink:** A live in-ISR follower whose last *caught-up-enough*
   observation is older than a configurable max lag **duration** is removed from
   the ISR on the next leader-side reconcile (ReplicaFetch or ClusterState apply
   on the leader), even if message lag is still ≤ `replica_lag_max_messages`.
2. **Second criterion alongside Phase 118:** Offset lag shrink and rejoin gates
   remain unchanged. Time lag is an additional shrink path only.
3. **Honest rejoin:** Once a recovering follower ReplicaFetches with LEO ≥ HWM
   and lag ≤ `replica_lag_max_messages`, it re-expands (Phase 118). Time lag does
   **not** block rejoin after catch-up; rejoin stamps a fresh last-caught-up time.
4. **Config:** `replica_lag_max_ms` in `cluster.toml` (default 30_000; `0` =
   disable time shrink). Optional env override `VOLANT_REPLICA_LAG_MAX_MS`.
5. **Metrics:** dedicated time-shrink counter + existing expand/shrink totals.
6. Integration tests + living-docs honesty (still no preferred-replica).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full Kafka `replica.lag.time.max.ms` parity | MVP uses monotonic last-caught-up; not Kafka’s full replica state machine |
| Preferred replica / rack-aware fetch | Orthogonal deferred product |
| Raft / dynamic membership | Out of scope |
| Leader→controller ISR report RPC | Same Metadata lag honesty as Phase 118 |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Registry GC / 114 prepare compensate / group failover | Separate backlog |
| Rewrite of Phase 114–124 history | Forbidden |

## Problem (today — post Phase 118)

```text
  death → ISR shrink + HWM recompute                         ✅ (108/110)
  message lag > replica_lag_max_messages → ISR shrink        ✅ (118)
  LEO ≥ HWM ∧ lag ≤ max → rejoin                             ✅ (118)
  follower alive, lag ≤ max for a long wall-clock time
      (stalled / very slow fetch while topic quiet or lag
       stays under the large message threshold)              ⚠ never time-shrunk
```

Phase 118 deferred time-based lag explicitly.

## Design principles

1. **Smallest honest design** — per-follower monotonic `Instant` of last
   “caught up enough” observation; not a full Kafka replica manager.
2. **Caught up enough** = observed LEO lag ≤ `replica_lag_max_messages` (same
   threshold as ISR stay/rejoin). Wall clock does not replace offset math for
   rejoin.
3. **`0` disables** time shrink so ops can keep Phase 118-only behaviour.
4. **Leader-local produce/HWM SoT** — same as Phase 118.
5. **No Raft / static membership.**

---

## Architecture

### State (leader partition)

| Field | Role |
|-------|------|
| `follower_leo: HashMap<u32, u64>` | Existing LEO observations (Phase 6/118) |
| `follower_caught_up_at: HashMap<u32, Instant>` | Last time lag ≤ `replica_lag_max_messages` was observed for that replica |

### Stamp rules

On leader-side LEO observation (`handle_replica_fetch`, and test LEO helpers when
cluster-aware):

```text
part.follower_leo[replica_id] = leo
if leader_leo.saturating_sub(leo) <= max_lag_messages:
    part.follower_caught_up_at[replica_id] = Instant::now()
```

On successful rejoin (expand into ISR): stamp `Instant::now()` for the rejoined id.
On death shrink: drop LEO + caught-up entries for the dead id.
Missing stamp → **do not** time-shrink that member (no evidence yet); offset lag
still applies.

### ReplicaFetch reconcile (leader)

```text
1. update follower_leo + maybe stamp caught-up
2. new_isr = shrink_isr(...)                 // offset lag (Phase 118)
3. new_isr = shrink_isr_by_time(..., now)    // Phase 125; no-op if max_ms == 0
4. if fetching replica eligible: expand_isr  // Phase 118 rejoin; stamp on add
5. ensure leader ∈ new_isr
6. if changed: metrics (expand / shrink / time_shrink) + HWM + assignment
```

### ClusterState apply (leader preserve)

Unchanged preserve gate (live + lag ≤ max + LEO ≥ HWM), then:

```text
offset shrink → time shrink → recompute HWM
```

so a preserved set cannot keep a member that is time-stale on apply.

### Config

| Knob | Default | Role |
|------|---------|------|
| `replica_lag_max_messages` | `10000` | Offset lag stay/rejoin (Phase 6/118) |
| `replica_lag_max_ms` | `30000` | Max age of last caught-up stamp before time shrink; `0` = off |
| `VOLANT_REPLICA_LAG_MAX_MS` | (unset) | Process env override of effective max ms when set |

Env override is applied when reading the effective knob on the broker (does not
mutate durable `cluster.toml`).

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_isr_expand_total` | counter | Unchanged (Phase 118) |
| `volant_isr_shrink_total` | counter | Unchanged — all removals (death, offset lag, time lag) |
| `volant_isr_time_shrink_total` | counter | Removals attributed to time lag (Phase 125) |

## Contract preserved

- Phase 118 rejoin: LEO ≥ HWM and lag ≤ `replica_lag_max_messages`
- `acks=all` / `|ISR| < min_insync_replicas` unchanged
- Single-node: no ReplicaFetch reconcile; time shrink idle
- Death path still removes LEO + stamps for the dead id

## Tests

`crates/volant-broker/tests/phase125_isr_time_lag.rs`:

1. Slow-but-alive follower stays within message lag but exceeds `replica_lag_max_ms` → ISR time shrink + metric
2. Catch-up ReplicaFetch after time shrink → rejoin (Phase 118 path)
3. Single-node unchanged (time shrink metric stays 0)
4. Offset lag shrink (Phase 118) still works with time lag enabled (interaction)
5. `replica_lag_max_ms = 0` disables time shrink

Unit tests in `assignment.rs` for `shrink_isr_by_time`.

## Files

| Path | Change |
|------|--------|
| `crates/volant-broker/src/cluster/config.rs` | `replica_lag_max_ms` field + default |
| `crates/volant-broker/src/cluster/assignment.rs` | `shrink_isr_by_time` + `reconcile_isr` time step + unit tests |
| `crates/volant-broker/src/partition.rs` | `follower_caught_up_at` map |
| `crates/volant-broker/src/broker.rs` | stamp + time shrink on fetch/apply/death; metric; env override; test helper |
| `crates/volant-broker/src/net.rs` | Prometheus exposition |
| `crates/volant-broker/tests/phase125_isr_time_lag.rs` | Integration tests |
| `docs/PHASE125_SPEC.md` | This spec |
| Living docs | ROADMAP, PHASE_HISTORY, INDEX, README, consistency, ops, features, KAFKA_COMPAT |
| `examples/cluster.toml`, `docs/cluster.toml` | Document new knob |

## Honest limitations

- Static membership only
- Not full Kafka `replica.lag.time.max.ms` / replica manager parity
- Preferred-replica / rack-aware consumer fetch still deferred
- Controller durable assignment / Metadata ISR may lag when leader ≠ controller
- Time shrink only evaluates on leader reconcile events (ReplicaFetch / apply),
  not a dedicated background ISR timer (lazy evaluation is honest MVP)
- Monotonic `Instant` is process-local (not wall-clock durable across restart)

## Still deferred after this

- Preferred replica / shared session store
- Registry GC / TTL for durable txn coordinator map
- 114 prepare compensate path polish
- Group failover hardening
- Multi-lang clients / chaos-mesh / long fuzz
- Full KIP-890/939 / `__transaction_state`
- Raft / dynamic membership / consensus truncate journal
