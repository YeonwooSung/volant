# Phase 118 — ISR rejoin + lag-based shrink (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** `reconcile_isr` / rejoin when LEO ≥ HWM and lag ≤ threshold + metrics — **landed**  
- **PR2** ClusterState apply preserves leader-local in-sync rejoin members — **landed**  
- **PR3** multi-node death→shrink→catch-up→re-expand + lag-shrink tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — Track B gap after Phase 108/110: death shrink
is strong; **re-expand after follower recovery** and **lag-based shrink of
slow-but-alive ISR members** become first-class, tested product paths.

## Goals

1. **ISR re-expand / rejoin:** After Phase 108/110 death shrink (or lag shrink),
   a recovering follower that `ReplicaFetch`es until its LEO is within
   `replica_lag_max_messages` of the leader LEO **and** at least the current
   committed HWM becomes eligible for ISR add again.
2. **Lag-based ISR shrink:** In-ISR followers whose LEO lag exceeds
   `replica_lag_max_messages` are removed on ReplicaFetch even if still
   membership-alive (same knob as Phase 6; now metrics + tests).
3. **HWM correctness:** After rejoin, HWM = min(LEO of live ISR); `acks=all`
   waits on the expanded set again.
4. **Metrics:** `volant_isr_expand_total` / `volant_isr_shrink_total` counters.
5. Integration tests (≥2–3 brokers): death → shrink → catch up → re-expand;
   lag path drops a slow-but-alive follower.
6. Living docs honesty (static membership, no preferred replica, Metadata lag).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Raft / dynamic membership | Out of scope |
| Preferred replica / rack-aware fetch | Orthogonal |
| Time-based lag (`replica.lag.time.max.ms`) | Offset lag only (Phase 6 knob) |
| Leader→controller ISR report RPC | Leader-local ISR is SoT for produce/HWM; Metadata may lag |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Multi-broker session handoff / full KIP-890 | Orthogonal |
| Rewrite of Phase 114–117 history | Forbidden |

## Problem (today — post Phase 108/110)

```text
  follower death → on_broker_death → local ISR shrink + HWM recompute  ✅ (108/110)
  follower restarts + ReplicaFetch catch-up → ISR re-expand              ⚠ weak
      - Phase 6 rejoin existed (lag ≤ max) but no tests, no metrics
      - rejoin could pin HWM when lag ≪ max but LEO still < HWM
      - ClusterState apply could overwrite leader-local rejoin with
        controller assignment still holding the shrunk set
  slow-but-alive follower lag > max → shrink on next ReplicaFetch       ⚠ untested
```

Phase 110 explicitly deferred: “Does not re-add peers to ISR when they reappear.”

## Design principles

1. **Kafka-style ISR** — only in-sync replicas in ISR; rejoin requires catch-up
   fetch; no Raft.
2. **Leader-local produce/HWM SoT** — partition leader’s local ISR drives
   `acks=all` / HWM (same as death path).
3. **Existing knob** — `replica_lag_max_messages` in `cluster.toml` (default
   10_000); no new config required for MVP.
4. **Controller durable assignment** — pure ISR changes still bump generation
   when the **controller** observes them (ReplicaFetch on controller-leader, or
   death). Non-controller leaders update **local** ISR for HWM; ClusterState
   apply **preserves** still-caught-up rejoin members so a later gen pull does
   not silently undo rejoin.
5. **Honest Metadata lag** — non-controller peers’ Metadata ISR may lag until
   controller assignment catches up (static membership, no LeaderAndIsr push).

---

## Architecture

### ReplicaFetch reconcile (leader)

On each successful leader-side `ReplicaFetch` for `replica_id` at `from_offset`:

```text
1. follower_leo[replica_id] = from_offset
2. new_isr = shrink_isr(leader, isr, leader_leo, max_lag, leo_of)
   // drop members with leader_leo - leo > max_lag
3. if replica_id ∈ replicas ∧ replica_id ∉ new_isr:
     lag = leader_leo - from_offset
     if lag ≤ max_lag ∧ from_offset ≥ committed_hwm:
       new_isr.push(replica_id)          // rejoin
4. ensure leader ∈ new_isr
5. if new_isr ≠ old_isr:
     metrics expand/shrink by set difference
     part.isr = new_isr; recompute_hwm; hwm_cvar.notify
     update assignment.isr; if controller: generation++
```

**Rejoin gate:** LEO must be ≥ committed HWM **and** lag ≤
`replica_lag_max_messages`. Death recovery after further produces therefore
requires the follower to fetch up through the committed frontier before
rejoining (does not pin HWM to a stale LEO).

**Lag shrink:** Same `shrink_isr` as Phase 6; any ReplicaFetch re-evaluates the
full ISR, so a slow member is dropped when another (or itself) fetches.

### ClusterState apply (leader preserve)

When applying assignment on partitions **this node leads**:

```text
assignment ISR applied via ensure_partition
union with previous local ISR members that are still:
  ∈ replicas, live in membership, lag ≤ max_lag, leo ≥ committed_hwm
then lag-shrink + recompute_hwm
```

Prevents a controller generation bump that still lists a shrunk ISR from
undoing a leader-local rejoin that is still valid.

### Death path (unchanged contract)

`on_broker_death` / `shrink_local_isr_for_dead` still remove the dead id from
local ISR on every observer; controller still updates durable assignment +
generation (Phase 108). Phase 118 increments `isr_shrink_total` for each
partition membership removal.

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| `volant_isr_expand_total` | counter | Replica ids added to an ISR |
| `volant_isr_shrink_total` | counter | Replica ids removed from an ISR (death or lag) |

### Config

| Knob | Default | Role |
|------|---------|------|
| `replica_lag_max_messages` | `10000` | Max `leader_leo - replica_leo` to stay in / rejoin ISR |

No new broker config keys. Tune via `cluster.toml` (same as Phase 6).

## Contract preserved

- `acks=all` waits for remaining / expanded live ISR LEOs
- `|ISR| < min_insync_replicas` → `NotEnoughReplicas` (15) before append
- Empty-ISR restore-last-known on controller death path (Phase 108)
- Single-node: ISR = `[self]`, no ReplicaFetch reconcile traffic

## Tests

`crates/volant-broker/tests/phase118_isr_rejoin.rs`:

1. Follower death shrinks ISR; ReplicaFetch catch-up re-expands; HWM advances
2. Slow-but-alive follower with lag > `replica_lag_max_messages` dropped from ISR
3. Rejoin metrics increment; expand blocked while LEO < HWM
4. ClusterState apply with shrunk assignment preserves caught-up local rejoin

## Files

| Path | Change |
|------|--------|
| `crates/volant-broker/src/cluster/assignment.rs` | `expand_isr` / `reconcile_isr` helpers + unit tests |
| `crates/volant-broker/src/broker.rs` | ReplicaFetch rejoin gate; metrics; apply preserve; death metrics |
| `crates/volant-broker/src/net.rs` | Prometheus exposition for expand/shrink |
| `crates/volant-broker/tests/phase118_isr_rejoin.rs` | Integration tests |
| `docs/PHASE118_SPEC.md` | This spec |
| `ROADMAP.md`, `docs/history/PHASE_HISTORY.md`, `docs/INDEX.md`, `README.md` | Living docs |
| `docs/consistency.md`, `docs/ops.md`, `docs/features.md`, `docs/KAFKA_COMPAT.md` | Honesty |

## Honest limitations

- Static membership only (no dynamic join / leave of brokers)
- No preferred-replica / rack-aware consumer fetch
- Offset lag only — no time-based `replica.lag.time.max.ms`
- Controller durable assignment may lag when the partition leader is **not**
  the controller (produce/HWM still correct on leader-local ISR)
- Metadata ISR on non-leaders may lag until controller assignment updates
- Not multi-DC / not Raft

## Still deferred after this

- Multi-lang clients / chaos-mesh / long fuzz
- Multi-broker session handoff
- Full KIP-890/939 / `__transaction_state`
- Transparent EndTxn forward
- Outbox handoff on leadership change
- Per-broker BROKER config overrides
- Time-based ISR lag / preferred replica
