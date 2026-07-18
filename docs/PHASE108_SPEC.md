# Phase 108 — Fix rolling restart produce timeout when follower down (MVP)

## Goals

1. Stop `rolling_restart_follower_preserves_data` failing with
   `produce failed with error_code=7` (REQUEST_TIMED_OUT) while a non-leader
   follower is down under `acks=all` / `min.insync.replicas=2`.
2. Prefer honest product ISR/HWM behavior over weakening the test.
3. Keep `acks=all` contract: response only after HWM covers the batch among
   **live** ISR; still reject when `|ISR| < min_insync_replicas`.
4. Living docs honesty; multi-run green under default cargo threads.

## Non-goals

- Accept-loop drain (native / Kafka / metrics)
- Single-flight / idempotent `start_background_tasks`
- Straddle marker clip
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Auto-mark peers dead from heartbeat `alive_brokers` gaps (still controller
  expire + ClusterState pull for remote observation)

## Problem

```text
cargo test -p volant-broker --test phase8_redirect_restart \
  rolling_restart_follower_preserves_data
```

Panic at mid-window produce (follower accept aborted + `test_kill_broker`):

```text
produce while follower down: Io(Custom {
  kind: TimedOut,
  error: "produce failed with error_code=7"
})
```

Error **7** = `Timeout` / REQUEST_TIMED_OUT from the native produce path after
the 10s HWM wait in `net.rs` when `acks=255`.

## Root cause

**Product bug** in `Broker::on_broker_death` (Phase 6 ISR path), not a Phase 106
follower-loop regression.

1. `HWM = min(LEO of every broker currently in ISR)`.
2. On follower death, the controller **did** `pa.isr.retain(|id| live…)` in the
   assignment, but:
   - `changed` was set only when the **leader** changed
   - Pure ISR shrink did **not** bump generation, save assignment, or call
     `apply_local_assignment`
   - Non-controller observers returned after `mark_dead` without touching local
     partition ISR
3. Leader partition ISR still listed the dead follower; `follower_leo` stayed
   stale → HWM could not advance past that LEO → `acks=all` waited 10s → error 7.
4. Lag-based shrink on ReplicaFetch only removes members when
   `leader_leo - replica_leo > replica_lag_max_messages` (default 10_000), so a
   few mid-restart messages never aged the dead replica out.

Phase 106 stop/join is orthogonal: the test aborts the accept task and calls
`test_kill_broker`; the timeout is ISR/HWM, not background-task shutdown.

## Fix

| Change | Why |
|--------|-----|
| `shrink_local_isr_for_dead` on **every** death observer | Leader may not be controller; immediate local ISR drop + HWM recompute + `hwm_cvar` notify unblocks `acks=all` |
| Controller marks `changed` on pure ISR shrink | Generation bump so peers pull ClusterState; durable assignment matches live set |
| Empty-ISR retain restores previous ISR | Comment said “keep last known” but retain had already emptied the vec |
| `apply_local_assignment` recomputes HWM when we lead | ClusterState apply path also unblocks waiters after remote ISR shrink |

Contract preserved:

- `acks=all` still waits for remaining live ISR to catch HWM
- `|ISR| < min_insync_replicas` still returns `NotEnoughReplicas` (15) before append
- No “empty ISR always succeeds” shortcut

## Tests / evidence

- `cargo test -p volant-broker --test phase8_redirect_restart` — **5 consecutive
  green** runs (default threads) after the fix (was failing every run before)
- `cargo test -p volant-broker --test cluster_failover` — green (leader-kill
  path still elects from ISR)

## Files

| Path | Change |
|------|--------|
| `crates/volant-broker/src/broker.rs` | Local ISR shrink on death; generation on ISR-only change; HWM recompute on apply |
| `docs/PHASE108_SPEC.md` | This spec |
| `ROADMAP.md`, `docs/history/PHASE_HISTORY.md`, `docs/INDEX.md`, `README.md` | Living docs |
| `docs/consistency.md`, `docs/ops.md` | Rolling-restart / ISR-death note |

## Still deferred

- Accept-loop drain (native / Kafka / metrics)
- Duplicate `start_background_tasks` single-flight guard
- Straddle marker clip
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Multi-broker BROKER config fan-out
- Non-controller auto-death from heartbeat alive-set diffs
