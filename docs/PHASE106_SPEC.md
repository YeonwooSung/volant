# Phase 106 — Graceful background task shutdown / join (MVP)

## Goals

1. Replace fire-and-forget `tokio::spawn` loops from `start_background_tasks`
   with a joinable handle that can be stopped cleanly.
2. All loops (group expiry, retention, **sweeper**, cluster membership /
   heartbeat, follower ReplicaFetch) observe a stop signal and exit.
3. `BackgroundTasks::shutdown` signals stop and awaits joins (bounded timeout;
   abort remaining on timeout).
4. Wire `serve_listener` / `run_server` / TLS accept path to drain background
   tasks on accept-loop exit or process shutdown signal (`ctrl_c` / SIGTERM).
5. Keep Phase 101 always-spawn + 0-pause sweeper behavior.
6. Tests (`phase106_background_shutdown.rs`) + living docs honesty.

## Non-goals

- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Draining in-flight client connections / metrics / Kafka accept loops → **closed by Phase 109** (abort drain)
- Preventing duplicate `start_background_tasks` → **closed by Phase 109**
- Phase 103 parallel flake → **closed by Phase 107**
- Straddle marker clip / full Kafka broker catalog

## Design (honest MVP)

### Pre-Phase 106 gap

```text
start_background_tasks:
  tokio::spawn(group expiry loop)   // forever
  tokio::spawn(retention loop)      // forever
  tokio::spawn(sweeper loop)        // forever (Phase 101 always-spawn)
  // cluster: tick / heartbeat / follower — forever
  // no handle; no stop; drop races with in-flight sweep
```

### Fix

```text
start_background_tasks(broker) -> BackgroundTasks {
  (stop_tx, _) = watch::channel(false)
  spawn each loop with stop_rx.subscribe():
    loop {
      select! {
        _ = stop_rx.changed() => break,
        _ = tick/sleep => { work }
      }
    }
  return BackgroundTasks { stop_tx, handles }
}

BackgroundTasks::shutdown(self):
  stop_tx.send(true)
  await all handles (timeout 5s; abort remaining)
```

| Path | Behavior |
|------|----------|
| `serve_listener` / `run_server` | Start bg; select accept loop vs shutdown signal; `bg.shutdown().await` |
| TLS accept (volant-server) | Same drain on accept exit / `ctrl_c` |
| Tests | Capture handle; `shutdown().await` (or `abort()` / Drop) |
| Drop without shutdown | Signals stop + aborts handles (best-effort) |

### API

```rust
pub struct BackgroundTasks { /* stop_tx + JoinHandles */ }

impl BackgroundTasks {
    pub async fn shutdown(self); // signal + join (≤5s, then abort)
    pub fn abort(self);          // signal + abort, no await
}

pub fn start_background_tasks(broker: Arc<Broker>) -> BackgroundTasks;
```

No new crates (`tokio::sync::watch` only; workspace already has `tokio` full).

### Sweeper (Phase 101 preserved)

| Case | Behavior |
|------|----------|
| Interval `0` | Task runs; paused (200ms poll); stop still observed |
| Interval `>0` | Sleep `ms` then `sweep_timeouts`; stop observed mid-sleep |
| Runtime `0→>0` | Still enables without restart (before shutdown) |
| After `shutdown` | No further background expire; lazy paths remain |

## Exit criteria

1. `start_background_tasks` returns `BackgroundTasks`
2. All bg loops exit on stop signal
3. `shutdown` joins without hang (active + paused)
4. After shutdown, no background open-txn expire
5. Server accept paths drain bg tasks
6. `phase106_*` + phase101 + phase97 green
7. Docs: PHASE106_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Connection accept loops (native / Kafka / metrics) drain → **closed by Phase 109**
- Calling `start_background_tasks` twice → **closed by Phase 109** (single-flight)
- Shutdown timeout aborts stragglers (does not wait forever)
- TLS path SIGTERM + connection drain → **closed by Phase 109**
- Single-node wall clock; no multi-broker coordinated expiry

## Test plan

`crates/volant-broker/tests/phase106_background_shutdown.rs`:

1. Interval `>0` → `shutdown` joins promptly
2. Interval `0` (paused) → `shutdown` joins promptly
3. After `shutdown`, aged open txn is **not** background-expired; lazy still works
4. `0→>0` before shutdown still enables expire (Phase 101 regression)

## Later phases

- Drain native / Kafka / metrics accept loops on shutdown → **closed by Phase 109**
- Single-flight guard against duplicate `start_background_tasks` → **closed by Phase 109**
- Phase 103 parallel flake → **closed by Phase 107**
- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Straddle marker clip / control-batch log GC
