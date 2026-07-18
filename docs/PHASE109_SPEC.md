# Phase 109 — Accept-loop drain + single-flight `start_background_tasks` (MVP)

## Goals

1. **Single-flight** `start_background_tasks`: only the first call per `Broker`
   spawns group/retention/sweeper/cluster loops. Subsequent calls return a
   no-op `BackgroundTasks` handle (safe `shutdown` / `abort` / `Drop`).
2. **Accept-loop drain** for production paths:
   - Native: `serve_listener` / `serve_listener_until` — stop on signal or
     custom future; track connection tasks and abort with a bounded timeout.
   - Kafka: `serve_kafka_listener` / `serve_kafka_listener_until` — same.
   - Metrics: `run_metrics_server` / `run_metrics_server_until` — same.
3. Wire `volant-server` so SIGTERM/ctrl_c drains accept loops **and**
   `BackgroundTasks::shutdown` without hang (side listeners aborted after
   primary server returns; TLS uses full `shutdown_signal` + connection drain).
4. Keep Phase 101 always-spawn + 0-pause sweeper; keep Phase 106 join timeout.
5. Tests (`phase109_shutdown_drain.rs`) + living docs honesty.

## Non-goals

- Multi-broker 2PC / sessions / multi-lang / fuzz CI
- Non-controller auto-death from alive-set diffs → **closed by Phase 110**
- Straddle marker clip
- Graceful in-flight request completion (connections are aborted, not drained
  to idle EOF)
- Sharing one stop channel across no-op second handles (second is pure no-op)

## Design (honest MVP)

### Pre-Phase 109 gap

```text
start_background_tasks called twice → two sweepers (double expire risk)
native accept: signal stops loop but connection tasks not tracked
kafka / metrics accept: run until process exit; no signal select
TLS path: ctrl_c only (no SIGTERM)
```

### Single-flight

```text
Broker.bg_tasks_started: AtomicBool

start_background_tasks(broker):
  if !broker.claim_background_tasks():  // swap true; first wins
    return BackgroundTasks { empty handles, stop already true }
  spawn loops with watch stop (Phase 106)
  return BackgroundTasks { stop_tx, handles }
```

| Call | Behavior |
|------|----------|
| 1st | Spawns all loops; handle owns joins |
| 2nd+ | No-op handle; `shutdown`/`abort`/`Drop` do not stop first-flight tasks |
| After 1st `shutdown` | No further background expire; lazy paths remain |

### Accept-loop drain

```text
accept_loop(listener, broker, shutdown):
  conns = []
  loop {
    select! {
      _ = shutdown => break
      accept => conns.push(spawn(handle_connection))
    }
  }
  abort all conns; join ≤ 2s
```

| API | Stop condition |
|-----|----------------|
| `serve_listener` | `shutdown_signal()` (ctrl_c + SIGTERM) |
| `serve_listener_until(f)` | future `f` |
| `serve_kafka_listener` / `_until` | same pattern |
| `run_metrics_server` / `_until` | same pattern |
| TLS `run_tls_server` | `shutdown_signal()` + conn drain |

Primary server path still starts bg tasks (single-flight) and calls
`BackgroundTasks::shutdown` after the accept loop returns.

### Server wiring

```text
spawn metrics accept (optional)
spawn kafka accept (optional)
run native/TLS primary server  // selects signal, drains bg
abort side handles (stragglers)
```

Side listeners also select on the same process signals, so they normally exit
before the abort; abort is belt-and-suspenders.

## Exit criteria

1. Double `start_background_tasks` → one sweeper effect
2. Native / Kafka accept `*_until` complete promptly on stop
3. Phase 106 join still works with a duplicate start
4. Phase 101 always-spawn + 0-pause preserved
5. `phase109_*` + phase106 + phase101 + phase97 green
6. Docs: PHASE109_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Connection drain **aborts** tasks (no half-close / in-flight RPC finish)
- No-op second `BackgroundTasks` cannot stop first-flight tasks
- Metrics/Kafka side tasks aborted after primary return (not soft-joined with
  shared timeout beyond their own signal select)
- Multi-broker 2PC still deferred; alive-set auto-death → **closed by Phase 110**;
  straddle clip → **closed by Phase 111**

## Test plan

`crates/volant-broker/tests/phase109_shutdown_drain.rs`:

1. Double start → open txn expires once (counter not wildly doubled)
2. Second handle shutdown is immediate no-op; first still sweeps
3. First shutdown stops background expire
4. `serve_listener_until` / `serve_kafka_listener_until` finish promptly
5. Phase 106 join regression with duplicate start

## Still deferred after this

- Non-controller auto-death from alive-set diffs → **closed by Phase 110**
- Straddle marker clip
- Multi-broker 2PC / multi-lang / fuzz CI
- Multi-broker BROKER config fan-out
