# Phase 101 — Graceful sweeper enable on 0→>0 (MVP)

## Goals

1. Always spawn the background open/prepared/session sweeper task from
   `start_background_tasks`, even when `volant.sweep.interval.ms` (or env /
   durable file / setter) is **0** at process start.
2. Treat interval **`0` as pause-only** (existing 200ms poll loop); no
   `sweep_timeouts` while paused.
3. A later **`0 → >0`** transition via `Broker::set_sweep_interval_ms` or
   AlterConfigs / IncrementalAlterConfigs **starts sweeping on the next poll
   cycle without process restart**.
4. **`>0 → 0`** still pauses; lazy expire paths remain correct.
5. Metrics / `sweep_timeouts` semantics unchanged.
6. Tests (`phase101_*.rs`) + living docs honesty.

## Non-goals

- Graceful sweeper shutdown / join on server stop (still fire-and-forget)
- Multi-broker coordinated clocks / multi-broker 2PC
- Multi-lang clients / fuzz CI
- Sparse durable config → **closed by Phase 102** / BROKER name=`node_id` validation → **closed by Phase 103**
- Marker GC → **closed by Phase 104** / empty-AddPartitions control markers → **closed by Phase 105**
- Full Kafka broker catalog

## Design (honest MVP)

### Pre-Phase 101 gap

```text
start_background_tasks:
  if sweep_interval_ms() > 0 {
    spawn sweeper loop
  }
```

Boot with `0` (env / durable / setter before server entry) **never spawned** the
task. Later Alter / `set_sweep_interval_ms(>0)` updated the Atomic and durable
file but the background loop did not exist until restart.

### Fix

```text
start_background_tasks:
  always spawn sweeper loop {
    ms = re-read Atomic
    if ms == 0 {
      sleep(200ms); continue   // pause; observe later enable
    }
    sleep(ms)
    if interval still > 0 {
      broker.sweep_timeouts()
    }
  }
```

| Case | Behavior |
|------|----------|
| Boot with interval `0` | Task runs; paused (no expire) |
| Runtime `0 → >0` (setter or Alter) | Next poll cycle sleeps `ms` then sweeps |
| Runtime `>0 → 0` | Pause; lazy expire still works |
| Boot with interval `>0` | Unchanged (periodic sweep) |
| Lazy API paths | Unchanged; correctness independent of sweeper |

### Config surface (unchanged keys)

| Source | Behavior |
|--------|----------|
| Default | **1000 ms** |
| Env `VOLANT_SWEEP_INTERVAL_MS` | At construction |
| Durable file (Phase 100) | After env |
| `Broker::set_sweep_interval_ms` | Process-local Atomic (no auto-persist) |
| Alter / IncrementalAlter BROKER | Live + sparse durable overlay (Phase 99–102) |
| **`0`** | **Pause** background work (lazy remains) |

## Exit criteria

1. `start_background_tasks` always spawns the sweeper task
2. Boot with `0` + later setter `>0` → background expires aged open txn
3. Boot with `0` + later AlterConfigs `>0` → same without restart
4. `>0 → 0` pauses; lazy expire still works
5. `phase101_*` + phase97 regression green
6. Docs: PHASE101_SPEC + living docs / ROADMAP / README status ceiling

## Honest limitations

- Fire-and-forget tokio task (no join on shutdown) — still deferred
- Single-node wall clock; no multi-broker coordinated expiry
- Calling `start_background_tasks` twice still spawns duplicate bg tasks
  (same as group expiry / retention; not fixed here)
- Idle session sweep only (LRU still lazy-on-create)
- Six BROKER knobs only; resource name still ignored → **closed by Phase 103**

## Test plan

`crates/volant-broker/tests/phase101_sweeper_restart.rs`:

1. Interval `0` → `start_background_tasks` → aged open txn stays open →
   `set_sweep_interval_ms(50)` → background aborts without explicit expire
2. Same pattern via AlterConfigs BROKER SET `volant.sweep.interval.ms=50`
3. `>0 → 0` pauses; lazy `expire_timed_out_open_txns` still works

## Phase 102 ideas

- Graceful sweeper shutdown / join on server stop
- Validate BROKER resource name against `node_id` → **closed by Phase 103**
- Sparse durable file (only keys differing from product default) → **closed by Phase 102**
- Marker compaction / GC with DeleteRecords
- Multi-broker config broadcast / multi-broker 2PC
- Multi-lang clients / cargo-fuzz corpus CI
- Empty-AddPartitions control markers → **closed by Phase 105**
