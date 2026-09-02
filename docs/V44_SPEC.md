# v0.44 — Rust GroupConsumer background heartbeat

**Status:** Shipped (MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Rust `GroupConsumer` only heartbeats inside
`poll()`” so a silent consumer does not expire. Language clients
(Python/Go/Java) are a sibling / unmerged v0.37 slice — this slice is
**Rust only**.

**Honesty:** this is **not** a fully concurrent consumer, **not** Kafka
`heartbeat.interval.ms` / auto-commit, and **not** a broker change. The
join lock only serializes the background task against `poll` / `commit` /
`leave`. Do not call `poll` from two tasks. `Drop` aborts the task and
does **not** send LeaveGroup — call `leave().await` for a clean leave.

## Goals

1. After a successful `join` / `join_static`, spawn a tokio task that
   heartbeats every [`heartbeat_interval`](#interval).
2. Default **on**. Escape: `join_with_heartbeat` /
   `join_static_with_heartbeat(..., heartbeat: bool)`. Existing
   `join` / `join_static` signatures are unchanged (`heartbeat = true`).
3. `heartbeat = false` keeps today’s poll-only membership (no task).
4. On heartbeat error 9/10/11 the background task `do_join`s (same as
   `poll`). Other heartbeat errors are ignored until the next tick.
5. `poll` still heartbeats once at the start of the call.
6. `leave` stops the task (short await, then abort) then LeaveGroup.
   Idempotent with `Drop`.

## Interval

`session_timeout_ms / 3`, clamped to **`[100ms, 3000ms]`**.

| `session_timeout_ms` | interval |
|----------------------|----------|
| 0 / 150 | 100ms |
| 900 | 300ms |
| 10_000 | 3000ms |

Helper: `volant_client::heartbeat_interval(session_timeout_ms) -> Duration`
(tests assert the clamp without sleeping 3s). Join still remaps
`session_timeout_ms == 0` to 10_000 for the broker; the helper is
independent.

## API

```rust
GroupConsumer::join(...)            // heartbeat on
GroupConsumer::join_static(...)     // heartbeat on
GroupConsumer::join_with_heartbeat(..., heartbeat: bool)
GroupConsumer::join_static_with_heartbeat(..., heartbeat: bool)
GroupConsumer::heartbeat_count()    // poll + background RPCs
GroupConsumer::leave(self)          // stop task, then LeaveGroup
```

Accessors (`assignment`, `member_id`, `positions`, …) return owned
snapshots of the shared join state.

## Drop vs leave

| Path | Heartbeat task | LeaveGroup |
|------|----------------|------------|
| `leave().await` | signal + await (500ms then abort) | yes |
| `Drop` | `abort()` only | **no** |

`leave().await` is required for a clean LeaveGroup.

## Tests

`crates/volant-client/tests/v44_group_heartbeat.rs` (tiny protocol stub,
no full broker):

1. Interval clamp: 0/150 → 100ms; 900 → 300ms; 10_000 → 3000ms.
2. Background heartbeat fires without `poll` (300ms session → 100ms tick).
3. `heartbeat=false` → no background heartbeats during a 2-interval sleep.
4. `leave` stops the task (no heartbeats after leave).

Unit: `heartbeat_interval_clamps` in `group.rs`. Existing
`e2e_group` / Phase 17 group tests still pass.

```bash
cargo test -p volant-client --test v44_group_heartbeat -- --test-threads=1
cargo test -p volant-client --lib -- --test-threads=1
```

## Non-goals

- Language clients (Python/Go/Java) — sibling v0.37
- Broker group coordinator / new opcodes / Kafka API keys
- Fully concurrent `poll` from two tasks
- Auto-commit / pause-resume / Kafka consumer heartbeat thread
- Phase 155 / homemade metadata Raft
