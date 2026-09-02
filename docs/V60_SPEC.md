# v0.60 — Rust GroupConsumer auto-commit

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V48_SPEC.md](./V48_SPEC.md): “Rust
`GroupConsumer` is still explicit-only.” Same opt-in auto-commit
semantics as v0.48, on `crates/volant-client` only. Default stays
**off**.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, change the broker, or touch the
Python / Go / Java clients (those already shipped v0.48).

## Goals

1. After a successful `poll` that returned ≥1 record, if auto-commit is
   on:
   - interval **0**: commit immediately (same as explicit commit:
     member_id + generation, assigned positions only);
   - interval **> 0**: commit if never committed yet **or**
     `now - last_auto_commit >= interval`.
2. **First successful poll always auto-commits**, then the interval
   applies.
3. `leave` (consume-self close): if auto-commit is on and there are
   uncommitted (dirty) positions, **best-effort commit once**, then
   LeaveGroup.
4. Explicit `commit()` still works and resets the interval clock.
5. Commit failures: surface on explicit commit and on auto-commit after
   poll (do not swallow). On leave, best-effort (swallow, still leave).
6. Default **off**: existing unit tests that poll without commit stay
   valid. Existing `join` / `join_static` /
   `join_with_heartbeat` / `join_static_with_heartbeat` signatures are
   unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `enable.auto.commit` background thread | Commits run after `poll` / on `leave`, not on a timer independent of poll |
| Changing the broker OffsetCommit path | Same member+generation commit as today |
| Kafka API keys / native opcodes | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Language clients | Already have this (v0.48) |
| Broker / Python / Go / Java edits | Out of scope |

## API

Default is explicit commit (today). Opt-in via an additive join:

```rust
GroupConsumer::join(...)            // explicit-only
GroupConsumer::join_static(...)
GroupConsumer::join_with_heartbeat(..., heartbeat)
GroupConsumer::join_static_with_heartbeat(..., heartbeat)

GroupConsumer::join_with_auto_commit(
    client, group_id, topics, session_timeout_ms,
    group_instance_id, heartbeat,
    auto_commit: bool,
    auto_commit_interval: Duration, // ZERO = after every successful poll
).await
```

```rust
let g = GroupConsumer::join_with_auto_commit(
    client, "g", vec!["t".into()], 10_000, "", true,
    true, Duration::from_secs(5),
).await?;
let g0 = GroupConsumer::join_with_auto_commit(
    client, "g", vec!["t".into()], 10_000, "", true,
    true, Duration::ZERO,
).await?;
```

`auto_commit = false` is the existing explicit-only path. Flags live on
`GroupConsumer`; dirty / last-commit live on the shared join state next
to positions. `poll` already holds `gate`; auto-commit runs after
records are collected, still under the gate so it serializes with
heartbeat.

## Behavior

```
poll returns N records
    │
    ├─ N == 0 → no auto-commit
    │
    └─ N >= 1, auto-commit on
            │
            ├─ never committed yet → commit (first successful poll)
            ├─ interval == 0 → commit
            └─ interval > 0 and now - last < interval → skip (dirty stays)
```

- Dirty is set when a poll advances positions and cleared on a
  successful commit (auto or explicit).
- `leave` with auto-commit on + dirty → best-effort commit, then leave.
- Empty poll does **not** auto-commit even if the interval has elapsed;
  leftover dirty is flushed on leave.

This is **not** Kafka `enable.auto.commit` / `auto.commit.interval.ms`
beyond “commit on an interval after poll.” There is no background
commit thread.

## Tests

Tiny protocol stub (same harness style as v0.44; `heartbeat=false` in
unit tests):

1. Default off: poll does not commit.
2. Interval 0: poll of records → one commit with joined member+generation.
3. Interval 10s: two quick polls → first successful poll auto-commits;
   the second does not.
4. Leave with auto-commit on and pending positions → commit then leave.
5. Existing group tests still pass (`heartbeat=false` in unit tests).

```bash
cargo test -p volant-client --lib -- --test-threads=1
cargo test -p volant-client --test v60_group_auto_commit -- --test-threads=1
cargo test -p volant-client --test v44_group_heartbeat -- --test-threads=1
```

| File | What |
|------|------|
| `crates/volant-client/src/group.rs` | `join_with_auto_commit`; due-interval unit tests |
| `crates/volant-client/tests/v60_group_auto_commit.rs` | Default off; interval 0; first-poll-only; leave flushes dirty |
| `docs/V60_SPEC.md` | This spec |

## Files

| Path | Role |
|------|------|
| `crates/volant-client/src/group.rs` | Auto-commit flags, poll / commit / leave |
| `crates/volant-client/src/lib.rs` | Crate-doc note |
| `crates/volant-client/tests/v60_group_auto_commit.rs` | Stub tests |
| `docs/V60_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `enable.auto.commit`.** No background commit timer
  independent of `poll`. Interval 0 means “after every successful poll,”
  not Kafka’s `auto.commit.interval.ms=0`.
- Default **off**. Callers that never `commit()` still do not commit.
- Leave is **best-effort**: a failed auto-commit on leave is swallowed
  so LeaveGroup still runs.
- Auto-commit after `poll` **returns the error**. Positions may already
  have advanced.
- Language clients already have this (v0.48); this slice is Rust only.
- Not a fully concurrent consumer. One TCP connection.
- No Kafka API keys / native opcodes / broker changes / Phase 155.

## Related

- [V48_SPEC.md](./V48_SPEC.md) — Python / Go / Java auto-commit
- [V44_SPEC.md](./V44_SPEC.md) — Rust background heartbeat
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
