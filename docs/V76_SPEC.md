# v0.76 — Rust GroupConsumer poll fetch knobs

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V75_SPEC.md](./V75_SPEC.md): Rust
`GroupConsumer::poll` hardcodes
`client.fetch(&topic, partition, Offset::new(from), 100, 0)`. Language
poll is already tunable (default **100 / 4MiB**). Rust `Client::fetch`
already takes `max_messages` and `max_wait_ms` but hardcodes
`max_bytes = 4MiB` inside `fetch`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / language clients (those
already shipped v0.75).

## Goals

1. Keep today’s **default** poll fetch size so existing tests stay
   valid:
   - `max_messages` default **100** (historical poll cap, **not**
     Client fetch’s 128)
   - `max_bytes` default **4 MiB** (same as Client fetch)
   - `poll` still takes no wait argument; the `0` passed to fetch is
     Fetch RPC `max_wait_ms`
2. Additive knobs on `GroupConsumer`, following
   `join_with_auto_commit` / `join_with_assignor`:
   `join_with_fetch_knobs(...)`. Existing `join` / `join_static` /
   `join_with_heartbeat` / `join_with_auto_commit` /
   `join_with_auto_offset_reset` / `join_with_assignor` stay valid
   (default 100 / 4MiB).
3. Values `0` clamp to the defaults (100 / 4MiB) at join and poll
   time.
4. Additive `Client::fetch_opts(..., max_bytes)`. Existing
   `Client::fetch` delegates to it with `4MiB`. **Do not** change the
   `fetch` signature.
5. Store knobs on `Shared` so rejoin reuses them.
6. Do **not** change assignor, auto_offset_reset, or `apply_reset`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `max.poll.records` | Native Fetch opcode 2 only; one fetch per assigned partition |
| Change Client fetch default 128 | Unrelated; poll stays 100 |
| Language clients | Already have this (v0.75) |
| New native opcodes / Kafka API keys | Reuse Fetch (2) |
| Broker / protocol changes | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Admin redirect / heartbeat retry | Different residuals; keep this hunk local |

## API

Keep existing join signatures. Additive, following the v0.60 / v0.67 /
v0.73 pattern:

```rust
GroupConsumer::join(...)            // 100 / 4MiB
GroupConsumer::join_static(...)
GroupConsumer::join_with_heartbeat(..., heartbeat)
GroupConsumer::join_static_with_heartbeat(..., heartbeat)
GroupConsumer::join_with_auto_commit(...) // 100 / 4MiB
GroupConsumer::join_with_auto_offset_reset(...) // 100 / 4MiB
GroupConsumer::join_with_assignor(...) // 100 / 4MiB

GroupConsumer::join_with_fetch_knobs(
    client, group_id, topics, session_timeout_ms,
    group_instance_id, heartbeat,
    auto_commit, auto_commit_interval,
    auto_offset_reset: &str,
    assignor: &str,
    fetch_max_messages: u32,
    fetch_max_bytes: u32,
).await
```

```rust
let g = GroupConsumer::join_with_fetch_knobs(
    client, "g", vec!["t".into()], 10_000, "", true,
    false, Duration::ZERO, "earliest", "broker",
    10, 4096,
).await?;
```

`join_with_assignor` calls through with `100` / `4MiB`. The knobs live
on the shared join state so rejoin / heartbeat-driven rebalance reuse
them. `0` clamps to 100 / 4MiB.

```rust
Client::fetch(topic, partition, from, max_messages, max_wait_ms)
// delegates to:
Client::fetch_opts(topic, partition, from, max_messages, max_wait_ms, max_bytes)
```

`GroupConsumer::fetch_max_messages()` / `fetch_max_bytes()` return the
stored (clamped) knobs.

`poll()` is unchanged: no wait argument; Fetch `max_wait_ms` stays
`0`.

## Tests

Tiny protocol stub (same harness style as v0.73; `heartbeat=false` in
unit tests). The stub records Fetch `max_messages` / `max_bytes`.

| Case | Expect |
|------|--------|
| Default join + poll | Fetch max_messages=100, max_bytes=4MiB |
| join with max_messages=10 | Fetch max_messages=10 |
| join with max_bytes=4096 | Fetch max_bytes=4096 |
| 0 clamps to defaults | 100 / 4MiB |
| Existing group tests | still pass (stubs already accept Fetch) |

```bash
cargo test -p volant-client -- --test-threads=1
```

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `fetch_opts`; `fetch` delegates with 4MiB |
| `crates/volant-client/src/group.rs` | `join_with_fetch_knobs`; poll uses knobs |
| `crates/volant-client/src/lib.rs` | Crate-doc note |
| `crates/volant-client/tests/v76_group_poll_fetch_knobs.rs` | Stub records Fetch knobs |
| `docs/V76_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `max.poll.records`.** One native Fetch (opcode 2) per
  assigned partition; knobs are `max_messages` / `max_bytes` on that
  request. Default 100 is the historical poll cap, not Client fetch’s
  128.
- Rust `poll` still has no wait / timeout argument. The `0` on Fetch
  is `max_wait_ms` only.
- Language clients already have this (v0.75); this slice is Rust
  only.
- Not a fully concurrent consumer. One TCP connection.
- No Kafka API keys / opcodes / broker changes / Phase 155.

## Merge notes

Siblings that also edit `group.rs` (v0.73 assignor, v0.71 earliest)
must keep this hunk local to poll / fetch knobs + `join_with_fetch_knobs`.
Do not change who `range_assign_multi` receives or how `apply_reset`
picks a position.

Do not drop auto_commit + heartbeat + assignor + instance id + reset
knob + these fetch knobs to resolve a conflict.

## Related

- [V75_SPEC.md](./V75_SPEC.md) — Python / Go / Java poll fetch knobs
- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs (Client)
- [V73_SPEC.md](./V73_SPEC.md) — Rust `join_with_assignor`
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
