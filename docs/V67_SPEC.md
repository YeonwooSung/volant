# v0.67 — Rust GroupConsumer auto_offset_reset

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V62_SPEC.md](./V62_SPEC.md):
“Rust `GroupConsumer` still OffsetFetch / 0.” Today an OffsetFetch miss
/ `OFFSET_UNKNOWN` (`u64::MAX`) becomes **0** (log start). This slice
ports the **tiny** Kafka subset already on the language clients
(`earliest` / `latest` / `none`) onto `crates/volant-client` only,
using native ListOffsets already on Rust `Client::list_offsets`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker or language clients (those already shipped v0.62).

## Goals

1. After join / rebalance, for each **newly assigned** partition:
   OffsetFetch as today. If a committed offset exists and is **not**
   `OFFSET_UNKNOWN` → use it. Else apply reset.
2. **`earliest`** (default, current behavior): position **0**. Do **not**
   require a ListOffsets RPC — 0 is the native log start / same as today.
3. **`latest`**: `client.list_offsets(topic, [partition])` and use
   `latest` (LEO). If ListOffsets fails or the partition is missing from
   the reply, return `Err`.
4. **`none`**: return a clear error — do not start at 0.
5. Empty assignment: no ListOffsets.
6. Invalid reset string → join fails **before** JoinGroup.
7. Default **`earliest`** so existing Rust group tests that expect 0
   still pass. Existing `join` / `join_static` /
   `join_with_heartbeat` / `join_static_with_heartbeat` /
   `join_with_auto_commit` stay valid.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `auto.offset.reset` (timestamp / isolation) | Native 48/49 has no timestamp selector |
| `earliest` via ListOffsets earliest | 0 is the native log start on a single-node leader (language leftover; not this slice) |
| Language clients | Already have this (v0.62) |
| New native opcodes / Kafka API keys | Reuse 48 / 49 |
| Broker / protocol changes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Behavior

```
newly assigned partition
    │
    ├─ OffsetFetch committed and not OFFSET_UNKNOWN → use it
    │
    └─ miss / OFFSET_UNKNOWN
            │
            ├─ earliest (default) → position 0 (no ListOffsets)
            ├─ latest → ListOffsets; position = latest (LEO); fail if
            │            RPC errors or the partition is missing
            └─ none → error; do not fetch records
```

Empty assignment skips OffsetFetch **and** ListOffsets.

## API

Keep existing join signatures. Additive, following the v0.60
`join_with_auto_commit` pattern. The public join accepts a **string**
(`"earliest"` / `"latest"` / `"none"`) to match Python / Go / Java.

```rust
GroupConsumer::join(...)            // earliest
GroupConsumer::join_static(...)
GroupConsumer::join_with_heartbeat(..., heartbeat)
GroupConsumer::join_static_with_heartbeat(..., heartbeat)
GroupConsumer::join_with_auto_commit(...) // earliest

GroupConsumer::join_with_auto_offset_reset(
    client, group_id, topics, session_timeout_ms,
    group_instance_id, heartbeat,
    auto_commit, auto_commit_interval,
    auto_offset_reset: &str, // "earliest" | "latest" | "none"
).await
```

```rust
let g = GroupConsumer::join_with_auto_offset_reset(
    client, "g", vec!["t".into()], 10_000, "", true,
    false, Duration::ZERO, "latest",
).await?;
```

`join_with_auto_commit` calls through with `"earliest"`. The policy
lives on the shared join state so rejoin / heartbeat-driven rebalance
reuse it. Invalid string → `Error::InvalidArgument` before JoinGroup
(not a panic). Empty string is `"earliest"`.

## Tests

Tiny protocol stub (same harness style as v0.60; `heartbeat=false` in
unit tests):

| Case | Expect |
|------|--------|
| Default join (no reset arg) + OffsetFetch miss | position 0; no ListOffsets |
| `latest` + OffsetFetch miss + ListOffsets latest=5 | position 5 |
| `none` + OffsetFetch miss | error mentioning auto_offset_reset |
| invalid string `"banana"` | error before JoinGroup |
| OffsetFetch committed=3 | position 3 regardless of reset policy |
| `latest` + ListOffsets missing partition | error |

```bash
cargo test -p volant-client --lib -- --test-threads=1
cargo test -p volant-client --test v67_group_auto_offset_reset -- --test-threads=1
cargo test -p volant-client --test v60_group_auto_commit -- --test-threads=1
cargo test -p volant-client --test v44_group_heartbeat -- --test-threads=1
```

| File | What |
|------|------|
| `crates/volant-client/src/group.rs` | `join_with_auto_offset_reset`; parse + reset unit tests |
| `crates/volant-client/tests/v67_group_auto_offset_reset.rs` | Stub: earliest / latest / none / invalid / committed / missing LEO |
| `docs/V67_SPEC.md` | This spec |

## Files

| Path | Role |
|------|------|
| `crates/volant-client/src/group.rs` | Reset policy, `do_join` / `apply_reset` |
| `crates/volant-client/src/lib.rs` | Crate-doc note |
| `crates/volant-client/tests/v67_group_auto_offset_reset.rs` | Stub tests |
| `docs/V67_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `auto.offset.reset`.** No timestamp reset. `latest` is
  LEO (native ListOffsets 48/49).
- **`earliest` is 0**, not a ListOffsets earliest read (usually the
  same on a single-node leader). That leftover stays deferred.
- Language clients already have this (v0.62); this slice is Rust only.
- Not a fully concurrent consumer. One TCP connection.
- No Kafka API keys / opcodes / broker changes / Phase 155.

## Related

- [V62_SPEC.md](./V62_SPEC.md) — Python / Go / Java auto_offset_reset
- [V60_SPEC.md](./V60_SPEC.md) — Rust auto-commit
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on language clients
- [V44_SPEC.md](./V44_SPEC.md) — Rust background heartbeat
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
