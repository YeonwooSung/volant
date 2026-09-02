# v0.71 — Rust GroupConsumer earliest via ListOffsets

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V67_SPEC.md](./V67_SPEC.md) /
[V70_SPEC.md](./V70_SPEC.md): Rust `apply_reset` for
`AutoOffsetReset::Earliest` still hardcodes position **0**. After
DeleteRecords (v0.52 / v0.65) the log start can be **> 0**. Hardcoding
position 0 on `auto_offset_reset=earliest` then fetches a truncated
prefix.

Language clients already call ListOffsets 48/49 and use the `earliest`
field (v0.70). `Client::list_offsets` already exists and returns
`PartitionOffsets { earliest, latest }`. This slice ports that same
RPC onto Rust `GroupConsumer` only.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or language clients (those already
shipped v0.70).

## Goals

1. In `apply_reset`: when policy is `Earliest` and OffsetFetch missed /
   `OFFSET_UNKNOWN`, use the same ListOffsets loop as `Latest`, but
   take `e.earliest`.
2. If ListOffsets fails or a wanted partition is missing from the
   reply → `Err` (same as `latest` today). Do **not** silently fall
   back to 0.
3. `Latest` and `None` stay exactly as v0.67.
4. Default policy stays **`earliest`**. Existing `join` /
   `join_with_auto_commit` signatures stay valid.
5. OffsetFetch hit (committed, not UNKNOWN) still wins; no ListOffsets
   in that case.
6. Empty assignment: no ListOffsets.
7. This is **not** Kafka timestamp / isolation reset. Native 48/49
   already returns both ends.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `auto.offset.reset` (timestamp / isolation) | Native 48/49 has no timestamp selector |
| Language clients | Already have this (v0.70) |
| New native opcodes / Kafka API keys | Reuse 48 / 49 |
| Broker / protocol changes | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Range assignor via DescribeGroup | Sibling v0.73; keep this hunk local |

## Behavior

```
newly assigned partition
    │
    ├─ OffsetFetch committed and not OFFSET_UNKNOWN → use it
    │
    └─ miss / OFFSET_UNKNOWN
            │
            ├─ earliest (default) → ListOffsets; position = earliest;
            │                        fail if RPC errors or the partition
            │                        is missing (no silent 0)
            ├─ latest → ListOffsets; position = latest (LEO); fail if
            │            RPC errors or the partition is missing
            └─ none → error; do not fetch records
```

Empty assignment skips OffsetFetch **and** ListOffsets.

## API

No new public methods. Existing join signatures stay valid:

```rust
GroupConsumer::join(...)            // earliest (ListOffsets earliest)
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

`join_with_auto_commit` still calls through with `"earliest"`. The
policy lives on the shared join state so rejoin / heartbeat-driven
rebalance reuse it.

## Tests

Tiny protocol stub (same harness style as v0.67; `heartbeat=false` in
unit tests):

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| earliest + OffsetFetch miss + ListOffsets earliest=7 latest=20 | position 7 |
| earliest + ListOffsets missing partition | error |
| latest still uses `latest` | position 20 in the same fixture |
| committed offset=3 | position 3; zero ListOffsets |
| none + miss | still errors; no position 0 |

Default-join tests that previously expected 0 with no ListOffsets
either answer ListOffsets (`earliest=0`) or give a committed offset.

| File | What |
|------|------|
| `crates/volant-client/src/group.rs` | `apply_reset` Earliest via ListOffsets |
| `crates/volant-client/src/lib.rs` | Crate-doc note |
| `crates/volant-client/tests/v67_group_auto_offset_reset.rs` | Stub: earliest / missing / latest / committed |
| `crates/volant-client/tests/v44_group_heartbeat.rs` | Stub answers ListOffsets earliest=0 |
| `crates/volant-client/tests/v60_group_auto_commit.rs` | Stub answers ListOffsets earliest=0 |
| `docs/V71_SPEC.md` | This spec |

## Files

| Path | Role |
|------|------|
| `crates/volant-client/src/group.rs` | `apply_reset` uses ListOffsets earliest |
| `crates/volant-client/src/lib.rs` | Honesty: earliest is ListOffsets earliest |
| `crates/volant-client/tests/v67_group_auto_offset_reset.rs` | Stub tests |
| `crates/volant-client/tests/v44_group_heartbeat.rs` | Default-join ListOffsets |
| `crates/volant-client/tests/v60_group_auto_commit.rs` | Default-join ListOffsets |
| `docs/V71_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `auto.offset.reset`.** No timestamp reset. `earliest` /
  `latest` are the two ends of native ListOffsets 48/49.
- Language clients already have this (v0.70); this slice is Rust only.
- Not a fully concurrent consumer. One TCP connection.
- No Kafka API keys / opcodes / broker changes / Phase 155.

## Merge notes

Sibling **v0.73** also edits `group.rs` (range via DescribeGroup).
Keep this hunk local to `apply_reset` / earliest docs. Do not add an
assignor.

Do not drop auto_commit + heartbeat + instance id + this reset knob
to resolve a conflict.

## Related

- [V70_SPEC.md](./V70_SPEC.md) — Python / Go / Java earliest via ListOffsets
- [V67_SPEC.md](./V67_SPEC.md) — Rust auto_offset_reset (earliest was 0)
- [V62_SPEC.md](./V62_SPEC.md) — Language-client auto_offset_reset
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on language clients
- [V52_SPEC.md](./V52_SPEC.md) — DeleteRecords (log start can be > 0)
- [V65_SPEC.md](./V65_SPEC.md) — DeleteRecords leader redirect
- [V60_SPEC.md](./V60_SPEC.md) — Rust auto-commit
- [V44_SPEC.md](./V44_SPEC.md) — Rust background heartbeat
