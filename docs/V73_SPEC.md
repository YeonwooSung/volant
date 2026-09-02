# v0.73 — Rust GroupConsumer multi-member range via DescribeGroup

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V41_SPEC.md](./V41_SPEC.md) /
[V69_SPEC.md](./V69_SPEC.md): Rust `GroupConsumer` still uses only the
JoinGroup assignment (no member list on the wire). Language clients
already call DescribeGroup (34/35) and `range_assign_multi` when
`assignor="range"` (v0.69). This slice ports that to
`crates/volant-client` only.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes
(there is still no SyncGroup), or change the broker / protocol /
language clients.

## Goals

1. Additive join option: `assignor = "range"` (default stays broker
   JoinGroup assignment — empty / `"broker"` / omitted).
2. After a successful JoinGroup (`member_id` set), if assignor is
   range:
   - `describe_group`
   - Build `member_ids` + per-member topic lists from
     `DescribeGroupResult.members`
   - Include **self** with the local topic list if describe omitted us
   - Partition counts from existing `metadata()`
   - `range_assign_multi` and take **this** member’s assignment
3. **Fallback to today’s JoinGroup assignment** (not solo-all, unless
   JoinGroup assignment is empty — then solo-range over `[self]` like
   language fallback) if:
   - DescribeGroup errors
   - members list empty after including self
   - self index missing
   Join must **not** fail just because describe failed.
4. Invalid assignor string → join fails **before** JoinGroup.
5. Existing `join` / `join_static` / `join_with_heartbeat` /
   `join_with_auto_commit` / `join_with_auto_offset_reset` stay valid
   (broker assignment).
6. `join_with_assignor(...)` follows the v0.60 / v0.67 pattern. The
   policy lives on `Shared` so heartbeat-driven rejoin reuses it.

## Non-goals

| Deferred | Why |
|----------|-----|
| SyncGroup / member list on JoinGroup | Frozen; do not invent an opcode |
| Sticky / cooperative client assignor | Broker already has sticky |
| Language clients | Already have this (v0.69) |
| `earliest` via ListOffsets | Different residual (v0.71); keep v0.67 (`earliest` = position 0) |
| New Kafka API keys / native opcodes | Frozen |
| Phase 155 / homemade metadata Raft | Frozen |
| Adding `volant-broker` as a client dep | Copy the small range assignor instead |

## Behavior

```
assignor == "range" after successful JoinGroup
    │
    ├─ describe_group(group)
    │       │
    │       ├─ error / empty after including self / self missing
    │       │     → JoinGroup assignment
    │       │       (or solo range_assign_multi([self], [self.topics],
    │       │        counts) if that assignment is empty)
    │       │
    │       └─ members (describe order; append self + self.topics if omitted)
    │             → metadata() → partition counts
    │             → range_assign_multi(ids, topics, counts)[self]
    │
assignor != "range"  (empty / "broker" / omitted)
    └─ honor JoinGroup assignment; no DescribeGroup
```

Range for `n=4` partitions and sorted members `m-a`, `m-b`: first
gets `0–1`, second `2–3`.

`range_assign` / `range_assign_multi` are copied into
`crates/volant-client/src/assignor.rs` and match
`volant_broker::{range_assign, range_assign_multi}`.

## API

Keep existing join signatures. Additive, following the v0.60 /
v0.67 pattern:

```rust
GroupConsumer::join(...)            // broker
GroupConsumer::join_static(...)
GroupConsumer::join_with_heartbeat(..., heartbeat)
GroupConsumer::join_static_with_heartbeat(..., heartbeat)
GroupConsumer::join_with_auto_commit(...) // broker
GroupConsumer::join_with_auto_offset_reset(...) // broker

GroupConsumer::join_with_assignor(
    client, group_id, topics, session_timeout_ms,
    group_instance_id, heartbeat,
    auto_commit, auto_commit_interval,
    auto_offset_reset: &str,
    assignor: &str, // "broker" | "range"
).await
```

```rust
let g = GroupConsumer::join_with_assignor(
    client, "g", vec!["t".into()], 10_000, "", true,
    false, Duration::ZERO, "earliest", "range",
).await?;
```

`join_with_auto_offset_reset` calls through with `"broker"`. The
policy lives on the shared join state so rejoin / heartbeat-driven
rebalance reuse it. Invalid string → `Error::InvalidArgument` before
JoinGroup (not a panic). Empty string is `"broker"`.

`GroupConsumer::assignor()` returns `"broker"` or `"range"`.

## Tests

Tiny protocol stub (same harness style as v0.67; `heartbeat=false` in
unit tests):

| Case | Expect |
|------|--------|
| range + describe 2 members + 4 parts | this member gets the range half (sorted ids; n=4 m=2 → first 0–1, second 2–3) |
| range + describe error | JoinGroup assignment used (or solo fallback); join succeeds |
| default join | no DescribeGroup |
| describe omits self | self still included |
| invalid `"banana"` | error before JoinGroup |

```bash
cargo test -p volant-client -- --test-threads=1
```

| File | What |
|------|------|
| `crates/volant-client/src/assignor.rs` | Copied `range_assign` / `range_assign_multi` |
| `crates/volant-client/src/group.rs` | `join_with_assignor`; describe + range override |
| `crates/volant-client/src/lib.rs` | Crate-doc note |
| `crates/volant-client/tests/v73_group_range_assign.rs` | Stub: 2-member / error / default / omit-self / invalid |
| `docs/V73_SPEC.md` | This spec |

## Honesty leftovers

- **Still no SyncGroup.** Native JoinGroup does not return the member
  list. This slice reuses DescribeGroup (34/35) only.
- DescribeGroup can race the just-completed join (omit self). The
  client appends self with the local subscription. A describe
  **error** falls back to the JoinGroup assignment (not solo-all), so
  two live range members may briefly keep overlapping broker slices
  instead of the full topic.
- **Not Kafka cooperative-sticky.** Range only; sticky stays broker.
- **Not kafka-python / kafka-clients / kafka-go.** Native protocol
  only.
- Language clients already have this (v0.69); this slice is Rust only.
- `earliest` reset is still position 0 (v0.67); this slice does not
  call ListOffsets for earliest.
- Not a fully concurrent consumer. One TCP connection.
- No Kafka API keys / opcodes / broker changes / Phase 155.

## Merge notes

Sibling **v0.71** also edits `group.rs` (`apply_reset` earliest).
Keep this hunk local to assignor / describe-group member collection /
`do_join` assignment override. Do **not** change earliest=0 vs
ListOffsets.

## Related

- [V41_SPEC.md](./V41_SPEC.md) — client-side range assignor (language)
- [V69_SPEC.md](./V69_SPEC.md) — language multi-member range via DescribeGroup
- [V67_SPEC.md](./V67_SPEC.md) — Rust `auto_offset_reset`
- [V49_SPEC.md](./V49_SPEC.md) — DescribeGroup (34/35)
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
