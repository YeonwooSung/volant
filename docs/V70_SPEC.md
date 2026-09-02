# v0.70 — GroupConsumer earliest via ListOffsets

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V62_SPEC.md](./V62_SPEC.md):
“`earliest` via ListOffsets earliest — 0 is the native log start on a
single-node leader.” After DeleteRecords (v0.52 / v0.65) the log start
can be **> 0**. Hardcoding position 0 on `auto_offset_reset=earliest`
then fetches a truncated prefix.

`latest` already calls `list_offsets` and uses the `latest` field. This
slice uses the same RPC and the `earliest` field.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. In `_apply_reset` / `applyReset`: when policy is `earliest` and
   OffsetFetch missed / `OFFSET_UNKNOWN`, call existing
   `list_offsets(topic, partitions)` and set position to each entry’s
   **`earliest`**.
2. If ListOffsets fails or a wanted partition is missing from the
   reply → raise (same as `latest` today). Do **not** silently fall
   back to 0.
3. `latest` and `none` stay exactly as v0.62.
4. Default policy stays **`earliest`**. Existing join signatures stay
   valid.
5. OffsetFetch hit (committed, not UNKNOWN) still wins; no ListOffsets
   in that case.
6. Empty assignment: no ListOffsets.
7. This is **not** Kafka timestamp / isolation reset. Native 48/49
   already returns both ends.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `auto.offset.reset` (timestamp / isolation) | Native 48/49 has no timestamp selector |
| Rust `GroupConsumer` reset | Still OffsetFetch or 0 |
| New native opcodes / Kafka API keys | Reuse 48 / 49 |
| Broker / protocol / Rust client changes | Frozen |
| Phase 155 / homemade Raft | Frozen |

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
            └─ none → raise; do not fetch records
```

Empty assignment skips OffsetFetch **and** ListOffsets.

## API

No new public methods. Existing join signatures stay valid:

```python
GroupConsumer.join(..., auto_offset_reset="earliest")  # default
GroupConsumer.join(..., auto_offset_reset="latest")
GroupConsumer.join(..., auto_offset_reset="none")
```

```go
JoinGroupConsumer(..., WithAutoOffsetReset("earliest"))
// default earliest
```

```java
GroupConsumer.join(...)                         // earliest
GroupConsumer.joinWithOffsetReset(..., "latest")
```

## Tests

No broker; fake Client / mock backend / fake TCP:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| earliest + OffsetFetch miss + ListOffsets earliest=7 latest=20 | position 7; one ListOffsets RPC |
| earliest + ListOffsets missing partition | raise |
| latest still uses `latest` | position 20 in the same fixture |
| committed offset=3 | position 3; zero ListOffsets |
| none + miss | still raises; no position 0 |

Existing group tests that join with default earliest and no committed
offset now answer ListOffsets (`earliest=0`, `latest=…`) so they do
not hang on opcode 48.

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Fake Client: earliest / missing / latest / committed |
| `clients/go/group_test.go` | Fake TCP: same |
| `clients/java/.../GroupConsumerTest.java` | Mock backend: same |

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/group.py` | `_apply_reset` uses ListOffsets earliest |
| `clients/go/group.go` | `applyReset` same |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | `applyReset` same |
| `clients/{python,go,java}/README.md` | Honesty: earliest is ListOffsets earliest |
| `docs/V70_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `auto.offset.reset`.** No timestamp reset. `earliest` /
  `latest` are the two ends of native ListOffsets 48/49.
- **Rust `GroupConsumer` still starts at 0 / OffsetFetch only.**
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling **v0.69** also edits `group.py` / `group.go` /
`GroupConsumer.java` (`_local_range_assignment`). Keep this hunk
local to `_apply_reset` / offset-reset docs. Do not change who
`range_assign_multi` receives.

Do not drop auto_commit + heartbeat + assignor + instance id + this
reset knob to resolve a conflict.

## Related

- [V62_SPEC.md](./V62_SPEC.md) — `auto_offset_reset` (earliest was 0)
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on language clients
- [V52_SPEC.md](./V52_SPEC.md) — DeleteRecords (log start can be > 0)
- [V65_SPEC.md](./V65_SPEC.md) — DeleteRecords leader redirect
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
