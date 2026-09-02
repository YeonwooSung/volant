# v0.62 — GroupConsumer auto_offset_reset on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V50_SPEC.md](./V50_SPEC.md):
“GroupConsumer does not call ListOffsets to seed a fetch position.”
Today an OffsetFetch miss / `OFFSET_UNKNOWN` (`u64::MAX`) becomes **0**
(log start). This slice adds opt-in `auto_offset_reset` matching a
**tiny** Kafka subset, using native ListOffsets (48/49) already on the
language clients.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker or Rust client.

## Goals

1. When joining / after rebalance, for each **newly assigned**
   partition: OffsetFetch as today. If a committed offset exists and is
   **not** `OFFSET_UNKNOWN` → use it. Else apply reset.
2. **`earliest`** (default, current behavior): position **0**. Do **not**
   require a ListOffsets RPC — 0 is the native log start / same as today.
3. **`latest`**: `list_offsets(topic, [partition])` and use `latest`
   (LEO). If ListOffsets fails or the partition is missing from the
   reply, raise.
4. **`none`**: raise a clear error (`ValueError` / `fmt.Errorf` /
   `IllegalStateException`) — do not start at 0.
5. Empty assignment: no ListOffsets.
6. Invalid reset string → join fails **before** JoinGroup.
7. Default **`earliest`** so existing group tests that expect 0 still
   pass. Existing join signatures stay valid.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `auto.offset.reset` (timestamp / isolation) | Native 48/49 has no timestamp selector |
| `earliest` via ListOffsets earliest | 0 is the native log start on a single-node leader |
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
            ├─ earliest (default) → position 0 (no ListOffsets)
            ├─ latest → ListOffsets; position = latest (LEO); fail if
            │            RPC errors or the partition is missing
            └─ none → raise; do not fetch records
```

Empty assignment skips OffsetFetch **and** ListOffsets.

## API

Keep existing join signatures. Additive:

```python
GroupConsumer.join(..., auto_offset_reset="earliest")  # default
GroupConsumer.join(..., auto_offset_reset="latest")
GroupConsumer.join(..., auto_offset_reset="none")
# invalid string → ValueError at join, before JoinGroup
```

```go
JoinGroupConsumer(..., WithAutoOffsetReset("latest"))
// default earliest; invalid string → error from Join
```

```java
GroupConsumer.joinWithOffsetReset(client, group, topics, timeoutMs, "latest")
// named method so it does not collide with join(..., String assignor)
// or joinStatic instance id
```

Combine with existing `group_instance_id` / heartbeat / assignor /
auto_commit:

- Python: `auto_offset_reset=` keyword on the same `join`
- Go: `WithAutoOffsetReset` composes with the other options
- Java: `joinWithAutoCommit` is unchanged (reset stays `earliest`).
  `joinWithOffsetReset` takes auto-commit **off**. Do not add another
  `join(..., String)` overload.

## Tests

No broker; fake Client / mock backend / fake TCP:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

1. Default / earliest: OffsetFetch unknown → position 0; **no**
   ListOffsets RPC.
2. latest: OffsetFetch unknown → ListOffsets called; position = latest
   (e.g. 10).
3. latest with a committed offset → use committed; no ListOffsets.
4. none + unknown → raise; no fetch of records.
5. Invalid reset string → join fails.
6. Existing group tests still pass (`auto_commit` / heartbeat /
   assignor).

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Fake Client: earliest / latest / none / invalid |
| `clients/go/group_test.go` | Fake TCP: same (`WithAutoOffsetReset`) |
| `clients/java/.../GroupConsumerTest.java` | Mock backend: same (`joinWithOffsetReset`) |

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/group.py` | `auto_offset_reset=` |
| `clients/go/group.go` | `WithAutoOffsetReset` |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | `joinWithOffsetReset` |
| `clients/{python,go,java}/README.md` | Usage + honesty |
| `docs/V62_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `auto.offset.reset`.** No timestamp reset. `latest` is
  LEO (native ListOffsets 48/49).
- **`earliest` is 0**, not a ListOffsets earliest read (usually the
  same on a single-node leader).
- **Rust `GroupConsumer` still starts at 0 / OffsetFetch only.**
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Siblings edit Client, not group files (except maybe README). Keep
auto_commit + heartbeat + assignor + instance id + this reset knob.

Do not drop any of those knobs to resolve a conflict.

## Related

- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
- [V48_SPEC.md](./V48_SPEC.md) — language auto-commit
- [V50_SPEC.md](./V50_SPEC.md) — ListOffsets on language clients
- [V60_SPEC.md](./V60_SPEC.md) — Rust auto-commit
