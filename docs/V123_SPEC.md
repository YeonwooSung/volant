# v0.123 — Python GroupConsumer batch OffsetCommit

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V119_SPEC.md](./V119_SPEC.md) /
[V31_SPEC.md](./V31_SPEC.md): language `Client.commit_offsets` already
sends one OffsetCommit with N entries, and Go / Java `GroupConsumer`
already batch via that path. Python `GroupConsumer._commit_unlocked`
still looped one-entry `offset_commit` per assigned position.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Rust, Go, or Java.

## Goals

1. Python `GroupConsumer._commit_unlocked` builds a list of
   `OffsetCommitEntry` (same assigned-set filter as today) and calls
   `Client.commit_offsets(..., member_id=..., generation=...)` once.
2. Empty positions: reset the auto-commit clock, no RPC (same as today).
3. Keep joined `member_id` + `generation` (not the admin empty-member
   path). Per-entry metadata stays `""`.
4. Auto-commit / leave / explicit `commit()` inherit the batch path.
5. No new public methods. Do not change `Client.commit_offsets`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Go / Java GroupConsumer | Already batch via `CommitOffsets` / list `offsetCommit` |
| Rust GroupConsumer | Already one OffsetCommit |
| `Client.commit_offsets` / one-entry `offset_commit` | Already public (v0.119) |
| ListMembers / OffsetFetch / DescribeGroup wraps | Other residuals |
| Broker / protocol / Kafka OffsetCommit versions | Frozen |
| Phase 155 / homemade Raft | Frozen |

## Behavior

```python
g.commit()  # one OffsetCommit for all assigned positions
```

- Two assigned positions → one OffsetCommit with two entries.
- Unassigned keys in `_positions` are still skipped.
- Empty `_positions` → zero OffsetCommit RPCs; clock still resets.

## Tests

Fake `Client` in `clients/python/tests/test_group.py`.

| Case | Expect |
|------|--------|
| Two assigned positions | one `commit_offsets` with two entries, member + generation set |
| Empty positions | zero OffsetCommit RPCs |
| Unassigned position in `_positions` | still skipped |
| Existing auto-commit / leave-commits-pending | still pass |

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

## Honesty leftovers

- **Not Kafka** OffsetCommit versions / `TxnOffsetCommit`.
- Native opcode **6** only. Per-entry metadata is already on the wire
  and stays `""` on the GroupConsumer path.
- Empty assignment still commits leftover `_positions` (same as today:
  skip filter only when the assigned set is non-empty).
- Default `max_retries` / `max_redirects` unchanged (inherit
  `commit_offsets`).
- **No Kafka API keys / opcodes / Phase 155.**
- Rust / Go / Java GroupConsumer and `Client.commit_offsets` are
  unchanged.

## Merge notes

Touches Python `group.py` + `test_group.py` only (optional one-line
README / this spec). Expect **no** collision on `Client.py`.

Do not wrap ListMembers / OffsetFetch / DescribeGroup. Do not change
the OffsetCommit send loop. Do not change the broker, Kafka shim, or
Rust / Go / Java in this merge.

## Related

- [V119_SPEC.md](./V119_SPEC.md) — language public CommitOffsets batch
- [V31_SPEC.md](./V31_SPEC.md) / [V32_SPEC.md](./V32_SPEC.md) /
  [V33_SPEC.md](./V33_SPEC.md) — GroupConsumer commit with member + generation
- [V48_SPEC.md](./V48_SPEC.md) — opt-in auto-commit inherits via `_commit_unlocked`
