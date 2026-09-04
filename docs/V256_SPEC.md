# v0.256 — OffsetFetch RequireStable honors LSO

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Kafka **OffsetFetch** (key **9**) v7+ already parses
`RequireStable` (v7 single-group; v8+ multi-group) and **ignored** it.
Honor the flag against existing `Broker::is_unstable_offset` (open or
prepared write-through range covering the committed offset). When set
and the offset is still unstable, return Kafka **81**
`UNSTABLE_OFFSET_COMMIT` on that partition and **do not** return the
committed offset (`offset = -1`). Flag unset/absent is unchanged.

This is residual **v0.256**. It is **not** a wait, **not** a new Kafka
key, and **not** join-set.

## Goals

1. Add `KafkaErrorCode::UnstableOffsetCommit = 81`.
2. In `encode_offset_fetch` (v7+) and `encode_offset_fetch_multi` (v8+):
   - Parse `RequireStable` as bool (already consumed as u8 — **use** it).
   - If `require_stable` is true and the fetched committed offset is
     `>= 0` and `broker.is_unstable_offset(topic, partition, offset)`:
     write partition error **81**, offset **-1**, keep metadata.
   - Uncommitted (`-1`) partitions stay error 0 / offset -1.
   - Flag false or version `< 7`: unchanged (return the committed
     offset even if unstable).
3. Do **not** block/wait for LSO. Do **not** change OffsetCommit /
   TxnOffsetCommit.
4. Do **not** add Kafka keys. Do **not** change `SUPPORTED_APIS`.
   Do **not** change hard `== 56` asserts.

## Non-goals

| Deferred | Why |
|----------|-----|
| Wait for LSO | Not a wait; immediate 81 |
| OffsetCommit / TxnOffsetCommit | Orthogonal (fence already shipped) |
| Join-set / PreparingRebalance | Not this leftover |
| PushTelemetry / AlterPartition / DelegationToken | Sibling leftovers |
| New Kafka API keys | Frozen |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
OffsetFetch v7+ / multi-group v8+
  │
  ├─ require_stable = 0  OR  version < 7
  │     → today's committed offset (even if unstable)
  ├─ require_stable = 1
  │     ├─ no committed offset (−1) → error 0, offset −1
  │     ├─ committed ≥ 0 AND is_unstable_offset → 81, offset −1
  │     └─ committed ≥ 0 AND stable (below LSO / no open txn) → 0, offset
  └─ not a wait
```

`is_unstable_offset` is true when the offset sits in an open **or**
prepared write-through range (`[first_offset, end_offset)`).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-broker --test v256_offset_fetch_require_stable -- --test-threads=1
```

| Case | Expect |
|------|--------|
| OffsetFetch v7 `require_stable=0`, offset in open txn | **0**, offset returned |
| Same setup, `require_stable=1` | partition **81**, offset **-1** |
| `require_stable=1`, no open txn / offset below LSO | **0**, offset returned |
| v6 (no RequireStable field) | **0**, offset returned |

## Honesty leftovers

- Not a wait for LSO / last-stable-offset barrier.
- Pending *transactional offset commits* (TxnOffsetCommit not yet
  EndTxn) are a different Kafka path; this slice uses write-through
  produce ranges via `is_unstable_offset`.
- Leader epoch on fetch is still `-1`.

## Merge notes

Keep this hunk local to OffsetFetch encode + `KafkaErrorCode`. Do
**not** edit living docs except the one-line `docs/KAFKA_COMPAT.md`
OffsetFetch row.

## Related

- [PHASE86_SPEC.md](./PHASE86_SPEC.md) — write-through LSO
- [V254_SPEC.md](./V254_SPEC.md) — TxnOffsetCommit fence
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix
