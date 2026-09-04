# v0.254 — TxnOffsetCommit v3+ generation/member fence

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Kafka **TxnOffsetCommit** (key **28**) v3+ already parses
`generation` / `member` / `instance` and **ignores** them. Honor
generation/member with the **existing** OffsetCommit fence. Empty
member (or `generation < 0`) still skips (txn-coordinator / admin
path). Instance id stays parsed and ignored.

This is residual **v0.254**. It is **not** join-set wait and **not** a
new Kafka key. Native OffsetCommit fence is reused, not rewritten.

## Goals

1. After parsing `generation` / `member` / `instance` on
   TxnOffsetCommit v3+:
   - Non-empty `member` **and** `generation >= 0`: same checks as
     `GroupCoordinator::commit_offsets` (unknown group/member → native
     **10**, wrong gen → **11**, `synced_generation != generation` →
     **9**).
   - Map via `map_group_error` (`9→27`, `10→25`, `11→22`).
   - Fence fail: do **not** buffer/commit any offsets. Stamp that
     Kafka error on **every** parsed partition.
   - Empty member **or** `generation < 0`: unchanged (no membership
     check).
   - Instance id: parse, ignore.
2. v0–2 have no generation/member fields — unchanged.
3. TransactionalId ACL Write (v0.247) stays as-is when txn id is
   non-empty.
4. Do **not** add Kafka keys. Do **not** change `SUPPORTED_APIS`.
   Do **not** change hard `== 52` asserts.

## Non-goals

| Deferred | Why |
|----------|-----|
| Join-set / PreparingRebalance | Not this leftover |
| Native OffsetCommit / SyncGroup apply rewrite | Reuse existing fence |
| WriteTxnMarkers / AssignReplicasToDirs / telemetry keys | Sibling leftovers |
| New Kafka API keys | Frozen |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
TxnOffsetCommit v3+
  │
  ├─ empty member OR generation < 0
  │     → today's skip; buffer as before
  ├─ non-empty member AND generation >= 0
  │     ├─ unknown group / unknown member → 25 on every partition
  │     ├─ generation != group.generation → 22
  │     ├─ synced_generation != generation → 27 (not stored)
  │     └─ else buffer (open txn) / 0
  └─ v0–2
        → no generation/member fields; unchanged
```

Parse failure after the fence fields still returns today's empty
topic list. If topics/partitions parsed, they are all stamped.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-broker --test v254_txn_offset_commit_fence -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Join, no SyncGroup; v3 member+gen | **27** on every partition; offsets not stored |
| After SyncGroup, matching member+gen | **0**; offsets visible after EndTxn |
| Empty member + v3 | skip fence; **0** |
| Unknown member | **25** on partitions |

Sequential fence tests use a short rebalance timeout (150ms) so a
parked second Join cannot sit on the 10s session.

## Honesty leftovers

- Not CompletingRebalance / PreparingRebalance join-set wait.
- Instance id is still ignored (no static-member mapping on this API).
- v0–2 cannot carry generation/member.
- Offsets still buffer until EndTxn (Phase 31); the fence only blocks
  the buffer.

## Merge notes

Keep this hunk local to `encode_txn_offset_commit` + a shared
`check_commit_fence` used by native OffsetCommit. Do **not** edit
living docs except the one-line `docs/KAFKA_COMPAT.md` TxnOffsetCommit
row.

## Related

- [V219_SPEC.md](./V219_SPEC.md) — native OffsetCommit SyncGroup fence
- [V247_SPEC.md](./V247_SPEC.md) — TransactionalId ACL Write
- [V248_SPEC.md](./V248_SPEC.md) — SyncGroup apply
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — shim matrix
