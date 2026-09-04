# v0.262 — persist OffsetCommit committed_leader_epoch

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Kafka **OffsetCommit** (key **8**) v6+ already parses
`committed_leader_epoch` and **ignored** it. OffsetFetch v5+ always
wrote `committed_leader_epoch = -1`. Store the parsed i32 on disk and
return it on OffsetFetch. Old files without the extra field still
read as epoch **-1**. Native OffsetCommit without an epoch still
writes **-1**.

This is residual **v0.262**. It is **not** a new Kafka key and **not**
join-set.

## Goals

1. Extend on-disk format: after metadata bytes, optional `i32` LE
   `committed_leader_epoch`.
   - Write: always append the i32 (native / missing epoch → **-1**).
   - Read: if `buf.len() >= 10 + meta_len + 4`, parse i32; else **-1**
     (legacy files).
2. `StoredOffset` and `OffsetStore::commit` / `fetch` / list helpers
   carry `i32` epoch. Existing `commit(..., metadata)` stays a thin
   wrapper that writes **-1**. Kafka uses `commit_with_epoch`.
3. `GroupCoordinator::commit_offsets` / fetch path plumb epoch so
   Kafka OffsetCommit v6+ stores the parsed i32 and OffsetFetch v5+
   writes that value (still **-1** when unknown). Native
   `commit_offsets` callers keep the old signature and default **-1**.
4. OffsetCommit versions `< 6` store **-1**. OffsetFetch versions
   `< 5` do not write the field (unchanged).
5. Do **not** add Kafka keys. Do **not** change `SUPPORTED_APIS`.
   Do **not** change hard `== 60` asserts.
6. Do **not** invent epoch from the current assignment when the stored
   value is -1 (stay honest: unknown).

## Non-goals

| Deferred | Why |
|----------|-----|
| Invent epoch from assignment / Metadata | Honest: unknown stays **-1** |
| TxnOffsetCommit epoch storage | Sibling leftover; still parsed, ignored |
| RequireStable | Already shipped (v0.256) |
| Expire/Renew delegation tokens, BrokerRegistration, ConsumerGroupDescribe | Sibling leftovers |
| New Kafka API keys | Frozen |
| Crate 0.3.0 | Stays 0.2.0 |

## On-disk

```
{data_dir}/__consumer_offsets/{group}/{topic}/{partition}
  u64 offset LE
  u16 meta_len LE
  UTF-8 metadata
  i32 committed_leader_epoch LE   // optional trailer; missing → -1
```

New writes always include the i32. Dual-read keeps existing files
loadable.

## Semantics

```
OffsetCommit
  │
  ├─ version < 6  OR  native / admin (no epoch)
  │     → store committed_leader_epoch = -1
  └─ version ≥ 6
        → store the parsed i32 (may itself be -1)

OffsetFetch
  │
  ├─ version < 5
  │     → no committed_leader_epoch field (unchanged)
  └─ version ≥ 5
        → write stored i32 (legacy file / missing / native → -1)
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --lib group -- --test-threads=1
cargo test -p volant-broker --test v262_offset_leader_epoch -- --test-threads=1
```

| Case | Expect |
|------|--------|
| OffsetCommit v6 `committed_leader_epoch = 3`, OffsetFetch v5+ | epoch **3**, offset matches |
| Legacy file (offset+meta only, no trailer) | OffsetFetch v5 epoch **-1** |
| OffsetCommit v5 (no epoch field), OffsetFetch v5 | **-1** |
| Native / admin OffsetCommit (empty member, gen 0) | still works; fetch epoch **-1** |

## Honesty leftovers

- Not inventing a leader epoch from the current assignment.
- TxnOffsetCommit still parses `committed_leader_epoch` and does not
  store it (writes **-1** via the native commit path).
