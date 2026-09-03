# v0.153 — Rust single-entry OffsetCommit

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V119_SPEC.md](./V119_SPEC.md) /
[V128_SPEC.md](./V128_SPEC.md) / [V139_SPEC.md](./V139_SPEC.md):
language clients already have one-entry OffsetCommit helpers. Rust
only has batch `Client::commit_offsets(group, member_id, generation,
entries)`.

Python `offset_commit(..., member_id=, generation=, metadata=)`, Go
`OffsetCommit` / `OffsetCommitMeta` / `OffsetCommitMember` /
`OffsetCommitMemberMeta`, and Java 4–7 arg `offsetCommit` already
send one entry. Add the matching Rust convenience without changing
`commit_offsets`. Reuse the existing OffsetCommit send loop (v0.83
retry + v0.98 error 14). This is **not** Kafka OffsetCommit versions.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Go, or Java.

## Goals

1. **Rust:** public `Client::commit_offset(group_id, topic, partition,
   offset)` wrapping `commit_offset_meta` with `metadata=""`.
2. **Rust:** public `Client::commit_offset_meta(..., metadata)` wrapping
   `commit_offsets(group, "", 0, vec![OffsetCommitEntry{...}])`.
3. **Rust:** public `Client::commit_offset_member(..., member_id,
   generation)` wrapping `commit_offset_member_meta` with
   `metadata=""`.
4. **Rust:** public `Client::commit_offset_member_meta(..., member_id,
   generation, metadata)` wrapping `commit_offsets` with one entry.
5. Do **not** change `commit_offsets`. Error 14 / transient retry
   inherit from that method. Do not reimplement the RPC.
6. `generation = 0` still skips the broker generation check.
7. No new constructor args. Default retry / redirect knobs unchanged.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `commit_offsets` | Already public (v0.119); frozen |
| Python `offset_commit(...)` | Already public |
| Go `OffsetCommit` / `OffsetCommitMeta` / `OffsetCommitMember` / `OffsetCommitMemberMeta` | Already public (v0.128 / v0.139) |
| Java 4–7 arg `offsetCommit` | Already public |
| Kafka OffsetCommit versions / txn offset commit | Native opcode 6 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Changing GroupConsumer commit policy | Thin Client only; GroupConsumer already batches via `commit_offsets` |

## API

```rust
c.commit_offset("g", "t", 0, 5).await?;                    // empty member, gen 0, empty metadata
c.commit_offset_meta("g", "t", 0, 5, "consumer-1").await?; // admin path + metadata
c.commit_offset_member("g", "t", 0, 5, "m1", 3).await?;    // one entry, member+gen
c.commit_offset_member_meta("g", "t", 0, 5, "m1", 3, "consumer-1").await?;
c.commit_offsets("g", "m1", 3, vec![OffsetCommitEntry { ... }]).await?; // unchanged
```

`commit_offset` calls `commit_offset_meta` with `metadata=""`.
`commit_offset_meta` calls `commit_offsets(group, "", 0, vec![one
entry])`. `commit_offset_member` calls `commit_offset_member_meta`
with `metadata=""`. `commit_offset_member_meta` calls
`commit_offsets(group, member_id, generation, vec![one entry])`.

`generation = 0` skips the broker generation check (same as today).

## Semantics

- `commit_offset` / `commit_offset_meta` send empty member and
  generation 0 (admin path).
- `commit_offset_member` / `commit_offset_member_meta` encode the
  given member + generation on the one OffsetCommit RPC.
- `commit_offset` / `commit_offset_member` send empty per-entry
  metadata.
- `commit_offset_meta` / `commit_offset_member_meta` encode the given
  metadata string on the one entry.
- Empty `member_id` + `generation=0` is allowed (admin path).
- Transient 6 / 7 / 15 / 16 and transport retry via existing
  `commit_offsets` (v0.83; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.98 `offset_admin_round_trip`).
- Not Kafka OffsetCommit versions / `TxnOffsetCommit`.

## Tests

Fake TCP stub that decodes OffsetCommit request entries.

| Case | Expect |
|------|--------|
| `commit_offset` | empty member, gen 0, empty metadata, one entry |
| `commit_offset_meta(..., "consumer-1")` | that metadata; still admin member/gen |
| `commit_offset_member(..., "m1", 3)` | member m1, gen 3, empty metadata |
| `commit_offset_member_meta(..., "m1", 3, "consumer-1")` | all four fields |

```bash
cargo test -p volant-client -- --test-threads=1
```

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | four thin wrappers on `commit_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc sentence |
| `crates/volant-client/tests/v153_commit_offset.rs` | four encode cases |
| `docs/V153_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetCommit versions / `TxnOffsetCommit`.
- Native opcode **6** only. Member + generation are already on the wire.
- `generation = 0` still skips the broker generation check.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python, Go, and Java clients are unchanged.
- `commit_offsets` is unchanged.

## Merge notes

Sibling slices that also edit Rust `Client` should keep this hunk
local to OffsetCommit:

- **Keep the one-entry convenience only.** Do not change
  `commit_offsets` (v0.83 retry + v0.98 14).
- Do not reimplement the RPC / retry / redirect loop.
- Do not change Python, Go, or Java.
- Do not change the broker, Kafka shim, or protocol in this merge.

Expect conflicts on:

- `crates/volant-client/src/client.rs` (`commit_offset*` next to
  `commit_offsets`)
- `crates/volant-client/src/lib.rs` (crate-doc)

The hunk is local to OffsetCommit wrappers.

## Related

- [V83_SPEC.md](./V83_SPEC.md) — Rust OffsetCommit / OffsetFetch /
  DeleteOffsets transient retry
- [V98_SPEC.md](./V98_SPEC.md) — OffsetCommit / OffsetFetch /
  DeleteOffsets error 14
- [V119_SPEC.md](./V119_SPEC.md) — language public CommitOffsets batch
- [V128_SPEC.md](./V128_SPEC.md) — Go/Java OffsetCommit metadata
- [V139_SPEC.md](./V139_SPEC.md) — Go OffsetCommit member + generation
