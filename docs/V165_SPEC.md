# v0.165 — Rust DeleteOffsets helpers

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V54_SPEC.md](./V54_SPEC.md) /
[V158_SPEC.md](./V158_SPEC.md): language clients gained
`DeleteOffsetsAll` (v0.158) and one-entry `DeleteOffset` (v0.164).
Rust only has `Client::delete_offsets(group_id, entries)` where empty
entries means all. There is no named all-group helper and no
one-entry helper.

Add `Client::delete_offsets_all` and `Client::delete_offset`. Reuse
`delete_offsets` (do not reimplement the RPC). `delete_offsets` stays
unchanged. This is **not** Kafka OffsetDelete.

This is residual **v0.165** (Rust DeleteOffsets helpers). It is **not**
Phase 165 work. It does **not** open Phase 155, add Kafka API keys,
add native opcodes, or change the broker, protocol, or Python/Go/Java.

## Goals

1. Add public `Client::delete_offsets_all(group_id)` that calls
   `delete_offsets(group_id, Vec::new())` (empty wire entries = all
   group offsets).
2. Add public `Client::delete_offset(group_id, topic, partition)` that
   calls `delete_offsets` with one `OffsetEntry { topic, partition }`.
3. Inherit retry / error **14** from `delete_offsets`
   (`offset_admin_round_trip`: v0.83 transient retry + v0.98 error 14).
   No new retry policy.
4. Do **not** change `delete_offsets`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `delete_offsets` | Frozen; empty entries already mean all |
| Kafka OffsetDelete (API key 47) | Native opcode 38 only |
| Kafka DeleteGroups (API key 42) | No native DeleteGroups opcode |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Go / Java | Already have group-only / one-entry helpers |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Delete every committed offset for `group_id` (empty wire entries).
pub async fn delete_offsets_all(&self, group_id: &str) -> Result<DeleteOffsetsResult> {
    self.delete_offsets(group_id, Vec::new()).await
}

/// Delete one committed offset.
pub async fn delete_offset(&self, group_id: &str, topic: &str, partition: u32) -> Result<DeleteOffsetsResult> {
    self.delete_offsets(
        group_id,
        vec![OffsetEntry { topic: topic.to_owned(), partition }],
    ).await
}
```

```rust
let _ = client.delete_offsets_all("g").await?;           // all group offsets
let _ = client.delete_offset("g", "t", 0).await?;        // one entry t/0
let _ = client.delete_offsets("g", vec![]).await?;       // unchanged: same as all
```

## Semantics

- Empty wire entries = all committed offsets for the group (same as
  today).
- `delete_offsets_all` is a named wrapper; it does not re-encode.
- `delete_offset` sends one `OffsetEntry` (`topic` + `partition`).
- `delete_offsets(group_id, entries)` is unchanged (empty still means
  all).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `delete_offsets` / `offset_admin_round_trip` (v0.83; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.98).
- Not Kafka OffsetDelete / DeleteGroups.

## Tests

Fake TCP stub that records decoded DeleteOffsets entries.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `delete_offsets_all("g")` | empty entries list |
| `delete_offset("g", "t", 0)` | one entry t/0 |

Existing `v98_delete_offsets_14.rs` and `v83_offset_admin_retry.rs`
must still pass (`delete_offsets` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `delete_offsets_all` / `delete_offset` wrap `delete_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v165_delete_offset.rs` | fake TCP empty / one-entry wire check |
| `docs/V165_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetDelete / DeleteGroups.
- Empty entries still delete **all** committed offsets for the group.
- `delete_offsets` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the DeleteOffsets helpers:

- **Keep the named wrappers only.** Do not change `delete_offsets`.
- Do not change the DeleteOffsets send loop (v0.83 retry + v0.98 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `delete_offsets_all` / `delete_offset` after `delete_offsets`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V54_SPEC.md](./V54_SPEC.md) — language DeleteOffsets
- [V78_SPEC.md](./V78_SPEC.md) — language DeleteOffsets transient retry
- [V83_SPEC.md](./V83_SPEC.md) — Rust OffsetCommit / OffsetFetch / DeleteOffsets retry
- [V97_SPEC.md](./V97_SPEC.md) — language DeleteOffsets error 14
- [V98_SPEC.md](./V98_SPEC.md) — Rust DeleteOffsets error 14
- [V158_SPEC.md](./V158_SPEC.md) — Go DeleteOffsetsAll
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — native 38/39
