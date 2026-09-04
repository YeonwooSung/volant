# v0.180 — Rust fetch_offset

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V154_SPEC.md](./V154_SPEC.md) /
[V159_SPEC.md](./V159_SPEC.md) / [V165_SPEC.md](./V165_SPEC.md):
Rust already has `Client::fetch_offsets(group, entries)`,
`fetch_offsets_for_topic` (v0.154), and `fetch_offsets_all` (v0.159).
`delete_offset` (v0.165) is the one-partition DeleteOffsets helper.
There is no named one-partition OffsetFetch helper. A sibling
language slice (v0.179) is adding singular wrappers.

Add `Client::fetch_offset`. Reuse `fetch_offsets` (do not reimplement
the RPC). `fetch_offsets` / `fetch_offsets_for_topic` /
`fetch_offsets_all` stay unchanged. This is **not** Kafka OffsetFetch
versions / require-stable.

This is residual **v0.180** (Rust fetch_offset). It is **not** Phase
180 work. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, or Python/Go/Java.

## Goals

1. Add public `Client::fetch_offset(group_id, topic, partition)` that
   calls `fetch_offsets` with one `OffsetEntry { topic, partition }`.
2. Return `Vec<OffsetFetchEntry>` including already-decoded metadata.
3. Inherit retry / error **14** from `fetch_offsets`
   (`offset_admin_round_trip`: v0.83 transient retry + v0.98 error 14).
   No new retry policy.
4. Do **not** change `fetch_offsets` / `fetch_offsets_for_topic` /
   `fetch_offsets_all`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `fetch_offsets` / `fetch_offsets_for_topic` / `fetch_offsets_all` | Frozen; batch / topic / all-group already exist |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Sibling **v0.179** singular wrappers |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Fetch one committed offset.
///
/// Same as `fetch_offsets` with a single `OffsetEntry`.
pub async fn fetch_offset(
    &self,
    group_id: &str,
    topic: &str,
    partition: u32,
) -> Result<Vec<OffsetFetchEntry>> {
    self.fetch_offsets(
        group_id,
        vec![OffsetEntry {
            topic: topic.to_owned(),
            partition,
        }],
    )
    .await
}
```

```rust
let one = client.fetch_offset("g", "t", 0).await?;       // one entry t/0
let all = client.fetch_offsets_all("g").await?;          // unchanged
let topic = client.fetch_offsets_for_topic("g", "t").await?; // unchanged
let same = client.fetch_offsets("g", vec![OffsetEntry {
    topic: "t".into(),
    partition: 0,
}]).await?;                                              // unchanged: same wire
```

## Semantics

- `fetch_offset` sends one `OffsetEntry` (`topic` + `partition`).
- `fetch_offset` is a named wrapper; it does not re-encode.
- Returned rows are public `OffsetFetchEntry` (topic, partition,
  offset, metadata).
- `fetch_offsets(group_id, entries)` is unchanged (empty still means
  all).
- `fetch_offsets_for_topic` is unchanged (v0.154 client-side filter).
- `fetch_offsets_all` is unchanged (v0.159 empty wire entries).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `fetch_offsets` / `offset_admin_round_trip` (v0.83; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.98).
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP stub that records decoded OffsetFetch entries.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `fetch_offset("g", "t", 0)` | one OffsetEntry `{topic: "t", partition: 0}` |

Existing `v154_fetch_offsets_topic.rs`, `v159_fetch_offsets_all.rs`,
and `v83_offset_admin_retry.rs` must still pass (`fetch_offsets` /
`fetch_offsets_for_topic` / `fetch_offsets_all` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `fetch_offset` wraps `fetch_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v180_fetch_offset.rs` | fake TCP one-entry OffsetFetch wire check |
| `docs/V180_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- One-partition fetch is still one wire `OffsetEntry`.
- `fetch_offsets` / `fetch_offsets_for_topic` / `fetch_offsets_all`
  are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the OffsetFetch one-partition helper:

- **Keep the named wrapper only.** Do not change `fetch_offsets` /
  `fetch_offsets_for_topic` / `fetch_offsets_all`.
- Do not change the OffsetFetch send loop (v0.83 retry + v0.98 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `fetch_offset` after `fetch_offsets`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V154_SPEC.md](./V154_SPEC.md) — Rust OffsetFetch topic + metadata
- [V159_SPEC.md](./V159_SPEC.md) — Rust OffsetFetch all-group helper
- [V165_SPEC.md](./V165_SPEC.md) — Rust DeleteOffsets helpers
- [V148_SPEC.md](./V148_SPEC.md) — language OffsetFetch topic + metadata
- [V122_SPEC.md](./V122_SPEC.md) — language OffsetFetch entries
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
- [V83_SPEC.md](./V83_SPEC.md) — Rust OffsetCommit / OffsetFetch retry
- [V98_SPEC.md](./V98_SPEC.md) — Rust OffsetFetch error 14
