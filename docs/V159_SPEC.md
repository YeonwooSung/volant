# v0.159 — Rust OffsetFetch all-group helper

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V118_SPEC.md](./V118_SPEC.md) /
[V122_SPEC.md](./V122_SPEC.md) / [V154_SPEC.md](./V154_SPEC.md):
language clients gained `OffsetFetchAll` / `offset_fetch_all` that
return the whole group. Rust already has `fetch_offsets(group, entries)`
where empty entries means all, plus `fetch_offsets_for_topic` (v0.154).
There is no named all-group helper.

Add `Client::fetch_offsets_all`. Reuse `fetch_offsets` (do not
reimplement the RPC). `fetch_offsets` and `fetch_offsets_for_topic`
stay unchanged. This is **not** Kafka OffsetFetch versions /
require-stable.

This is residual **v0.159** (Rust OffsetFetch all-group named helper).
It is **not** Phase 159 work. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::fetch_offsets_all(group_id)` that calls
   `fetch_offsets(group_id, Vec::new())` (empty wire entries = all
   group offsets).
2. Return `Vec<OffsetFetchEntry>` including already-decoded metadata.
3. Inherit retry / error **14** from `fetch_offsets` (`offset_admin_round_trip`:
   v0.83 transient retry + v0.98 error 14). No new retry policy.
4. Do **not** change `fetch_offsets` or `fetch_offsets_for_topic`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `fetch_offsets` / `fetch_offsets_for_topic` | Frozen; empty entries already mean all |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Go / Java | Already have `offset_fetch_all` / `OffsetFetchAll` (v0.118) |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Fetch every committed offset for `group` (empty wire entries).
/// Same as `fetch_offsets(group_id, vec![])`.
pub async fn fetch_offsets_all(&self, group_id: &str) -> Result<Vec<OffsetFetchEntry>>
```

```rust
let all = client.fetch_offsets_all("g").await?;        // all group offsets, metadata kept
let same = client.fetch_offsets("g", vec![]).await?;   // unchanged: same rows
```

## Semantics

- Empty wire entries = all group offsets (same as `fetch_offsets`
  with `vec![]`).
- Returned rows are public `OffsetFetchEntry` (topic, partition,
  offset, metadata).
- `fetch_offsets` is unchanged (empty entries still mean all).
- `fetch_offsets_for_topic` is unchanged (v0.154 client-side filter).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `fetch_offsets` / `offset_admin_round_trip` (v0.83; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.98).
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP stub that injects a scripted OffsetFetch reply with two
topics; one entry has metadata `"consumer-1"`.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `fetch_offsets_all("g")` | same entries as `fetch_offsets("g", vec![])` including metadata |

Existing `v154_fetch_offsets_topic.rs` and `v83_offset_admin_retry.rs`
must still pass (`fetch_offsets` / `fetch_offsets_for_topic` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `fetch_offsets_all` wraps `fetch_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v159_fetch_offsets_all.rs` | fake TCP two-topic reply |
| `docs/V159_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- All-group fetch is still empty wire entries (same as today).
- `fetch_offsets` and `fetch_offsets_for_topic` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the OffsetFetch all-group helper:

- **Keep the named wrapper only.** Do not change `fetch_offsets` or
  `fetch_offsets_for_topic`.
- Do not change the OffsetFetch send loop (v0.83 retry + v0.98 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `fetch_offsets_all` after `fetch_offsets_for_topic`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V154_SPEC.md](./V154_SPEC.md) — Rust OffsetFetch topic + metadata
- [V148_SPEC.md](./V148_SPEC.md) — language OffsetFetch topic + metadata
- [V140_SPEC.md](./V140_SPEC.md) — Go/Java OffsetFetch entry metadata
- [V122_SPEC.md](./V122_SPEC.md) — language OffsetFetch entries
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
- [V83_SPEC.md](./V83_SPEC.md) — Rust OffsetCommit / OffsetFetch retry
- [V98_SPEC.md](./V98_SPEC.md) — Rust OffsetFetch error 14
