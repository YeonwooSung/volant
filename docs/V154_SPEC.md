# v0.154 — Rust OffsetFetch topic + metadata

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V148_SPEC.md](./V148_SPEC.md) /
[V122_SPEC.md](./V122_SPEC.md) / [V118_SPEC.md](./V118_SPEC.md):
language clients gained `OffsetFetchEntries` / `offset_fetch_entries`
that filter the group OffsetFetch to one topic and keep metadata.
Rust already has `fetch_offsets(group, entries)` returning
`Vec<OffsetFetchEntry>` (metadata included). There is no topic-filter
helper.

Add `Client::fetch_offsets_for_topic`. Reuse `fetch_offsets` (do not
reimplement the RPC). `fetch_offsets` stays unchanged. This is **not**
Kafka OffsetFetch versions / require-stable.

This is residual **v0.154** (Rust OffsetFetch by topic with metadata).
It is **not** Phase 154 work (Phase 154 is already shipped). It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Python/Go/Java.

## Goals

1. Add public `Client::fetch_offsets_for_topic(group_id, topic)` that
   calls `fetch_offsets(group_id, vec![])` (all group offsets) and
   keeps rows whose `e.topic == topic`.
2. Return `Vec<OffsetFetchEntry>` including already-decoded metadata.
3. Inherit retry / error **14** from `fetch_offsets` (`offset_admin_round_trip`:
   v0.83 transient retry + v0.98 error 14). No new retry policy.
4. Do **not** change `fetch_offsets`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `fetch_offsets` | Frozen; already returns metadata |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Go / Java | Already have `offset_fetch_entries` (v0.148) |
| Phase 154 / Phase 155 / homemade Raft | Frozen; Phase 154 already shipped |

## API

```rust
/// Fetch committed offsets for `topic`, including per-entry metadata.
/// Calls `fetch_offsets(group_id, vec![])` (all group offsets) and
/// keeps rows whose topic matches.
pub async fn fetch_offsets_for_topic(
    &self,
    group_id: &str,
    topic: &str,
) -> Result<Vec<OffsetFetchEntry>>
```

```rust
let rows = client.fetch_offsets_for_topic("g", "t").await?; // topic t only, metadata kept
let all = client.fetch_offsets("g", vec![]).await?;         // unchanged: all group offsets
```

## Semantics

- Topic filter stays client-side (empty wire entries = all group
  offsets, then keep the named topic).
- Returned rows are public `OffsetFetchEntry` (topic, partition,
  offset, metadata).
- `fetch_offsets` is unchanged (empty entries still mean all).
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
| `fetch_offsets_for_topic("g", "t")` | only topic t, metadata preserved |
| `fetch_offsets("g", vec![])` | both topics (unchanged) |

Existing `v83_offset_admin_retry.rs` must still pass (`fetch_offsets`
unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `fetch_offsets_for_topic` wraps `fetch_offsets` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v154_fetch_offsets_topic.rs` | fake TCP two-topic reply |
| `docs/V154_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- Topic-filtered fetch is still client-side (empty wire entries).
- `fetch_offsets` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Phase 154 (metadata Raft) is already shipped and is not this slice.
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the OffsetFetch topic filter:

- **Keep the filter wrapper only.** Do not change `fetch_offsets`.
- Do not change the OffsetFetch send loop (v0.83 retry + v0.98 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `fetch_offsets_for_topic` after `fetch_offsets`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V148_SPEC.md](./V148_SPEC.md) — language OffsetFetch topic + metadata
- [V140_SPEC.md](./V140_SPEC.md) — Go/Java OffsetFetch entry metadata
- [V122_SPEC.md](./V122_SPEC.md) — language OffsetFetch entries
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
- [V83_SPEC.md](./V83_SPEC.md) — Rust OffsetCommit / OffsetFetch retry
- [V98_SPEC.md](./V98_SPEC.md) — Rust OffsetFetch error 14
- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — already-shipped metadata Raft
  (not this residual)
