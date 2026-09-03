# v0.166 — Rust list_offsets_all

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V50_SPEC.md](./V50_SPEC.md) /
[V163_SPEC.md](./V163_SPEC.md): Go already has `ListOffsetsAll(topic)`
→ `ListOffsets(topic, nil)`. Java has `listOffsets(topic)` (no
partitions). Python `list_offsets(topic, None)` already lists all.
Rust only has `list_offsets(topic, partitions: Vec<u32>)` — empty
already means all on the wire, but there is no named all-partition
helper.

Add `Client::list_offsets_all`. Reuse `list_offsets` (do not
reimplement the RPC). `list_offsets` stays unchanged. This is **not**
Kafka ListOffsets.

This is residual **v0.166** (Rust list_offsets_all). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Python, Go, or Java.

## Goals

1. Add public `Client::list_offsets_all(topic)` that calls
   `list_offsets(topic, Vec::new())` (empty wire partitions = all
   partitions of the topic).
2. Return `ListOffsetsResult`.
3. Inherit retry / error **13** from `list_offsets` (v0.84 transient
   retry + v0.113 error 13). No new retry policy.
4. Do **not** change `list_offsets(topic, partitions)`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `list_offsets(topic, partitions)` | Frozen; empty already means all |
| Kafka ListOffsets (API key 2) isolation / timestamp | Native opcode 48/49 only |
| Kafka specials (max-timestamp, earliest-local, tiered) | Kafka shim only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Python / Go / Java | Already have topic-only overloads (v0.50 / v0.163) |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// List earliest/latest offsets for every partition of `topic`
/// (empty wire partitions). Same as `list_offsets(topic, vec![])`.
pub async fn list_offsets_all(&self, topic: &str) -> Result<ListOffsetsResult> {
    self.list_offsets(topic, Vec::new()).await
}
```

```rust
let bounds = client.list_offsets_all("events").await?;           // all partitions
let same = client.list_offsets("events", vec![]).await?;         // unchanged: same wire
let filtered = client.list_offsets("events", vec![0, 1]).await?;
```

## Semantics

- Empty wire partitions = all partitions of the topic (same as
  today).
- `list_offsets_all` is a named wrapper; it does not re-encode.
- `list_offsets(topic, partitions)` is unchanged (empty still means
  all).
- Transient 6 / 7 / 15 / 16 and transport retry via `list_offsets`
  (v0.84; default `max_retries=0`).
- Error 13 follows `max_redirects` (v0.113).
- Not Kafka ListOffsets (no timestamp or isolation); both ends of
  each log are returned.

## Tests

Fake TCP stub that records decoded ListOffsets partitions.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `list_offsets_all("t")` | wire partitions empty (count 0); same as `list_offsets("t", vec![])` |
| Existing `list_offsets` empty / explicit / retry / 13 cases | still pass |

Existing ListOffsets retry / 13 tests must still pass
(`list_offsets` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `list_offsets_all` wraps `list_offsets(topic, Vec::new())` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v166_list_offsets_all.rs` | empty-partitions wire check |
| `docs/V166_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** ListOffsets (API key 2). Native opcode **48/49**
  only. No isolation, timestamp, max-timestamp, earliest-local, or
  tiered specials.
- Empty partitions still list **all** partitions of the topic.
- `list_offsets(topic, partitions)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the ListOffsets all-partition helper:

- **Keep the named wrapper only.** Do not change `list_offsets`.
- Do not change the ListOffsets send loop (v0.84 retry + v0.113 13).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `list_offsets_all` after `list_offsets`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V50_SPEC.md](./V50_SPEC.md) — language ListOffsets
- [V82_SPEC.md](./V82_SPEC.md) — language ListOffsets transient retry
- [V84_SPEC.md](./V84_SPEC.md) — Rust ListOffsets transient retry
- [V112_SPEC.md](./V112_SPEC.md) — language ListOffsets error 13
- [V113_SPEC.md](./V113_SPEC.md) — Rust ListOffsets error 13
- [V163_SPEC.md](./V163_SPEC.md) — Go ListOffsetsAll (same wrapper pattern)
- [PHASE15_SPEC.md](./PHASE15_SPEC.md) — native 48/49
