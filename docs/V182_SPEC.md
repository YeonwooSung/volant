# v0.182 — Rust metadata_topic

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V114_SPEC.md](./V114_SPEC.md) /
[V157_SPEC.md](./V157_SPEC.md): Rust already has `Client::metadata()`
(all topics) and `Client::metadata_topics(Vec<String>)`. There is no
named one-topic helper. A sibling language slice (v0.181) is adding
singular wrappers.

Add `Client::metadata_topic`. Reuse `metadata_topics` (do not
reimplement the RPC). `metadata` / `metadata_topics` stay unchanged.
Hunt still uses `metadata_rpc` (no recursion). This is **not** Kafka
Metadata.

This is residual **v0.182** (Rust metadata_topic). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Python, Go, or Java.

## Goals

1. Add public `Client::metadata_topic(topic)` that calls
   `metadata_topics(vec![topic.to_owned()])` (one topic name on the
   wire).
2. Return `Metadata`.
3. Inherit retry / error **14** from `metadata_topics`
   (`metadata_list_members_round_trip`: v0.96 transient retry +
   v0.157 error 14). No new retry policy.
4. Do **not** change `metadata` / `metadata_topics` / `metadata_rpc`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `metadata` / `metadata_topics` / `metadata_rpc` | Frozen; all-topics / named-list / hunt already exist |
| Kafka Metadata `allow_auto_topic_creation` / topic ids | Native opcode 4 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Sibling **v0.181** singular wrappers |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Fetch cluster metadata for one topic.
///
/// Same as `metadata_topics` with a single topic name.
pub async fn metadata_topic(&self, topic: &str) -> Result<Metadata> {
    self.metadata_topics(vec![topic.to_owned()]).await
}
```

```rust
let one = client.metadata_topic("events").await?;                 // one topic
let same = client.metadata_topics(vec!["events".into()]).await?;  // unchanged: same wire
let all = client.metadata().await?;                               // unchanged: empty = all
```

## Semantics

- `metadata_topic` sends one topic name on the native Metadata
  `topics` list.
- `metadata_topic` is a named wrapper; it does not re-encode.
- `metadata()` is unchanged (empty list = all topics).
- `metadata_topics(topics)` is unchanged (empty still means all).
- Hunt still uses private `metadata_rpc` (no 14 wrap; no recursion).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `metadata_topics` / `metadata_list_members_round_trip` (v0.96;
  default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.157).
- Native Metadata has no top-level error_code; failures arrive as
  `Response::Error` or transport.
- Not Kafka Metadata versions / `allow_auto_topic_creation` / topic
  ids.

## Tests

Fake TCP stub that records decoded Metadata topics.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `metadata_topic("events")` | wire topics is `["events"]`; same as `metadata_topics(vec!["events".into()])` |
| Existing `metadata` / `metadata_topics` empty / named / retry / 14 cases | still pass |

Existing Metadata retry / 14 tests must still pass
(`metadata` / `metadata_topics` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `metadata_topic` wraps `metadata_topics(vec![topic.to_owned()])` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v182_metadata_topic.rs` | one-topic wire check |
| `docs/V182_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** Metadata (API key 3). Native opcode **4** only. No
  `allow_auto_topic_creation`, topic ids, or authorized operations.
- One-topic fetch is still one wire topic name.
- `metadata` / `metadata_topics` / `metadata_rpc` are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the Metadata one-topic helper:

- **Keep the named wrapper only.** Do not change `metadata` /
  `metadata_topics` / `metadata_rpc`.
- Do not change the Metadata send loop (v0.96 retry + v0.157 14).
- Hunt still uses `metadata_rpc` (no recursion).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `metadata_topic` after `metadata_topics`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
- [V114_SPEC.md](./V114_SPEC.md) — Rust `metadata_topics`
- [V116_SPEC.md](./V116_SPEC.md) — language Metadata topic filter
- [V157_SPEC.md](./V157_SPEC.md) — Rust Metadata error 14
- [V166_SPEC.md](./V166_SPEC.md) — same named-wrapper pattern
