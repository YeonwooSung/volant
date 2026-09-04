# v0.203 — Rust create_topic_default

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V199_SPEC.md](./V199_SPEC.md):
Python `create_topic(name)` already defaults `partitions=1`. Go
`CreateTopicDefault(name)` and Java `createTopic(name)` shipped in
**v0.199**. Rust `Client::create_topic(&self, name, partitions)` still
requires an explicit partition count.

Add `Client::create_topic_default`. Reuse `create_topic` (do not
reimplement the RPC). `create_topic` / `create_topic_with_configs`
stay unchanged. This is **not** Kafka CreateTopics.

This is residual **v0.203** (Rust create_topic_default). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, change homemade Raft, or change the broker, protocol,
Python, Go, or Java.

## Goals

1. Add public `Client::create_topic_default(name)` that calls
   `create_topic(name, 1)`.
2. Return `TopicId` (same as `create_topic`).
3. Inherit retry / error **14** from `create_topic` /
   `admin_round_trip` (v0.104 transient retry + v0.79 error 14).
   No new retry policy.
4. Do **not** change `create_topic` / `create_topic_with_configs`
   signatures.
5. Do **not** change Python / Go / Java / broker / protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `create_topic` / `create_topic_with_configs` | Frozen; still require an explicit count |
| Kafka CreateTopics default partitions / replication | Native opcode 3 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Already have partitions=1 helpers (v0.199) |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Create a topic with 1 partition (v0.203).
///
/// Same as `create_topic(name, 1)`.
pub async fn create_topic_default(&self, name: &str) -> Result<TopicId> {
    self.create_topic(name, 1).await
}
```

```rust
let id = client.create_topic_default("events").await?; // partitions=1
let same = client.create_topic("events", 1).await?;    // unchanged: explicit
let many = client.create_topic("events", 3).await?;    // unchanged: explicit
```

Existing `create_topic` / `create_topic_with_configs` signatures are
unchanged.

## Semantics

- Partitions is **1**, same as Python `create_topic(name)` /
  Go `CreateTopicDefault` / Java `createTopic(name)`.
- Wrapper only — do not re-encode.
- `create_topic(name, partitions)` is unchanged (still requires the
  count).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `admin_round_trip` (default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.79).
- Not Kafka CreateTopics default partitions / replication.

## Tests

Fake TCP stub that records decoded CreateTopic name + partitions.
Assert `create_topic_default("events")` encodes partitions **1**.

```bash
cargo test -p volant-client --test v203_create_topic_default -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `create_topic_default("events")` | encodes name=`events` partitions **1**; returns `TopicId` |

Existing `v104_admin_round_trip_retry.rs` / `v79_admin_not_controller.rs`
must still pass (`create_topic` / `create_topic_with_configs`
unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `create_topic_default` wraps `create_topic(name, 1)` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v203_create_topic_default.rs` | fake TCP partitions=1 wire check |
| `docs/V203_SPEC.md` | This spec |

Do **not** change broker / protocol / Python / Go / Java. Do **not**
run the full workspace.

## Honesty leftovers

- `create_topic(name, partitions)` still requires an explicit count.
- Go `CreateTopic` still discards the topic id (out of slice; use
  `CreateTopicID`).
- Not Kafka CreateTopics.
- No Kafka API keys / opcodes / Phase 155.
- Python / Go / Java / broker / protocol unchanged.

## Merge notes

This is the only Rust slice in the batch. Sibling v0.201 / v0.202 do
not touch Rust. Hunk is local to `create_topic_default` + crate-doc
tail + new test file.

- **Keep the named wrapper only.** Do not change `create_topic` /
  `create_topic_with_configs`.
- Do not change the CreateTopic send loop (v0.104 retry + v0.79 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `create_topic_default` after `create_topic`
- `crates/volant-client/src/lib.rs` (crate-doc tail)

## Related

- [V199_SPEC.md](./V199_SPEC.md) — Go/Java CreateTopic partitions=1 helpers
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin error 14
- [V182_SPEC.md](./V182_SPEC.md) — same named-wrapper crate-doc style
- [V172_SPEC.md](./V172_SPEC.md) — same named-wrapper pattern
