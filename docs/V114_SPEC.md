# v0.114 — Rust metadata topic filter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V96_SPEC.md](./V96_SPEC.md):
language clients can already send a native Metadata `topics` list
(Python `metadata(topics=…)`; Go / Java encode the same field). Rust
`Client::metadata` always sends `topics: vec![]` (all topics).

Add a thin public method that sends a topic filter. Keep
`metadata()` as “all topics” (empty list). Reuse
`metadata_list_members_round_trip` so Metadata retry (v0.96) is
inherited. No new retry policy. Empty `topics` remains “all”. This
is **not** Kafka Metadata `allow_auto_topic_creation` / topic ids.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / `volant-protocol` / language clients (they
already have the field).

## Goals

1. Keep `Client::metadata()` as “all topics” (`Vec::new()`). Existing
   signatures stay.
2. Add `Client::metadata_topics(topics: Vec<String>)` that sends
   `Request::Metadata { topics }` and uses the same decode / retry /
   error handling as today’s `metadata()`.
3. Reuse `metadata_list_members_round_trip` so v0.96 transient retry
   is inherited. No new retry policy.
4. Empty `topics` remains “all” (current broker / protocol behavior).
5. Do **not** wrap DeleteRecords (v0.111) or ListOffsets 13 (v0.113).
6. Do **not** change broker / protocol. The Metadata request already
   has a `topics` field.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `allow_auto_topic_creation` / topic ids | Native opcode 4 only; not Kafka |
| Language clients | Already have `metadata(topics=…)` / equivalent |
| DeleteRecords wrap (v0.111) / ListOffsets 13 (v0.113) | Sibling residuals |
| New retry policy | Inherit v0.96 via `metadata_list_members_round_trip` |
| Broker / protocol / new opcodes | Frozen; field already exists |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
pub async fn metadata(&self) -> Result<Metadata> {
    self.metadata_topics(Vec::new()).await
}

pub async fn metadata_topics(&self, topics: Vec<String>) -> Result<Metadata> {
    // same decode / retry / error handling as today's metadata()
    self.metadata_list_members_round_trip(Request::Metadata { topics }).await?
    ...
}
```

Name is `metadata_topics` (clear, no overload). Existing `metadata()`
callers are unchanged.

```rust
client.metadata().await?;                       // all topics
client.metadata_topics(vec!["events".into()]).await?;
client.metadata_topics(Vec::new()).await?;      // same as metadata()
```

## Semantics

- Empty `topics` = all topics (same as today).
- Named list is encoded as `Request::Metadata { topics }` (`u32` count
  + strings). Broker already filters on that field.
- Transient 6 / 7 / 15 / 16 and `Error::Io` retry via
  `metadata_list_members_round_trip` (v0.96; default `max_retries=0`).
- Error 2 / 9 / 10 / 11 / 13 / 14 and protocol are not retried.
- Native Metadata still has no top-level `error_code`.
- Not Kafka Metadata versions / `allow_auto_topic_creation` / topic
  ids.

## Tests

Tiny protocol stub that decodes inbound `Request::Metadata` and
records the `topics` list:

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `metadata()` | inbound topics is empty (all) |
| `metadata_topics(vec!["events"])` | inbound topics is `["events"]` |
| `metadata_topics(vec![])` | same empty list as `metadata()` |
| `metadata()` with queued Timeout then ok | still retries (v0.96 inherit) |

Existing `v96_metadata_list_members_retry.rs` must still pass.

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `metadata` wraps `metadata_topics` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v114_metadata_topics.rs` | tokio TCP stub |
| `docs/V114_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** Metadata versions / `allow_auto_topic_creation` /
  topic ids.
- **Empty still means all** (native opcode 4).
- **No Kafka API keys / opcodes / Phase 155.**
- Language clients are unchanged (already have the field).
- DeleteRecords (v0.111) and ListOffsets 13 (v0.113) are unchanged.
- Retry policy is unchanged (v0.96 inherit).
- Native Metadata still has no top-level `error_code`.
- Broker / protocol are frozen.

## Merge notes

Sibling slices **v0.111 / v0.113** also edit `client.rs`. When
merging:

- **Keep `metadata()` as a wrapper** around `metadata_topics`.
- Keep `metadata_topics` using `metadata_list_members_round_trip`.
- Do **not** wrap DeleteRecords (v0.111) or ListOffsets 13 (v0.113).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to `metadata`
  / new `metadata_topics`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
  this inherits
- [V95_SPEC.md](./V95_SPEC.md) — language Metadata / ListMembers retry
- [V77_SPEC.md](./V77_SPEC.md) — Metadata `controller_id` trailer
- [PHASE2_SPEC.md](./PHASE2_SPEC.md) — native Metadata `topics` field
