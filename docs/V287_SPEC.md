# v0.287 — Kafka AlterShareGroupOffsets key 91 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **AlterShareGroupOffsets** (API key **91**,
version **0** only, always flexible). This is **not** KIP-932 share
offsets. Parse the official v0 body and reject with **42**
`INVALID_REQUEST` (`not KIP-932 share offsets`). Do **not** persist.
Do **not** wrap OffsetCommit **8** / DescribeShareGroupOffsets **90**.

This is residual **v0.287**, not Phase 155. Official Apache Kafka
`AlterShareGroupOffsetsRequest.json` uses apiKey **91**. Official
`validVersions` is **0** only. Official response has `throttleTimeMs`,
**top-level** ErrorCode / ErrorMessage, **and** per-topic/per-partition
errors. TopicId is present on the response even though the request is
name-only. Closer to OffsetCommit **8** / ShareAcknowledge **79** than
InitializeShareGroupState **83**.

## Goals

1. Advertise `(ApiKey::AlterShareGroupOffsets, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after DescribeShareGroupOffsets
   **90**, before UnregisterController **94**). Soft length assert
   `>= 85`. Do **not** change hard `== 84`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `AlterShareGroupOffsetsRequest.json` v0. Parse GroupId
   for ACL; parse topic/partition names to echo. Do **not** persist
   StartOffset. Do **not** wrap OffsetCommit 8 / DescribeShareGroupOffsets 90.
4. Official response has **throttleTimeMs**, **top-level** ErrorCode /
   ErrorMessage, **and** per-topic/per-partition ErrorCode /
   ErrorMessage. Echo each requested topic/partition with **42**,
   errorMessage `"not KIP-932 share offsets"`. TopicId is the zero
   UUID (request has TopicName only). Unparseable body → throttle **0**
   + top-level **42** + empty `Responses[]` (no top-level 0 success).
5. Controller is **not** required (group-local reject).
6. ACL: Group **ALTER** on the parsed groupId (mutate path;
   Describe **90** is DESCRIBE). Disabled ACLs allow the **42** path.
   Denied → top-level **30**, errorMessage **null** (echoed partitions
   also **30** / null).

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-offset store | Reject only |
| Wrap OffsetCommit 8 / DescribeShareGroupOffsets 90 | Clients keep using 8 / 90 |
| DeleteShareGroupOffsets 92 | Sibling leftover |
| Advertise v1 | Official validVersions is 0 only |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 84` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`AlterShareGroupOffsetsRequest.json`;
`flexibleVersions: 0+`):

```
GroupId compact string
Topics[] {
  TopicName compact string
  Partitions[] {
    PartitionIndex i32
    StartOffset i64
    tagged
  }
  tagged
}
tagged
```

StartOffset is discarded. Volant does not invent share offsets.

Official response v0 (`AlterShareGroupOffsetsResponse.json`;
**has** `throttleTimeMs`; **has** top-level ErrorCode / ErrorMessage
**and** per-topic/per-partition errors; TopicId is on the response
even though the request is name-only):

```
throttleTimeMs i32 = 0
ErrorCode i16 = 42                 // or 30 if Group ALTER denied
ErrorMessage compact nullable string
                                   // "not KIP-932 share offsets";
                                   // null on ACL deny
Responses[] {
  TopicName compact string         // echo
  TopicId uuid                     // zeros (request is name-only)
  Partitions[] {
    PartitionIndex i32             // echo
    ErrorCode i16 = 42             // or 30 if denied
    ErrorMessage compact nullable string
    tagged
  }
  tagged
}
tagged
```

Official `validVersions` is **0** only; Volant advertises **0** only.

## Semantics

```
AlterShareGroupOffsets v0
  │
  ├─ Group ALTER fail → top-level 30, errorMessage null,
  │                     echo topics/partitions 30 / null; no persist
  ├─ Unparseable body → throttle 0 + top-level 42 + empty Responses[]
  ├─ Controller not required
  │
  └─ else → top-level 42 + echo topic/partition 42 INVALID_REQUEST
            (not KIP-932 share offsets)
            nothing persisted
            OffsetCommit / offsets / share state unchanged
```

- Response throttle is always 0.
- Does **not** wrap OffsetCommit **8** or DescribeShareGroupOffsets **90**.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official). v1 is not
  advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v287_alter_share_group_offsets -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **91** min=max=0; key **90** still listed; `SUPPORTED_APIS.len() >= 85` |
| v0 alter one topic/partition | throttle **0**, top-level **42**, echo topic/partition **42**, OffsetCommit/offsets unchanged |
| v1 | **35** |
| ACL deny | top-level **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 91 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v287_alter_share_group_offsets.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 91 v0 reject |
| `docs/V287_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** Official validVersions is **0** only. Volant
  advertises **0** only.
- Does **not** wrap OffsetCommit **8** / DescribeShareGroupOffsets **90**.
- Official response has throttle, top-level error, **and**
  per-partition error; reject is **42** at both levels after echoing
  parsed topic/partition names. TopicId is zeros.
- `group.rs` hard `== 84` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V284_SPEC.md](./V284_SPEC.md) — DescribeShareGroupOffsets 90 reject
- [V278_SPEC.md](./V278_SPEC.md) — ShareAcknowledge 79 reject
- OffsetCommit key **8** — classic consumer offsets; not share offsets
