# v0.288 — Kafka DeleteShareGroupOffsets key 92 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DeleteShareGroupOffsets** (API key **92**,
version **0** only, always flexible). This is **not** KIP-932 share
offsets. Parse the official v0 body and reject with **42**
`INVALID_REQUEST` (`not KIP-932 share offsets`). Do **not** persist.
Do **not** wrap OffsetCommit **8** / DeleteGroups **42** /
OffsetDelete **47** / DescribeShareGroupOffsets **90**.

This is residual **v0.288**, not Phase 155. Official Apache Kafka
`DeleteShareGroupOffsetsRequest.json` uses apiKey **92**. Official
`validVersions` is **0** only. Official response has `throttleTimeMs`,
**top-level** ErrorCode/ErrorMessage, and **per-topic** `Responses[]`.

## Goals

1. Advertise `(ApiKey::DeleteShareGroupOffsets, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after DescribeShareGroupOffsets
   **90**, before UnregisterController **94**). Soft length assert
   `>= 85`. Do **not** change hard `== 84`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `DeleteShareGroupOffsetsRequest.json` v0. Parse GroupId
   for ACL; parse topic **names** to echo. Do **not** persist. Do
   **not** wrap OffsetCommit 8 / DeleteGroups 42 / OffsetDelete 47 /
   DescribeShareGroupOffsets 90.
4. Official response has **throttleTimeMs**, **top-level** ErrorCode
   + ErrorMessage, and per-topic `Responses[]` (TopicName, TopicId,
   ErrorCode, ErrorMessage). Throttle **0**. Top-level and per-topic
   **42** `"not KIP-932 share offsets"`. Echo parsed topic names with
   TopicId **zero** (request has no TopicId). Unparseable body →
   throttle **0** + top-level **42** + empty `Responses[]` (not
   success).
5. Controller is **not** required (group-local reject).
6. ACL: Group **ALTER** on parsed groupId. Disabled ACLs allow the
   **42** path. Denied → top-level **30**, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-offset store | Reject only |
| Wrap OffsetCommit 8 / DeleteGroups 42 / OffsetDelete 47 / DescribeShareGroupOffsets 90 | Clients keep using 8 / 42 / 47 / 90 |
| AlterShareGroupOffsets 91 | Sibling leftover |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 84` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`DeleteShareGroupOffsetsRequest.json`;
`flexibleVersions: 0+`):

```
GroupId compact string
Topics[] {
  TopicName compact string
  tagged
}
tagged
```

Request topics are **topic names only** (no partitions, no TopicId).
Volant echoes names and does not invent offsets.

Official response v0 (`DeleteShareGroupOffsetsResponse.json`;
**has** `throttleTimeMs` **and** top-level ErrorCode/ErrorMessage
**and** per-topic `Responses[]`):

```
throttleTimeMs i32 = 0
ErrorCode i16 = 42               // or 30 if Group ALTER denied
ErrorMessage compact nullable string
                                 // "not KIP-932 share offsets";
                                 // null on ACL deny
Responses[] {
  TopicName compact string       // echo
  TopicId uuid = zeros           // request has no TopicId
  ErrorCode i16 = 42             // or 30 if Group ALTER denied
  ErrorMessage compact nullable string
  tagged
}
tagged
```

Official `validVersions` is **0** only; Volant advertises **0** only
as flexible (matches official).

## Semantics

```
DeleteShareGroupOffsets v0
  │
  ├─ Group ALTER fail → top-level 30, errorMessage null,
  │                      echo topics with 30 / null; no persist
  ├─ Unparseable body → throttle 0 + top-level 42 + empty
  │                      Responses[] (not success)
  ├─ Controller not required
  │
  └─ else → top-level + per-topic 42 INVALID_REQUEST
            (not KIP-932 share offsets)
            nothing persisted
            OffsetCommit / DeleteGroups / OffsetDelete / 90 unchanged
```

- Response throttle is always 0.
- Does **not** wrap OffsetCommit **8**, DeleteGroups **42**,
  OffsetDelete **47**, or DescribeShareGroupOffsets **90**.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official). v1 is not
  advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v288_delete_share_group_offsets -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **92** min=max=0; key **90** still listed; `SUPPORTED_APIS.len() >= 85` |
| v0 delete one topic | throttle **0**, top-level **42**, echo topic + zero TopicId + per-topic **42**, offsets unchanged |
| v1 | **35** |
| ACL deny | top-level **30**, errorMessage **null** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 92 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + top-level/per-topic reject) |
| `crates/volant-broker/tests/v288_delete_share_group_offsets.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 92 v0 reject |
| `docs/V288_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** Official validVersions is **0** only.
- Does **not** wrap OffsetCommit / DeleteGroups / OffsetDelete / 90.
- Official response has throttle, top-level error, **and** per-topic
  `Responses[]`. Request topics are names only.
- Official Kafka ACL is Group **DELETE**; Volant uses Group **ALTER**
  (slice).
- `group.rs` hard `== 84` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V284_SPEC.md](./V284_SPEC.md) — DescribeShareGroupOffsets 90 reject
- [V282_SPEC.md](./V282_SPEC.md) — DeleteShareGroupState 86 reject
- OffsetDelete key **47** — classic consumer offset delete; not share offsets
