# v0.278 — Kafka ShareAcknowledge key 79 v1 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ShareAcknowledge** (API key **79**,
version **1** only, always flexible). Volant has no share-group
acknowledge path — **not** KIP-932. Parse the official v1 body and
reject **42** `INVALID_REQUEST` (`not KIP-932 share acknowledge`).
Do **not** wrap OffsetCommit / Fetch. Do **not** mutate offsets or
record state. Empty `Responses[]` and `NodeEndpoints[]`.

This is residual **v0.278**, not Phase 155. Official Apache Kafka
`ShareAcknowledgeRequest.json` uses apiKey **79**. Official
`validVersions` is **1–2** (v0 was EA and removed in Kafka 4.1).
ShareGroupHeartbeat **76** / ShareGroupDescribe **77** / ShareFetch
**78** / InitializeShareGroupState **83** are siblings and are not
advertised.

## Goals

1. Advertise `(ApiKey::ShareAcknowledge, 1, 1)` in `SUPPORTED_APIS`
   (numeric order after DescribeTopicPartitions **75**, before
   AddRaftVoter **80**). Soft length assert `>= 75`. Do **not**
   change hard `== 74` asserts.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v1 only**. v0 and v2+ → **35**.
3. Official `ShareAcknowledgeRequest.json` v1. Parse enough to
   consume the body without panicking. Discard every field. Do **not**
   persist. IsRenewAck is v2+ and is not parsed.
4. Response matches official `ShareAcknowledgeResponse.json` v1:
   throttle **0**, error **42**, errorMessage
   `"not KIP-932 share acknowledge"`, empty `Responses[]`, empty
   `NodeEndpoints[]`. AcquisitionLockTimeoutMs is v2+ and is **not**
   written.
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId`. If `groupId` cannot
   be parsed, treat as empty and still reject **42** (or **30** if
   ACLs on and empty-id denied). Disabled ACLs allow the **42**
   path. Denied → **30**, errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 acknowledge / share session | Classic groups + offsets only |
| Wrap OffsetCommit / Fetch | Clients keep using **8** / **1** |
| Mutate offsets or record state | Reject only |
| ShareGroupHeartbeat 76 / ShareGroupDescribe 77 / ShareFetch 78 / InitializeShareGroupState 83 | Sibling leftovers |
| Official v2 IsRenewAck / AcquisitionLockTimeoutMs | Advertise v1 only |
| `group.rs` hard `== 74` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v1 always flexible)

Official `flexibleVersions` is `0+`. Request
(`ShareAcknowledgeRequest.json` v1):

```
GroupId compact nullable string
MemberId compact nullable string
ShareSessionEpoch i32
Topics[] {
  TopicId uuid
  Partitions[] {
    PartitionIndex i32
    AcknowledgementBatches[] {
      FirstOffset i64
      LastOffset i64
      AcknowledgeTypes[] i8
      tagged
    }
    tagged
  }
  tagged
}
tagged
```

IsRenewAck is v2+ — do not parse as v1.

Response (`ShareAcknowledgeResponse.json` v1; do not write v2
`AcquisitionLockTimeoutMs`):

```
ThrottleTimeMs i32                  // 0
ErrorCode i16                       // 42, or 30 if Group READ denied
ErrorMessage compact nullable string
                                    // "not KIP-932 share acknowledge";
                                    // null on ACL deny
Responses[]                         // compact empty
NodeEndpoints[]                     // compact empty
tagged
```

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent per-topic
responses or node endpoints.

## Semantics

```
ShareAcknowledge v1
  │
  ├─ Group READ fail → 30, errorMessage null, no offset mutation
  ├─ Controller not required
  │
  └─ else → 42 "not KIP-932 share acknowledge"
       empty Responses[], empty NodeEndpoints[];
       committed offsets unchanged
```

- Response throttle is always 0.
- Does **not** wrap OffsetCommit **8** or Fetch **1**.
- Official Apache Kafka advertises 1–2 today (v0 removed in 4.1;
  v2 = IsRenewAck / AcquisitionLockTimeoutMs). Volant advertises
  **1** only.
- Official first flex is **0+**; Volant v1 is flexible (matches
  official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v278_share_acknowledge -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **79** min=max=1; `SUPPORTED_APIS.len() >= 75` |
| v1 acknowledge | throttle **0**, error **42**, empty responses / endpoints; offsets unchanged |
| v0 | **35** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 79 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v1 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v278_share_acknowledge.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 79 v1 reject |
| `docs/V278_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** Official validVersions 1–2; Volant advertises
  **1** only. Official v0 removed in Kafka 4.1.
- Official first flex is 0+; Volant v1 is flexible.
- `group.rs` hard `== 74` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V269_SPEC.md](./V269_SPEC.md) — ConsumerGroupHeartbeat 68 reject
- OffsetCommit key **8** / Fetch key **1** — clients must keep using
