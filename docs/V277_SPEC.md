# v0.277 — Kafka ShareFetch key 78 v1 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **ShareFetch** (API key **78**, version **1**
only, always flexible). This is **not** KIP-932. Parse the official v1
body and reject top-level **42** `INVALID_REQUEST`
(`not KIP-932 share fetch`). Empty `Responses[]` and
`NodeEndpoints[]`. Do **not** wrap Kafka Fetch **1** or native Fetch.
Do **not** acquire records or create a share session.

This is residual **v0.277**, not Phase 155. Official Apache Kafka
`ShareFetchRequest.json` uses apiKey **78**. Fetch is already **1**.

## Goals

1. Advertise `(ApiKey::ShareFetch, 1, 1)` in `SUPPORTED_APIS` (numeric
   order after DescribeTopicPartitions **75**, before AddRaftVoter
   **80**). Soft length assert `>= 75`. Do **not** change hard `== 74`
   asserts in `group.rs`.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch **v1
   only**. v0 and v2+ → **35**.
3. Official `ShareFetchRequest.json` v1. Parse enough to consume the
   body without panicking. Discard every field. Do **not** fetch
   records, open a share session, or wrap Fetch **1**.
4. Response matches official `ShareFetchResponse.json` v1: throttle
   **0**, error **42**, errorMessage `"not KIP-932 share fetch"`,
   `acquisitionLockTimeoutMs` **0**, empty `Responses[]` and
   `NodeEndpoints[]`.
5. Controller is **not** required (group-local reject).
6. ACL: Group **READ** on the parsed `groupId` (nullable → treat None
   as empty). Disabled ACLs allow the **42** path. Denied → **30**,
   errorMessage **null**.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share sessions / acquired records | Reject only |
| Wrap Kafka Fetch 1 / native Fetch | Clients keep using Fetch **1** |
| ShareGroupHeartbeat 76 / ShareGroupDescribe 77 / ShareAcknowledge 79 / InitializeShareGroupState 83 | Sibling leftovers |
| Official v2 ShareAcquireMode / Renew | Advertise v1 only |
| Official v0 (EA, removed in Kafka 4.1) | Not advertised |
| `group.rs` hard `== 74` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v1 always flexible)

Official `flexibleVersions` is `0+`. Request
(`ShareFetchRequest.json` v1):

```
GroupId compact nullable string
MemberId compact nullable string
ShareSessionEpoch i32
MaxWaitMs i32
MinBytes i32
MaxBytes i32
MaxRecords i32            // v1+
BatchSize i32             // v1+
Topics[] {
  TopicId uuid
  Partitions[] {
    PartitionIndex i32
    AcknowledgementBatches[] { FirstOffset i64, LastOffset i64, AcknowledgeTypes[] i8, tagged }
    tagged
  }
  tagged
}
ForgottenTopicsData[] { TopicId uuid, Partitions[] i32, tagged }
tagged
```

`PartitionMaxBytes` is official versions **"0" only** — **not** in v1.
`ShareAcquireMode` / `IsRenewAck` are v2+ — do not parse as v1.

Response (`ShareFetchResponse.json` v1):

```
throttleTimeMs i32 = 0
errorCode i16 = 42            // or 30 if Group READ denied
errorMessage compact nullable string = "not KIP-932 share fetch"  // null on ACL deny
acquisitionLockTimeoutMs i32 = 0    // v1+
Responses[] compact empty
NodeEndpoints[] compact empty
tagged
```

Official supported errors include `INVALID_REQUEST` (**42**) and
`GROUP_AUTHORIZATION_FAILED` (**30**). Do **not** invent partition
records or node endpoints.

## Semantics

```
ShareFetch v1
  │
  ├─ Group READ fail → 30, errorMessage null, no records written
  ├─ Controller not required
  │
  └─ else → throttle 0, error 42 INVALID_REQUEST
            (not KIP-932 share fetch)
            empty Responses[] / NodeEndpoints[]
            acquisitionLockTimeoutMs 0
            no share session; log unchanged
```

- Response throttle is always 0.
- Does **not** wrap Kafka Fetch **1**. Clients must keep using **1**.
- Official Apache Kafka advertises 1–2 today (v0 EA removed in 4.1;
  v2 = ShareAcquireMode / Renew). Volant advertises **1** only.
- Official first flex is **0+**; Volant v1 is flexible (matches official).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v277_share_fetch -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **78** min=1 max=1; key **1** still listed; `SUPPORTED_APIS.len() >= 75` |
| v1 share fetch (any group/member/epoch) | throttle **0**, error **42**, empty responses; no records written |
| v0 | **35** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 78 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v1 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + reject) |
| `crates/volant-broker/tests/v277_share_fetch.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 78 v1 reject |
| `docs/V277_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** No share session, no acquired records.
- Does **not** wrap Fetch 1. Clients must keep using Fetch **1**.
- Official apiKey is **78**. Official first flex is **0+**; Volant v1
  is flexible (matches official).
- Official validVersions 1–2; Volant advertises **1** only. Official
  v0 was EA and removed in Kafka 4.1.
- `group.rs` hard `== 74` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- Kafka Fetch key **1** — record fetch clients must use
