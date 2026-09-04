# v0.289 — Kafka BeginQuorumEpoch key 53 v1 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **BeginQuorumEpoch** (API key **53**,
version **1** only, always flexible). Volant is **not** a KRaft
quorum. Parse the official v1 body and reject with top-level **42**
`INVALID_REQUEST` (`not KRaft quorum epoch`). Empty `topics[]`. Do
**not** wrap openraft RequestVote / Vote 52 / native vote. Do **not**
persist voter set / DirectoryId / quorum epoch.

This is residual **v0.289**, not Phase 155. Official Apache Kafka
`BeginQuorumEpochRequest.json` uses apiKey **53**. Vote stays **52**.
DescribeQuorum stays **55**. EndQuorumEpoch is official **54**
(sibling — not advertised). Official field layout is used.

## Goals

1. Advertise `(ApiKey::BeginQuorumEpoch, 1, 1)` in `SUPPORTED_APIS`
   (numeric order after Vote **52**, before DescribeQuorum **55**).
   Soft length assert `>= 85`. Do **not** change hard `== 84` asserts
   in `group.rs`, `v206_*`, `v225_*`, `v228_*`, `v233_*`.
2. Always-flex header (advertised v1 is flex). Dispatch **v1** only.
   Official v0 is **classic** (`flexibleVersions: 1+`) — do **not**
   advertise v0 as flex. v0 and v2+ → **35**.
3. Official BeginQuorumEpochRequest v1 (confirmed
   `BeginQuorumEpochRequest.json`): `clusterId` compact nullable
   string, `voterId` i32 (v1+), `topics[]` compact with
   `partitionIndex`, `voterDirectoryId` uuid (v1+), `leaderId`,
   `leaderEpoch`, then `leaderEndpoints[]` (v1+). Parse enough to
   consume the body without panicking. Discard every field. Do
   **not** persist.
4. Response matches official `BeginQuorumEpochResponse.json` v1.
   **There is no `throttleTimeMs`**: `ErrorCode` i16 = **42** (or
   **31** if Cluster ALTER denied), empty `topics[]`, tagged.
   NodeEndpoints is v1+ tag 0; unused.
5. Controller is **not** required (local reject).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → top-level
   **31**, empty topics.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft quorum epoch / voter set / DirectoryId | Not a KRaft controller |
| Wrap openraft RequestVote or Vote 52 | Different protocol; no epoch started |
| EndQuorumEpoch 54 | Sibling leftover |
| Advertise official v0 (classic) | Honesty: official v0 is not flex |
| Official v2+ | Out of official range (validVersions 0–1) |
| join-set wait, unclean election, live reassignment, txn-topic default-on, Kafka Fetch group tags | Sibling leftovers |
| `group.rs` hard `== 84` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v1 always flexible)

Official request v1 (`BeginQuorumEpochRequest.json`; official
`validVersions: "0-1"`, `flexibleVersions: "1+"`):

```
ClusterId compact nullable string
VoterId i32
Topics[] {
  TopicName compact string
  Partitions[] {
    PartitionIndex i32
    VoterDirectoryId uuid
    LeaderId i32
    LeaderEpoch i32
    tagged
  }
  tagged
}
LeaderEndpoints[] {
  Name compact string
  Host compact string
  Port u16
  tagged
}
tagged
```

Official v0 is **classic** (not flexible) and is **not advertised**.
VoterId / VoterDirectoryId / LeaderEndpoints are v1+ only.

Parse loosely. If a field is missing, stop parsing that level and
still return 42. Never panic.

Official response (`BeginQuorumEpochResponse.json` v1; **no
throttleTimeMs**; NodeEndpoints is v1+ tag 0, unused because topics
is empty; no errorMessage):

```
ErrorCode i16 = 42            // or 31 if Cluster ALTER denied
Topics[] compact = empty
tagged
```

## Semantics

```
BeginQuorumEpoch v1
  │
  ├─ Cluster ALTER fail → 31, empty topics
  ├─ Controller not required
  │
  └─ else → error 42 INVALID_REQUEST
            (not KRaft quorum epoch)
            empty topics[]
            no throttleTimeMs
            nothing persisted
            membership / openraft state unchanged
            openraft RequestVote is not called
            Vote 52 is not called
            no quorum epoch started
```

- Official Apache Kafka first flexible version is **1+**; official v0
  is classic. Volant advertises **v1 only** as flexible (matches
  advertised version).
- Official `validVersions` is 0–1; Volant advertises 1 only.
- Official response has no `throttleTimeMs` (same as Vote 52).

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v289_begin_quorum_epoch -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **53** min=1 max=1; keys **52** and **55** still listed; `SUPPORTED_APIS.len() >= 85` |
| v1 begin (ClusterId + VoterId + one topic/partition + endpoints) | header v1 tags, error **42**, empty topics; no throttle field; membership / openraft state unchanged |
| v0 | **35** |
| v2 | **35** |
| ACL deny | **31**, empty topics |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 53 + `from_i16` + `SUPPORTED_APIS` + soft test + crate-doc |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v1 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v289_begin_quorum_epoch.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 53 v1 reject |
| `docs/V289_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft quorum. No voter set, no DirectoryId store, no
  quorum epoch started.
- Does **not** wrap openraft RequestVote or Vote 52.
- Official apiKey is **53**. Official first flex is **1+**; official
  v0 is classic — Volant does not advertise v0.
- Official validVersions 0–1; Volant advertises 1 only.
- Official response has no throttleTimeMs.
- `group.rs` hard `== 84` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V270_SPEC.md](./V270_SPEC.md) — Vote 52 reject
- [V245_SPEC.md](./V245_SPEC.md) — DescribeQuorum wrap of openraft
