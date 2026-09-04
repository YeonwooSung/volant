# v0.270 — Kafka Vote key 52 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **Vote** (API key **52**, version **0**
only, always flexible). Volant is **not** a KRaft controller. Parse
the official v0 body and reject with top-level **42**
`INVALID_REQUEST` (`not KRaft vote`). Empty `topics[]`. Do **not**
wrap openraft RequestVote / native vote. Do **not** grant any vote.

This is residual **v0.270**, not Phase 155. Official Apache Kafka
`VoteRequest.json` uses apiKey **52**. DescribeQuorum stays **55**.
BeginQuorumEpoch is official **53**, EndQuorumEpoch **54** (siblings
— not advertised). Official field layout is used.

## Goals

1. Advertise `(ApiKey::Vote, 0, 0)` in `SUPPORTED_APIS` (numeric
   order after AlterUserScramCredentials **51**, before DescribeQuorum
   **55**). Soft length assert `>= 70`. Do **not** change hard `== 69`
   asserts in `group.rs`, `v206_*`, `v225_*`, `v228_*`, `v233_*`.
2. Always flexible (official `flexibleVersions: 0+`). Dispatch v0 only.
   v1+ → **35**.
3. Official VoteRequest v0 (confirmed `VoteRequest.json`): `clusterId`
   compact nullable string, `topics[]` compact with `partitionIndex`,
   `replicaEpoch`, `replicaId`, `lastOffsetEpoch`, `lastOffset`. Parse
   enough to consume the body without panicking. Discard every field.
   Do **not** persist.
4. Response matches official `VoteResponse.json` v0. **There is no
   `throttleTimeMs`**: `ErrorCode` i16 = **42** (or **31** if Cluster
   ALTER denied), empty `topics[]`, tagged. NodeEndpoints is v1+ tag
   0; unused.
5. Controller is **not** required (local reject).
6. ACL: Cluster **ALTER**. Disabled ACLs allow. Denied → top-level
   **31**, empty topics.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft vote / PreVote / voter keys / DirectoryId | Not a KRaft controller |
| Wrap openraft RequestVote or native vote | Different protocol; no vote granted |
| BeginQuorumEpoch 53 / EndQuorumEpoch 54 / AddRaftVoter / RemoveRaftVoter / UpdateRaftVoter / UnregisterController | Sibling leftovers |
| Official v1 VoterId / ReplicaDirectoryId / VoterDirectoryId | Advertised max is 0 |
| Official v2 PreVote | Advertised max is 0 |
| join-set wait, unclean election, live reassignment, txn-topic default-on, Kafka Fetch group tags | Sibling leftovers |
| `group.rs` hard `== 69` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`VoteRequest.json`; `flexibleVersions: 0+`):

```
ClusterId compact nullable string
Topics[] {
  TopicName compact string
  Partitions[] {
    PartitionIndex i32
    ReplicaEpoch i32
    ReplicaId i32
    LastOffsetEpoch i32
    LastOffset i64
    tagged
  }
  tagged
}
tagged
```

VoterId is v1+ — not in the v0 body. ReplicaDirectoryId /
VoterDirectoryId are v1+; PreVote is v2+ — out of advertised range.

Parse loosely. If a field is missing, stop parsing that level and
still return 42. Never panic.

Official response (`VoteResponse.json` v0; **no throttleTimeMs**;
NodeEndpoints is v1+ tag 0, unused because topics is empty):

```
ErrorCode i16 = 42            // or 31 if Cluster ALTER denied
Topics[] compact = empty
tagged
```

## Semantics

```
Vote v0
  │
  ├─ Cluster ALTER fail → 31, empty topics
  ├─ Controller not required
  │
  └─ else → error 42 INVALID_REQUEST
            (not KRaft vote)
            empty topics[]
            no throttleTimeMs
            nothing persisted
            membership / openraft state unchanged
            openraft RequestVote is not called
            no vote granted
```

- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official).
- Official `validVersions` is 0–2; Volant advertises 0 only.
- Official response has no `throttleTimeMs`.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v270_vote -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **52** min=0 max=0; key **55** still listed; `SUPPORTED_APIS.len() >= 70` |
| v0 vote (ClusterId + one topic/partition) | header v1 tags, error **42**, empty topics; no throttle field; membership / openraft state unchanged |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 52 + `from_i16` + `SUPPORTED_APIS` + soft test + crate-doc |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | parse + reject 42; ACL deny 31; no persist |
| `crates/volant-broker/tests/v270_vote.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 52 v0 reject |
| `docs/V270_SPEC.md` | This spec |

## Honesty leftovers

- **Not** a KRaft controller. No vote granted, no PreVote, no voter
  keys.
- Does **not** wrap openraft RequestVote.
- Official apiKey is **52**. Official first flex is **0+**; Volant v0
  is flexible (matches official).
- Official validVersions 0–2; Volant advertises 0 only.
- Official response has no throttleTimeMs.
- `group.rs` hard `== 69` intentionally untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V267_SPEC.md](./V267_SPEC.md) — FetchSnapshot reject
- [V266_SPEC.md](./V266_SPEC.md) — Envelope reject (also no throttle)
- [V245_SPEC.md](./V245_SPEC.md) — DescribeQuorum wrap of openraft
