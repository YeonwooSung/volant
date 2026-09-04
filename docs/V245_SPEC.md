# v0.245 — Kafka DescribeQuorum key 55 v0–1

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeQuorum** (API key **55**, versions
**0–1**, always flexible — Kafka `flexibleVersions: 0+`). Wrap
openraft helpers already on Broker:

- `openraft_leader_id()`
- `openraft_term()`
- `openraft_voter_ids()`

This is residual **v0.245**. It is **not** full KRaft DescribeQuorum
(no `__cluster_metadata` topic, no per-replica lastCaughtUp). Do
**not** touch AllocateProducerIds, ACLs, SyncGroup apply,
AlterReplicaLogDirs, or `group.rs`.

## Goals

1. Advertise `(ApiKey::DescribeQuorum, 0, 1)` in `SUPPORTED_APIS`.
   Soft length assert `>= 50`.
2. Dispatch key 55 v0–1 (always flexible request header + compact
   body). Official Kafka v0–1: no throttle, no Nodes, no
   ReplicaDirectoryId (those are v2).
3. Flag off / raft not started: top-level **0**, empty topics
   (single-node / overlay-only).
4. Cluster configured + `!is_controller()` → top-level **41**.
5. Empty request topics + raft started → one synthetic cluster
   partition **0** (empty name) using `openraft_voter_ids()`.
6. `logEndOffset` / `highWatermark`: local LEO/HWM if the requested
   topic exists locally, else **0**. Do **not** invent
   `__cluster_metadata`.
7. `lastFetchTimestamp` / `lastCaughtUpTimestamp` (v1): **-1**.
8. ACL: Cluster **DESCRIBE**. Disabled ACLs allow.
9. v2+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| KRaft `__cluster_metadata` | Honest openraft wrap |
| Per-replica lastCaughtUp | Not available; always -1 |
| v2 Nodes / DirectoryId / ErrorMessage | Advertised max is 1 |
| AllocateProducerIds / ACLs / SyncGroup apply | Sibling leftovers |
| AlterReplicaLogDirs / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0–1 always flexible)

Official Kafka `DescribeQuorum` is flexible from v0. Official clients
accept this layout (no throttleTimeMs; Nodes/DirectoryId are v2).

Request:

```
topics[] {
  topicName compact string
  partitions[] {
    partitionIndex i32
    tagged
  }
  tagged
}
tagged
```

Response:

```
errorCode i16
topics[] {
  topicName compact string
  partitions[] {
    partitionIndex i32
    errorCode i16
    leaderId i32
    leaderEpoch i32
    highWatermark i64
    currentVoters[] {
      replicaId i32
      logEndOffset i64
      lastFetchTimestamp i64      // v1
      lastCaughtUpTimestamp i64   // v1
      tagged
    }
    observers[] { same as voters }
    tagged
  }
  tagged
}
tagged
```

## Semantics

```
DescribeQuorum v0–1
  │
  ├─ cluster + not controller → top-level 41, empty topics
  ├─ Cluster DESCRIBE fail → top-level 31, empty topics
  ├─ raft off / not started → top-level 0, empty topics
  │
  └─ raft started
        ├─ empty request topics → synthetic "" / partition 0
        └─ per requested topic/partition
              leaderId    = openraft_leader_id() or -1
              leaderEpoch = openraft_term()
              highWatermark / logEndOffset = local if topic exists else 0
              currentVoters = openraft_voter_ids()
              lastFetch / lastCaughtUp = -1
              observers empty
```

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v245_describe_quorum -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **55** min=0 max=1; `SUPPORTED_APIS.len() >= 50` |
| Single-node / raft off | error **0**, empty or synthetic, no crash |
| not controller (cluster) | **41** |
| v2 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 55 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0–1 + always-flex header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode + lib tests |
| `crates/volant-broker/tests/v245_describe_quorum.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 55 v0–1 |
| `docs/V245_SPEC.md` | This spec |

## Honesty leftovers

- **Not** KRaft DescribeQuorum. No `__cluster_metadata` log, no
  controller quorum LEO, no per-replica lastCaughtUp.
- Empty request topics report a synthetic cluster partition **0**
  with an **empty** topic name (documented). We do not materialize
  Kafka's `__cluster_metadata` topic.
- `logEndOffset` / `highWatermark` are **0** unless the requested
  topic exists locally.
- Kafka v2 (Nodes, ReplicaDirectoryId, ErrorMessage) is refused
  with **35**.
- `group.rs` `SUPPORTED_APIS.len()==49` assertion is intentionally
  untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V244_SPEC.md](./V244_SPEC.md) — previous Kafka admin wrap
