# Phase history index

Ship records for **phases 0–79**. Binding core contracts are
**[PHASE1_SPEC](../PHASE1_SPEC.md)–[PHASE6_SPEC](../PHASE6_SPEC.md)**. Living
docs for day-to-day reading: [ops](../ops.md), [consistency](../consistency.md),
[tuning](../tuning.md), [KAFKA_COMPAT](../KAFKA_COMPAT.md),
[features](../features.md), [WHITEPAPER](../WHITEPAPER.md). Individual
`PHASE*_SPEC.md` files remain for deep dives.

---

## Core (0–6)

| Phase | Status | One-line goal | Spec |
|------:|:------:|---------------|------|
| 0 | ✅ | Scaffold: Cargo workspace, core types, in-memory broker, server/CLI binaries | — |
| 1 | ✅ | Durable append-only segment log with crash recovery and retention | [PHASE1_SPEC.md](../PHASE1_SPEC.md) |
| 2 | ✅ | Framed TCP protocol, multi-partition broker, async client + CLI | [PHASE2_SPEC.md](../PHASE2_SPEC.md) |
| 3 | ✅ | Consumer groups, heartbeats, durable offsets, range assignor | [PHASE3_SPEC.md](../PHASE3_SPEC.md) |
| 4 | ✅ | Lightweight stream operators (map/filter/window) without a heavy runtime | [PHASE4_SPEC.md](../PHASE4_SPEC.md) |
| 5 | ✅ | DMA-friendly I/O: io_uring, O_DIRECT, batch coalesce, affinity, tuning | [PHASE5_SPEC.md](../PHASE5_SPEC.md) |
| 6 | ✅ | Clustering & ISR replication (static membership, HWM, acks=all) | [PHASE6_SPEC.md](../PHASE6_SPEC.md) |

---

## Ops (7–9)

| Phase | Status | One-line goal | Spec |
|------:|:------:|---------------|------|
| 7 | ✅ | Metrics, JSON logs, shared-token auth, optional TLS, packaging | [PHASE7_SPEC.md](../PHASE7_SPEC.md) |
| 8 | ✅ | Client leader redirect, client TLS, CLI auth token, Helm, rolling restart | [PHASE8_SPEC.md](../PHASE8_SPEC.md) |
| 9 | ✅ | TLS hardening, inter-broker TLS, multi-node Helm, fuzz scaffold | [PHASE9_SPEC.md](../PHASE9_SPEC.md) |

---

## Native features (10–22)

| Phase | Status | One-line goal | Spec |
|------:|:------:|---------------|------|
| 10 | ✅ | Idempotent produce, client retries, consumer lag metrics + CLI | [PHASE10_SPEC.md](../PHASE10_SPEC.md) |
| 11 | ✅ | Sticky assignor, durable producer PID state, group describe | [PHASE11_SPEC.md](../PHASE11_SPEC.md) |
| 12 | ✅ | ListGroups / DeleteOffsets, static membership via group_instance_id | [PHASE12_SPEC.md](../PHASE12_SPEC.md) |
| 13 | ✅ | Per-topic configs (retention/segment), describe/config CLI, background retention | [PHASE13_SPEC.md](../PHASE13_SPEC.md) |
| 14 | ✅ | Durable topic catalog across restart; DeleteRecords truncate-by-offset | [PHASE14_SPEC.md](../PHASE14_SPEC.md) |
| 15 | ✅ | CreatePartitions + ListOffsets (add-partitions / topic offsets) | [PHASE15_SPEC.md](../PHASE15_SPEC.md) |
| 16 | ✅ | Log compaction (`cleanup.policy=compact`) on sealed segments | [PHASE16_SPEC.md](../PHASE16_SPEC.md) |
| 17 | ✅ | Cooperative rebalance — JoinGroup revoked list; keep sticky positions | [PHASE17_SPEC.md](../PHASE17_SPEC.md) |
| 18 | ✅ | Transactions MVP — Begin/EndTxn, fencing, multi-partition atomic commit | [PHASE18_SPEC.md](../PHASE18_SPEC.md) |
| 19 | ✅ | mTLS identity — client CA/allowlist; CN principal without shared token | [PHASE19_SPEC.md](../PHASE19_SPEC.md) |
| 20 | ✅ | Principal ACLs (allow/deny on topic/group/cluster) + ACL CLI | [PHASE20_SPEC.md](../PHASE20_SPEC.md) |
| 21 | ✅ | Durable ACLs under data_dir + metrics Bearer auth | [PHASE21_SPEC.md](../PHASE21_SPEC.md) |
| 22 | ✅ | SCRAM-SHA-256 auth, durable users, coexists with token + mTLS | [PHASE22_SPEC.md](../PHASE22_SPEC.md) |

---

## Kafka classic (23–50)

| Phase | Status | One-line goal | Spec |
|------:|:------:|---------------|------|
| 23 | ✅ | Kafka wire shim MVP — ApiVersions/Metadata/Produce/Fetch (MessageSet) | [PHASE23_SPEC.md](../PHASE23_SPEC.md) |
| 24 | ✅ | Kafka RecordBatch (magic 2) on Produce/Fetch; auto-detect format | [PHASE24_SPEC.md](../PHASE24_SPEC.md) |
| 25 | ✅ | Kafka admin — CreateTopics / DeleteTopics / ListOffsets | [PHASE25_SPEC.md](../PHASE25_SPEC.md) |
| 26 | ✅ | Kafka consumer groups — FindCoordinator, Join/Sync/Heartbeat/Leave, offsets | [PHASE26_SPEC.md](../PHASE26_SPEC.md) |
| 27 | ✅ | Kafka ops — List/Describe/DeleteGroups, CreatePartitions, configs | [PHASE27_SPEC.md](../PHASE27_SPEC.md) |
| 28 | ✅ | Kafka RecordBatch compression on Produce (gzip/snappy/lz4/zstd) | [PHASE28_SPEC.md](../PHASE28_SPEC.md) |
| 29 | ✅ | Kafka InitProducerId + idempotent Produce (PID/epoch/sequence) | [PHASE29_SPEC.md](../PHASE29_SPEC.md) |
| 30 | ✅ | Kafka SASL — PLAIN + SCRAM-SHA-256 against Volant SCRAM store | [PHASE30_SPEC.md](../PHASE30_SPEC.md) |
| 31 | ✅ | Kafka transactions on shim — AddPartitions/EndTxn/TxnOffsetCommit | [PHASE31_SPEC.md](../PHASE31_SPEC.md) |
| 32 | ✅ | Kafka compressed Fetch v4 RecordBatches (default lz4) | [PHASE32_SPEC.md](../PHASE32_SPEC.md) |
| 33 | ✅ | Kafka MessageSet compression on Produce + Fetch v0–3 | [PHASE33_SPEC.md](../PHASE33_SPEC.md) |
| 34 | ✅ | SCRAM-SHA-512 on Kafka shim; dual SHA-256/512 credentials | [PHASE34_SPEC.md](../PHASE34_SPEC.md) |
| 35 | ✅ | Kafka DeleteRecords + ACL admin (Describe/Create/DeleteAcls) | [PHASE35_SPEC.md](../PHASE35_SPEC.md) |
| 36 | ✅ | Kafka OffsetDelete + Fetch isolation honesty (LSO ≡ HWM) | [PHASE36_SPEC.md](../PHASE36_SPEC.md) |
| 37 | ✅ | Kafka IncrementalAlterConfigs SET/DELETE on topic configs | [PHASE37_SPEC.md](../PHASE37_SPEC.md) |
| 38 | ✅ | Kafka Metadata classic v0–8 (cluster_id, rack, authorized-ops) | [PHASE38_SPEC.md](../PHASE38_SPEC.md) |
| 39 | ✅ | Kafka OffsetForLeaderEpoch classic v0–3 | [PHASE39_SPEC.md](../PHASE39_SPEC.md) |
| 40 | ✅ | Kafka ListOffsets classic v0–5 (isolation, throttle, epoch fence) | [PHASE40_SPEC.md](../PHASE40_SPEC.md) |
| 41 | ✅ | Kafka OffsetFetch classic v0–5 (null topics, throttle, top-level error) | [PHASE41_SPEC.md](../PHASE41_SPEC.md) |
| 42 | ✅ | Kafka group classic static membership (Join/Heartbeat/Sync/Leave) | [PHASE42_SPEC.md](../PHASE42_SPEC.md) |
| 43 | ✅ | Kafka group admin classic — Describe/List/DeleteGroups version bumps | [PHASE43_SPEC.md](../PHASE43_SPEC.md) |
| 44 | ✅ | Kafka OffsetCommit classic 0–7 + FindCoordinator 0–2 | [PHASE44_SPEC.md](../PHASE44_SPEC.md) |
| 45 | ✅ | Kafka topic admin classic — Create/DeleteTopics, CreatePartitions | [PHASE45_SPEC.md](../PHASE45_SPEC.md) |
| 46 | ✅ | Kafka DescribeConfigs 0–3 + AlterConfigs 0–1 | [PHASE46_SPEC.md](../PHASE46_SPEC.md) |
| 47 | ✅ | Kafka transaction APIs classic 0–2 | [PHASE47_SPEC.md](../PHASE47_SPEC.md) |
| 48 | ✅ | Kafka Produce classic 0–8 (log_start_offset, record_errors framing) | [PHASE48_SPEC.md](../PHASE48_SPEC.md) |
| 49 | ✅ | Kafka Fetch classic 0–11 (session, epoch fence, preferred replica) | [PHASE49_SPEC.md](../PHASE49_SPEC.md) |
| 50 | ✅ | Kafka ApiVersions classic 0–2 (trailing throttle) | [PHASE50_SPEC.md](../PHASE50_SPEC.md) |

---

## Kafka flexible/modern (51–79)

| Phase | Status | One-line goal | Spec |
|------:|:------:|---------------|------|
| 51 | ✅ | Flexible wire foundation (KIP-482) + ApiVersions v3 | [PHASE51_SPEC.md](../PHASE51_SPEC.md) |
| 52 | ✅ | Flexible Metadata v9 + FindCoordinator v3–4 (batch keys) | [PHASE52_SPEC.md](../PHASE52_SPEC.md) |
| 53 | ✅ | Flexible Produce v9 — compact records/topics + header v1 | [PHASE53_SPEC.md](../PHASE53_SPEC.md) |
| 54 | ✅ | Flexible Fetch v12 — compact topics/records + header v1 | [PHASE54_SPEC.md](../PHASE54_SPEC.md) |
| 55 | ✅ | Flexible group consumer — JoinGroup v6, Sync/Heartbeat/Leave v4 | [PHASE55_SPEC.md](../PHASE55_SPEC.md) |
| 56 | ✅ | Group flex field completeness — Join v7–9, Sync v5, Leave v5 | [PHASE56_SPEC.md](../PHASE56_SPEC.md) |
| 57 | ✅ | Flexible OffsetCommit v8 + OffsetFetch v6–7 | [PHASE57_SPEC.md](../PHASE57_SPEC.md) |
| 58 | ✅ | OffsetFetch multi-group flexible v8 | [PHASE58_SPEC.md](../PHASE58_SPEC.md) |
| 59 | ✅ | Flexible group admin — Describe v5, List v3, Delete v2 | [PHASE59_SPEC.md](../PHASE59_SPEC.md) |
| 60 | ✅ | Flexible topic admin — Create v5, Delete v4, CreatePartitions v2 | [PHASE60_SPEC.md](../PHASE60_SPEC.md) |
| 61 | ✅ | Flexible configs — Describe v4, Alter v2, IncrementalAlter v1 | [PHASE61_SPEC.md](../PHASE61_SPEC.md) |
| 62 | ✅ | Flexible transaction APIs — InitProducerId v2 + txn APIs v3 | [PHASE62_SPEC.md](../PHASE62_SPEC.md) |
| 63 | ✅ | Flexible ListOffsets v6 + OffsetForLeaderEpoch v4 | [PHASE63_SPEC.md](../PHASE63_SPEC.md) |
| 64 | ✅ | Flexible DeleteRecords v2 + Describe/Create/DeleteAcls v2 | [PHASE64_SPEC.md](../PHASE64_SPEC.md) |
| 65 | ✅ | SaslAuthenticate v2 + DescribeCluster v0 + ListTransactions v0 | [PHASE65_SPEC.md](../PHASE65_SPEC.md) |
| 66 | ✅ | DescribeTransactions/Producers v0; DescribeCluster/ListTransactions bumps | [PHASE66_SPEC.md](../PHASE66_SPEC.md) |
| 67 | ✅ | Metadata TopicId v10–12 — deterministic UUID mapping | [PHASE67_SPEC.md](../PHASE67_SPEC.md) |
| 68 | ✅ | Fetch TopicId v13 — request/response UUID topics (KIP-516) | [PHASE68_SPEC.md](../PHASE68_SPEC.md) |
| 69 | ✅ | Admin TopicId — CreateTopics v7; DeleteTopics v5–6 by TopicId | [PHASE69_SPEC.md](../PHASE69_SPEC.md) |
| 70 | ✅ | DescribeCluster v2 (IsFenced) + ListTransactions v2 (pattern filter) | [PHASE70_SPEC.md](../PHASE70_SPEC.md) |
| 71 | ✅ | Produce TopicId v13 — UUID topics; KIP-951 tags empty at ship | [PHASE71_SPEC.md](../PHASE71_SPEC.md) |
| 72 | ✅ | OffsetCommit/OffsetFetch v9–10 — TopicId + MemberId fields | [PHASE72_SPEC.md](../PHASE72_SPEC.md) |
| 73 | ✅ | Metadata v13 — top-level ErrorCode (always 0 on success) | [PHASE73_SPEC.md](../PHASE73_SPEC.md) |
| 74 | ✅ | ListOffsets v7–11 — MAX_TIMESTAMP, EARLIEST_LOCAL, tiered specials | [PHASE74_SPEC.md](../PHASE74_SPEC.md) |
| 75 | ✅ | KIP-890-era txn max versions (Init/AddPartitions/EndTxn/TxnOffsetCommit ≤5) | [PHASE75_SPEC.md](../PHASE75_SPEC.md) |
| 76 | ✅ | TxnOffsetCommit v6 TopicId — UUID topics, buffers until EndTxn | [PHASE76_SPEC.md](../PHASE76_SPEC.md) |
| 77 | ✅ | InitProducerId v6 — Enable2Pc/KeepPreparedTxn parsed+ignored; no real 2PC | [PHASE77_SPEC.md](../PHASE77_SPEC.md) |
| 78 | ✅ | KIP-951 CurrentLeader / NodeEndpoints on Produce/Fetch leader errors | [PHASE78_SPEC.md](../PHASE78_SPEC.md) |
| 79 | ✅ | Group admin version bumps — List 0–5, Describe 0–6, Delete 0–3 | [PHASE79_SPEC.md](../PHASE79_SPEC.md) |
| 80 | ✅ | CreatePartitions v3 — wire-identical to v2; no KIP-599 quotas | [PHASE80_SPEC.md](../PHASE80_SPEC.md) |

---

## Still deferred (post–Phase 80)

- Multi-language clients
- Chaos-mesh / cargo-fuzz corpus CI
- True control-marker `READ_COMMITTED`
- Real 2PC / prepared transaction state
