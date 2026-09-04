# Phase history index

Ship records for **phases 0–154** (shipped). Binding core contracts are
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

## Kafka flexible/modern (51–85)

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
| 77 | ✅ | InitProducerId v6 — Enable2Pc/KeepPreparedTxn wire (prepared state in Phase 90) | [PHASE77_SPEC.md](../PHASE77_SPEC.md) |
| 78 | ✅ | KIP-951 CurrentLeader / NodeEndpoints on Produce/Fetch leader errors | [PHASE78_SPEC.md](../PHASE78_SPEC.md) |
| 79 | ✅ | Group admin version bumps — List 0–5, Describe 0–6, Delete 0–3 | [PHASE79_SPEC.md](../PHASE79_SPEC.md) |
| 80 | ✅ | CreatePartitions v3 — wire-identical to v2; no KIP-599 quotas | [PHASE80_SPEC.md](../PHASE80_SPEC.md) |
| 81 | ✅ | FindCoordinator v5–6 — wire-identical to v4 batch; no TRANSACTION_ABORTABLE / share key_type | [PHASE81_SPEC.md](../PHASE81_SPEC.md) |
| 82 | ✅ | AddOffsetsToTxn v4 — wire-identical to v3; 123 emission deferred → Phase 94 | [PHASE82_SPEC.md](../PHASE82_SPEC.md) |
| 83 | ✅ | ApiVersions v4–5 — Kafka max; empty feature tags; v5 ClusterId/NodeId ignored | [PHASE83_SPEC.md](../PHASE83_SPEC.md) |
| 84 | ✅ | Fetch v14–18 — Kafka max; ReplicaState ignore; NodeEndpoints v16+ | [PHASE84_SPEC.md](../PHASE84_SPEC.md) |
| 85 | ✅ | ACL admin v3 — User resource type; Describe/Create/DeleteAcls 0–3 Kafka max | [PHASE85_SPEC.md](../PHASE85_SPEC.md) |
| 86 | ✅ | Write-through txn + soft-marker READ_COMMITTED (true LSO, aborted list MVP) | [PHASE86_SPEC.md](../PHASE86_SPEC.md) |
| 87 | ✅ | Durable OffsetForLeaderEpoch history MVP + Metadata live leader_epoch | [PHASE87_SPEC.md](../PHASE87_SPEC.md) |
| 88 | ✅ | Fetch DivergingEpoch + real fetch sessions (MVP) | [PHASE88_SPEC.md](../PHASE88_SPEC.md) |
| 89 | ✅ | Kafka control batches on the data log (COMMIT/ABORT dual-write) | [PHASE89_SPEC.md](../PHASE89_SPEC.md) |
| 90 | ✅ | Real 2PC / prepared transactions MVP (Enable2Pc, OngoingTxn*, durable prepare) | [PHASE90_SPEC.md](../PHASE90_SPEC.md) |
| 91 | ✅ | Omit-unchanged incremental fetch session responses (MVP) | [PHASE91_SPEC.md](../PHASE91_SPEC.md) |
| 92 | ✅ | Prepared transaction timeout / auto-abort (MVP) | [PHASE92_SPEC.md](../PHASE92_SPEC.md) |
| 93 | ✅ | Open transaction timeout (InitProducerId / broker default; lazy auto-abort) | [PHASE93_SPEC.md](../PHASE93_SPEC.md) |
| 94 | ✅ | TRANSACTION_ABORTABLE (123) honest subset after timeout auto-abort | [PHASE94_SPEC.md](../PHASE94_SPEC.md) |
| 95 | ✅ | Fetch session idle TTL + max concurrent sessions (lazy LRU) | [PHASE95_SPEC.md](../PHASE95_SPEC.md) |
| 96 | ✅ | Broker `transaction.max.timeout.ms` clamp (Init reject 50 + effective clamp) | [PHASE96_SPEC.md](../PHASE96_SPEC.md) |
| 97 | ✅ | Background txn + session sweeper with metrics (MVP) | [PHASE97_SPEC.md](../PHASE97_SPEC.md) |
| 98 | ✅ | Control batches for crash≡abort open write-through txns (MVP) | [PHASE98_SPEC.md](../PHASE98_SPEC.md) |
| 99 | ✅ | DescribeConfigs/Alter BROKER for txn/session/sweep knobs (MVP) | [PHASE99_SPEC.md](../PHASE99_SPEC.md) |
| 100 | ✅ | Durable dynamic broker config file (MVP; six Phase 99 knobs) | [PHASE100_SPEC.md](../PHASE100_SPEC.md) |
| 101 | ✅ | Graceful sweeper enable on 0→>0 interval without process restart | [PHASE101_SPEC.md](../PHASE101_SPEC.md) |
| 102 | ✅ | Sparse durable broker config (only altered keys; env re-applies after DELETE) | [PHASE102_SPEC.md](../PHASE102_SPEC.md) |
| 103 | ✅ | Validate BROKER resource name against local `node_id` (empty or decimal match) | [PHASE103_SPEC.md](../PHASE103_SPEC.md) |
| 104 | ✅ | Aborted soft-marker GC with DeleteRecords / retention / load (MVP) | [PHASE104_SPEC.md](../PHASE104_SPEC.md) |
| 105 | ✅ | Control batches for empty AddPartitions (MVP) | [PHASE105_SPEC.md](../PHASE105_SPEC.md) |
| 106 | ✅ | Graceful background task shutdown / join on server stop (MVP) | [PHASE106_SPEC.md](../PHASE106_SPEC.md) |
| 107 | ✅ | Stabilize phase103 parallel test flake (unique temp dirs; catalog/config parent recreate) | [PHASE107_SPEC.md](../PHASE107_SPEC.md) |
| 108 | ✅ | Fix rolling restart produce timeout when follower down (ISR shrink + HWM on death) | [PHASE108_SPEC.md](../PHASE108_SPEC.md) |
| 109 | ✅ | Accept-loop drain + single-flight `start_background_tasks` (MVP) | [PHASE109_SPEC.md](../PHASE109_SPEC.md) |
| 110 | ✅ | Non-controller auto-death from heartbeat alive-set diffs (MVP) | [PHASE110_SPEC.md](../PHASE110_SPEC.md) |
| 111 | ✅ | Clip straddling soft abort markers to log_start (MVP) | [PHASE111_SPEC.md](../PHASE111_SPEC.md) |
| 112 | ✅ | cargo-fuzz corpus smoke + CI (MVP) | [PHASE112_SPEC.md](../PHASE112_SPEC.md) |
| 113 | ✅ | Cluster admin fan-out MVP (DeleteRecords + BROKER config + ACL snapshot) | [PHASE113_SPEC.md](../PHASE113_SPEC.md) |
| 114 | ✅ | Multi-broker 2PC / KIP-890-ish MVP (Enable2Pc prepare/complete across leaders) | [PHASE114_SPEC.md](../PHASE114_SPEC.md) |
| 115 | ✅ | Durable fetch sessions MVP (per-broker `__fetch_sessions`; restart restore; not multi-broker sticky) | [PHASE115_SPEC.md](../PHASE115_SPEC.md) |
| 116 | ✅ | Durable DeleteRecords outbox for offline replicas (leader-local pending + live drain) | [PHASE116_SPEC.md](../PHASE116_SPEC.md) |
| 117 | ✅ | Controller failover catch-up for ACL + BROKER config (durable gens + heartbeat re-push) | [PHASE117_SPEC.md](../PHASE117_SPEC.md) |
| 118 | ✅ | ISR rejoin + lag-based shrink (ReplicaFetch catch-up re-expand; metrics) | [PHASE118_SPEC.md](../PHASE118_SPEC.md) |
| 119 | ✅ | Multi-broker fetch session handoff MVP (owner-encoded id + transparent forward) | [PHASE119_SPEC.md](../PHASE119_SPEC.md) |
| 120 | ✅ | Transparent EndTxn / txn RPC forward MVP (coordinator registry + KafkaTxnForward) | [PHASE120_SPEC.md](../PHASE120_SPEC.md) |
| 121 | ✅ | Sticky FindCoordinator assignment MVP (murmur2 static ring + Init-owner override) | [PHASE121_SPEC.md](../PHASE121_SPEC.md) |
| 122 | ✅ | Transparent AddOffsetsToTxn / TxnOffsetCommit forward MVP (reuse KafkaTxnForward 84/85) | [PHASE122_SPEC.md](../PHASE122_SPEC.md) |
| 123 | ✅ | DeleteRecords outbox leadership handoff MVP (new leader reconcile from log_start) | [PHASE123_SPEC.md](../PHASE123_SPEC.md) |
| 124 | ✅ | Durable txn coordinator registry MVP (per-broker `__txn_coordinator`; restart restore for forward/FC) | [PHASE124_SPEC.md](../PHASE124_SPEC.md) |
| 125 | ✅ | Time-based ISR lag shrink MVP (last-caught-up + `replica_lag_max_ms`; rejoin still Phase 118) | [PHASE125_SPEC.md](../PHASE125_SPEC.md) |
| 126 | ✅ | PreferredReadReplica / rack-aware Fetch MVP (KIP-392 subset; Metadata rack; LEO≥HWM ISR peer) | [PHASE126_SPEC.md](../PHASE126_SPEC.md) |
| 127 | ✅ | Txn coordinator registry TTL GC MVP (last-touch + `VOLANT_TXN_COORDINATOR_TTL_MS`; sweeper hook) | [PHASE127_SPEC.md](../PHASE127_SPEC.md) |
| 128 | ✅ | BROKER Describe/Alter for txn coordinator registry TTL (`volant.txn.coordinator.registry.ttl.ms`) | [PHASE128_SPEC.md](../PHASE128_SPEC.md) |
| 129 | ✅ | Controller SoT DeleteRecords truncate journal MVP (note/push + reconcile max watermark) | [PHASE129_SPEC.md](../PHASE129_SPEC.md) |
| 130 | ✅ | Multi-controller majority consensus for truncate journal (Raft-style commit; always full-snapshot push) | [PHASE130_SPEC.md](../PHASE130_SPEC.md) |
| 131 | ✅ | Truncate journal rejoin catch-up (HeartbeatBroker applied_journal_generation + lag-driven TruncateJournalPush) | [PHASE131_SPEC.md](../PHASE131_SPEC.md) |
| 132 | ✅ | Truncate journal catch-up hardening (non-blocking schedule / single-flight / min-interval + push wire ITs) | [PHASE132_SPEC.md](../PHASE132_SPEC.md) |
| 133 | ✅ | Preferred read-replica selector polish (usable addr + highest LEO then lowest id) | [PHASE133_SPEC.md](../PHASE133_SPEC.md) |
| 134 | ✅ | Peer-to-peer heartbeat mesh (HB all peers; alive-set only vs controller; journal catch-up path) | [PHASE134_SPEC.md](../PHASE134_SPEC.md) |
| 135 | ✅ | DeleteRecords optional majority wait (client-visible journal majority; default off) | [PHASE135_SPEC.md](../PHASE135_SPEC.md) |
| 136 | ✅ | Non-blocking admin ACL/BROKER catch-up (schedule + single-flight + min-interval) | [PHASE136_SPEC.md](../PHASE136_SPEC.md) |
| 137 | ✅ | DeleteRecords native wait trailer + journal topic GC (assignment prune + anti-resurrection push) | [PHASE137_SPEC.md](../PHASE137_SPEC.md) |
| 138 | ✅ | Shared fetch session mirror + promote MVP (best-effort peer put/delete 90–93; promote on owner miss; not Raft) | [PHASE138_SPEC.md](../PHASE138_SPEC.md) |
| 139 | ✅ | Session mirror polish (coalesce/debounce Puts; optional durable `__fetch_session_mirrors`; `mirror_gen` fence) | [PHASE139_SPEC.md](../PHASE139_SPEC.md) |
| 140 | ✅ | Preferred-replica selector depth (optional max LEO lag; RC suppress metric) | [PHASE140_SPEC.md](../PHASE140_SPEC.md) |
| 141 | ✅ | N=2 majority ops tooling (configured/live/quorum/impossible gauges + Broker helpers) | [PHASE141_SPEC.md](../PHASE141_SPEC.md) |
| 142 | ✅ | Metadata ISR freshness when leader ≠ controller (overlay + IsrUpdate 94/95) | [PHASE142_SPEC.md](../PHASE142_SPEC.md) |
| 143 | ✅ | Fetch session promote claim fence (lowest-id `promoted_by`; MirrorPut converge) | [PHASE143_SPEC.md](../PHASE143_SPEC.md) |
| 144 | ✅ | Preferred × session thrash suppress (`session_id != 0` → no PreferredReadReplica) | [PHASE144_SPEC.md](../PHASE144_SPEC.md) |
| 145 | ✅ | Rack-aware partition assignment MVP (multi-rack create diversity + metric) | [PHASE145_SPEC.md](../PHASE145_SPEC.md) |
| 146 | ✅ | Incremental/delta MirrorPut wire (`mode=full|delta`; last_mirrored cache) | [PHASE146_SPEC.md](../PHASE146_SPEC.md) |
| 147 | ✅ | Serve-from-mirror without promote (owner miss; dual-epoch residual) | [PHASE147_SPEC.md](../PHASE147_SPEC.md) |
| 148 | ✅ | Defer local DeleteRecords truncate until journal majority (wait mode) | [PHASE148_SPEC.md](../PHASE148_SPEC.md) |
| 149 | ✅ | Durable stream state store (`DurableStore` / redb; not exactly-once alone) | [PHASE149_SPEC.md](../PHASE149_SPEC.md) |
| 150 | ✅ | Assignment generation majority consensus (opcodes 96/97; static N) | [PHASE150_SPEC.md](../PHASE150_SPEC.md) |
| 151 | ✅ | Stream exactly-once MVP (txn produce + deferred group offsets) | [PHASE151_SPEC.md](../PHASE151_SPEC.md) |
| 152 | ✅ | Assignment consensus depth (Metadata = committed snapshot) | [PHASE152_SPEC.md](../PHASE152_SPEC.md) |
| 153 | ✅ | EOS + durable stream state atomic checkpoint boundary | [PHASE153_SPEC.md](../PHASE153_SPEC.md) |
| 154 | ✅ | KRaft-style metadata Raft log MVP (opcodes 98/99; static N) | [PHASE154_SPEC.md](../PHASE154_SPEC.md) |
| 155 | 🚧 | openraft cluster SoT + native SyncGroup 116/117 + Join retry + Go CreateTopic id | [PHASE155_SPEC.md](../PHASE155_SPEC.md) |

### Residual slices (not phases)

| Slice | Status | One-liner | Spec |
|------:|:------:|-----------|------|
| v0.3 | ✅ | Assignment wait-fail local rollback | — |
| v0.4 | ✅ | Kafka admin assignment wait/rollback | — |
| v0.5 | ✅ | Ops confidence (unwritable dir / isolate / in-flight acks=all) | — |
| v0.6 | ✅ | Kafka DeleteRecords flex v2 tag 0 wait flag | [V06_SPEC.md](../V06_SPEC.md) |
| v0.7 | ✅ | Preferred redirect throttle + TCP probe | [V07_SPEC.md](../V07_SPEC.md) |
| v0.8 | ✅ | Cross-app EOS fencing via `application_id` | [V08_SPEC.md](../V08_SPEC.md) |
| v0.9 | ✅ | EOS changelog-backed durable state (txn 2PC MVP) | [V09_SPEC.md](../V09_SPEC.md) |
| v0.10 | ✅ | Dynamic membership overlay (add/remove broker) | [V10_SPEC.md](../V10_SPEC.md) |
| v0.11 | ✅ | openraft metadata leader election (opt-in; default lowest-id) | [V11_SPEC.md](../V11_SPEC.md) |
| v0.12 | ✅ | `__cluster_metadata` topic + per-partition Raft log MVP | [V12_SPEC.md](../V12_SPEC.md) |
| v0.13 | ✅ | `__transaction_state` topic (KIP-890 log MVP) | [V13_SPEC.md](../V13_SPEC.md) |
| v0.14 | ✅ | Python native client (produce/fetch/metadata) | [V14_SPEC.md](../V14_SPEC.md) |
| v0.15 | ✅ | Fuzz corpus expansion + chaos-mesh + A→B isolate | [V15_SPEC.md](../V15_SPEC.md) |
| v0.16 | ✅ | openraft SetAssignment log apply | [V16_SPEC.md](../V16_SPEC.md) |
| v0.17 | ✅ | openraft InstallSnapshot (opcodes 112/113) | [V17_SPEC.md](../V17_SPEC.md) |
| v0.18 | ✅ | Partition reassignment after add-broker | [V18_SPEC.md](../V18_SPEC.md) |
| v0.19 | ✅ | Go native client (produce/fetch/metadata) | [V19_SPEC.md](../V19_SPEC.md) |
| v0.20 | ✅ | Produce group-commit (coalesced fsync) | [V20_SPEC.md](../V20_SPEC.md) |
| v0.21 | ✅ | Durable openraft log + hard state | [V21_SPEC.md](../V21_SPEC.md) |
| v0.22 | ✅ | Apply assignment from openraft snapshot | [V22_SPEC.md](../V22_SPEC.md) |
| v0.23 | ✅ | Java native client (produce/fetch/metadata) | [V23_SPEC.md](../V23_SPEC.md) |
| v0.24 | ✅ | Python and Go offset commit/fetch | [V24_SPEC.md](../V24_SPEC.md) |
| v0.25 | ✅ | Fetch-session dual-epoch converge | [V25_SPEC.md](../V25_SPEC.md) |
| v0.26 | ✅ | openraft joint membership on add/remove | [V26_SPEC.md](../V26_SPEC.md) |
| v0.27 | ✅ | TLS for Python/Go/Java native clients | [V27_SPEC.md](../V27_SPEC.md) |
| v0.28 | ✅ | JoinGroup/Heartbeat/LeaveGroup on native clients | [V28_SPEC.md](../V28_SPEC.md) |
| v0.29 | ✅ | Cluster DeleteRecords wait-off safety | [V29_SPEC.md](../V29_SPEC.md) |
| v0.30 | ✅ | Fetch-session mirror-only self-converge | [V30_SPEC.md](../V30_SPEC.md) |
| v0.31 | ✅ | Python GroupConsumer (join/poll/commit) | [V31_SPEC.md](../V31_SPEC.md) |
| v0.32 | ✅ | Go GroupConsumer (join/poll/commit) | [V32_SPEC.md](../V32_SPEC.md) |
| v0.33 | ✅ | Java offset commit/fetch + GroupConsumer | [V33_SPEC.md](../V33_SPEC.md) |
| v0.34 | ✅ | Rollback overlay if openraft joint fails | [V34_SPEC.md](../V34_SPEC.md) |
| v0.35 | ✅ | Store openraft log in redb | [V35_SPEC.md](../V35_SPEC.md) |
| v0.36 | ✅ | Static group_instance_id on Python/Go/Java GroupConsumers | [V36_SPEC.md](../V36_SPEC.md) |
| v0.37 | ✅ | GroupConsumer background heartbeat (Python/Go/Java) | [V37_SPEC.md](../V37_SPEC.md) |
| v0.38 | ✅ | Follower Add/RemoveBroker forward to openraft leader | [V38_SPEC.md](../V38_SPEC.md) |
| v0.39 | ✅ | Restore assignment if add-broker joint rolls back | [V39_SPEC.md](../V39_SPEC.md) |
| v0.40 | ✅ | Wait for homemade metadata-raft commit before CreateTopic ok | [V40_SPEC.md](../V40_SPEC.md) |
| v0.41 | ✅ | Client-side range assignor on Python/Go/Java | [V41_SPEC.md](../V41_SPEC.md) |
| v0.42 | ✅ | Shared-token Auth on Python/Go/Java clients | [V42_SPEC.md](../V42_SPEC.md) |
| v0.43 | ✅ | Leader redirect on Python/Go/Java produce/fetch | [V43_SPEC.md](../V43_SPEC.md) |
| v0.44 | ✅ | Rust GroupConsumer background heartbeat | [V44_SPEC.md](../V44_SPEC.md) |
| v0.45 | ✅ | Clustered DeleteRecords wait-off requires ACK | [V45_SPEC.md](../V45_SPEC.md) |
| v0.46 | ✅ | SCRAM-SHA-256 on Python/Go/Java clients | [V46_SPEC.md](../V46_SPEC.md) |
| v0.47 | ✅ | Idempotent produce on Python/Go/Java | [V47_SPEC.md](../V47_SPEC.md) |
| v0.48 | ✅ | GroupConsumer auto-commit on Python/Go/Java | [V48_SPEC.md](../V48_SPEC.md) |
| v0.49 | ✅ | ListGroups and DescribeGroup on Python/Go/Java | [V49_SPEC.md](../V49_SPEC.md) |
| v0.50 | ✅ | ListOffsets on Python/Go/Java clients | [V50_SPEC.md](../V50_SPEC.md) |
| v0.51 | ✅ | CreatePartitions on Python/Go/Java clients | [V51_SPEC.md](../V51_SPEC.md) |
| v0.52 | ✅ | DeleteRecords on Python/Go/Java clients | [V52_SPEC.md](../V52_SPEC.md) |
| v0.53 | ✅ | DescribeConfigs and AlterConfigs on Python/Go/Java | [V53_SPEC.md](../V53_SPEC.md) |
| v0.54 | ✅ | DeleteOffsets on Python/Go/Java clients | [V54_SPEC.md](../V54_SPEC.md) |
| v0.55 | ✅ | Create/Delete/ListScramUsers on Python/Go/Java | [V55_SPEC.md](../V55_SPEC.md) |
| v0.56 | ✅ | Create/Delete/ListAcls on Python/Go/Java | [V56_SPEC.md](../V56_SPEC.md) |
| v0.57 | ✅ | BeginTxn/EndTxn on Python/Go/Java | [V57_SPEC.md](../V57_SPEC.md) |
| v0.58 | ✅ | AddBroker/RemoveBroker/ListMembers on Python/Go/Java | [V58_SPEC.md](../V58_SPEC.md) |
| v0.59 | ✅ | ReassignPartitions on Python/Go/Java | [V59_SPEC.md](../V59_SPEC.md) |
| v0.60 | ✅ | Rust GroupConsumer auto-commit | [V60_SPEC.md](../V60_SPEC.md) |
| v0.61 | ✅ | Produce retry on Python/Go/Java | [V61_SPEC.md](../V61_SPEC.md) |
| v0.62 | ✅ | GroupConsumer auto_offset_reset on Python/Go/Java | [V62_SPEC.md](../V62_SPEC.md) |
| v0.63 | ✅ | TransactionalProducer helper on Python/Go/Java | [V63_SPEC.md](../V63_SPEC.md) |
| v0.64 | ✅ | Go/Java Fetch knobs and Produce acks | [V64_SPEC.md](../V64_SPEC.md) |
| v0.65 | ✅ | DeleteRecords leader redirect on Python/Go/Java | [V65_SPEC.md](../V65_SPEC.md) |
| v0.66 | ✅ | Fetch retry on Python/Go/Java | [V66_SPEC.md](../V66_SPEC.md) |
| v0.67 | ✅ | Rust GroupConsumer auto_offset_reset | [V67_SPEC.md](../V67_SPEC.md) |
| v0.68 | ✅ | Go/Java ProduceBatch | [V68_SPEC.md](../V68_SPEC.md) |
| v0.69 | ✅ | Multi-member range via DescribeGroup | [V69_SPEC.md](../V69_SPEC.md) |
| v0.70 | ✅ | GroupConsumer earliest via ListOffsets | [V70_SPEC.md](../V70_SPEC.md) |
| v0.71 | ✅ | Rust GroupConsumer earliest via ListOffsets | [V71_SPEC.md](../V71_SPEC.md) |
| v0.72 | ✅ | Admin NotController redirect on Python/Go/Java | [V72_SPEC.md](../V72_SPEC.md) |
| v0.73 | ✅ | Rust GroupConsumer range via DescribeGroup | [V73_SPEC.md](../V73_SPEC.md) |
| v0.74 | ✅ | Heartbeat retry on Python/Go/Java | [V74_SPEC.md](../V74_SPEC.md) |
| v0.75 | ✅ | GroupConsumer poll fetch knobs on Python/Go/Java | [V75_SPEC.md](../V75_SPEC.md) |
| v0.76 | ✅ | Rust GroupConsumer poll fetch knobs | [V76_SPEC.md](../V76_SPEC.md) |
| v0.77 | ✅ | Metadata controller_id trailer | [V77_SPEC.md](../V77_SPEC.md) |
| v0.78 | ✅ | OffsetCommit/Fetch/DeleteOffsets retry on Python/Go/Java | [V78_SPEC.md](../V78_SPEC.md) |
| v0.79 | ✅ | Rust admin NotController redirect | [V79_SPEC.md](../V79_SPEC.md) |
| v0.80 | ✅ | Rust heartbeat retry | [V80_SPEC.md](../V80_SPEC.md) |
| v0.81 | ✅ | Language admin-14 prefers Metadata.controller_id | [V81_SPEC.md](../V81_SPEC.md) |
| v0.82 | ✅ | ListOffsets retry on Python/Go/Java | [V82_SPEC.md](../V82_SPEC.md) |
| v0.83 | ✅ | Rust OffsetCommit/Fetch/DeleteOffsets retry | [V83_SPEC.md](../V83_SPEC.md) |
| v0.84 | ✅ | Rust ListOffsets retry | [V84_SPEC.md](../V84_SPEC.md) |
| v0.85 | ✅ | SCRAM-admin/ListAcls NotController redirect on Python/Go/Java | [V85_SPEC.md](../V85_SPEC.md) |
| v0.86 | ✅ | LeaveGroup retry on Python/Go/Java | [V86_SPEC.md](../V86_SPEC.md) |
| v0.87 | ✅ | Rust LeaveGroup retry | [V87_SPEC.md](../V87_SPEC.md) |
| v0.88 | ✅ | Rust SCRAM-admin/ListAcls NotController redirect | [V88_SPEC.md](../V88_SPEC.md) |
| v0.89 | ✅ | AddBroker/RemoveBroker NotController redirect on Python/Go/Java | [V89_SPEC.md](../V89_SPEC.md) |
| v0.90 | ✅ | DescribeGroup/ListGroups retry on Python/Go/Java | [V90_SPEC.md](../V90_SPEC.md) |
| v0.91 | ✅ | Rust AddBroker/RemoveBroker NotController redirect | [V91_SPEC.md](../V91_SPEC.md) |
| v0.92 | ✅ | Rust DescribeGroup/ListGroups retry | [V92_SPEC.md](../V92_SPEC.md) |
| v0.93 | ✅ | Describe/AlterConfigs NotController redirect on Python/Go/Java | [V93_SPEC.md](../V93_SPEC.md) |
| v0.94 | ✅ | Rust Describe/AlterConfigs NotController redirect | [V94_SPEC.md](../V94_SPEC.md) |
| v0.95 | ✅ | Metadata/ListMembers retry on Python/Go/Java | [V95_SPEC.md](../V95_SPEC.md) |
| v0.96 | ✅ | Rust Metadata/ListMembers retry | [V96_SPEC.md](../V96_SPEC.md) |
| v0.97 | ✅ | DeleteOffsets NotController redirect on Python/Go/Java | [V97_SPEC.md](../V97_SPEC.md) |
| v0.98 | ✅ | Rust DeleteOffsets NotController redirect | [V98_SPEC.md](../V98_SPEC.md) |
| v0.99 | ✅ | BeginTxn/EndTxn retry on Python/Go/Java | [V99_SPEC.md](../V99_SPEC.md) |
| v0.100 | ✅ | Rust BeginTxn/EndTxn retry | [V100_SPEC.md](../V100_SPEC.md) |
| v0.101 | ✅ | InitProducerId retry on Python/Go/Java | [V101_SPEC.md](../V101_SPEC.md) |
| v0.102 | ✅ | Rust InitProducerId retry | [V102_SPEC.md](../V102_SPEC.md) |
| v0.103 | ✅ | admin_round_trip transient retry on Python/Go/Java | [V103_SPEC.md](../V103_SPEC.md) |
| v0.104 | ✅ | Rust admin_round_trip transient retry | [V104_SPEC.md](../V104_SPEC.md) |
| v0.105 | ✅ | OffsetCommit/Fetch NotController redirect on Python/Go/Java | [V105_SPEC.md](../V105_SPEC.md) |
| v0.106 | ✅ | Auth retry on Python/Go/Java | [V106_SPEC.md](../V106_SPEC.md) |
| v0.107 | ✅ | Rust Auth retry | [V107_SPEC.md](../V107_SPEC.md) |
| v0.108 | ✅ | SCRAM handshake retry on Python/Go/Java | [V108_SPEC.md](../V108_SPEC.md) |
| v0.109 | ✅ | Rust SCRAM handshake retry | [V109_SPEC.md](../V109_SPEC.md) |
| v0.110 | ✅ | DeleteRecords transient retry on Python/Go/Java | [V110_SPEC.md](../V110_SPEC.md) |
| v0.111 | ✅ | Rust DeleteRecords 13 redirect + transient retry | [V111_SPEC.md](../V111_SPEC.md) |
| v0.112 | ✅ | ListOffsets NotLeader redirect on Python/Go/Java | [V112_SPEC.md](../V112_SPEC.md) |
| v0.113 | ✅ | Rust ListOffsets NotLeader redirect | [V113_SPEC.md](../V113_SPEC.md) |
| v0.114 | ✅ | Rust metadata topic filter | [V114_SPEC.md](../V114_SPEC.md) |
| v0.115 | ✅ | public reconnect on Python/Go/Java | [V115_SPEC.md](../V115_SPEC.md) |
| v0.116 | ✅ | Go/Java metadata topic filter | [V116_SPEC.md](../V116_SPEC.md) |
| v0.117 | ✅ | Go/Java CreateTopic configs | [V117_SPEC.md](../V117_SPEC.md) |
| v0.118 | ✅ | OffsetFetch all-group on Python/Go/Java | [V118_SPEC.md](../V118_SPEC.md) |
| v0.119 | ✅ | public CommitOffsets batch on Python/Go/Java | [V119_SPEC.md](../V119_SPEC.md) |
| v0.120 | ✅ | Rust ListMembers NotController redirect | [V120_SPEC.md](../V120_SPEC.md) |
| v0.121 | ✅ | ListMembers NotController redirect on Python/Go/Java | [V121_SPEC.md](../V121_SPEC.md) |
| v0.122 | ✅ | OffsetFetch entries on Python/Go/Java | [V122_SPEC.md](../V122_SPEC.md) |
| v0.123 | ✅ | Python GroupConsumer batch OffsetCommit | [V123_SPEC.md](../V123_SPEC.md) |
| v0.124 | ✅ | DescribeGroup/ListGroups NotController redirect on Python/Go/Java | [V124_SPEC.md](../V124_SPEC.md) |
| v0.125 | ✅ | Rust DescribeGroup/ListGroups NotController redirect | [V125_SPEC.md](../V125_SPEC.md) |
| v0.126 | ✅ | Go CreateTopicID returns topic id | [V126_SPEC.md](../V126_SPEC.md) |
| v0.127 | ✅ | Go/Java JoinGroup with instance id | [V127_SPEC.md](../V127_SPEC.md) |
| v0.128 | ✅ | Go/Java OffsetCommit metadata | [V128_SPEC.md](../V128_SPEC.md) |
| v0.129 | ✅ | language produce default acks | [V129_SPEC.md](../V129_SPEC.md) |
| v0.130 | ✅ | Go/Java Produce headers | [V130_SPEC.md](../V130_SPEC.md) |
| v0.131 | ✅ | Go/Java JoinGroup rejoin member_id | [V131_SPEC.md](../V131_SPEC.md) |
| v0.132 | ✅ | Go/Java Produce timestamp | [V132_SPEC.md](../V132_SPEC.md) |
| v0.133 | ✅ | Go/Java Produce headers + acks | [V133_SPEC.md](../V133_SPEC.md) |
| v0.134 | ✅ | Heartbeat NotController redirect on Python/Go/Java | [V134_SPEC.md](../V134_SPEC.md) |
| v0.135 | ✅ | Rust Heartbeat NotController redirect | [V135_SPEC.md](../V135_SPEC.md) |
| v0.136 | ✅ | LeaveGroup NotController redirect on Python/Go/Java | [V136_SPEC.md](../V136_SPEC.md) |
| v0.137 | ✅ | Rust LeaveGroup NotController redirect | [V137_SPEC.md](../V137_SPEC.md) |
| v0.138 | ✅ | Go/Java Produce timestamp + headers | [V138_SPEC.md](../V138_SPEC.md) |
| v0.139 | ✅ | Go OffsetCommit member + generation | [V139_SPEC.md](../V139_SPEC.md) |
| v0.140 | ✅ | Go/Java OffsetFetch entry metadata | [V140_SPEC.md](../V140_SPEC.md) |
| v0.141 | ✅ | Go/Java Produce timestamp + acks | [V141_SPEC.md](../V141_SPEC.md) |
| v0.142 | ✅ | Go/Java Produce timestamp + headers + acks | [V142_SPEC.md](../V142_SPEC.md) |
| v0.143 | ✅ | language Fetch client-level default knobs | [V143_SPEC.md](../V143_SPEC.md) |
| v0.144 | ✅ | Rust ClientConfig Fetch knobs | [V144_SPEC.md](../V144_SPEC.md) |
| v0.145 | ✅ | Go/Java Fetch high watermark | [V145_SPEC.md](../V145_SPEC.md) |
| v0.146 | ✅ | Java JoinGroup member + instance | [V146_SPEC.md](../V146_SPEC.md) |
| v0.147 | ✅ | Go/Java ProduceBatch default acks | [V147_SPEC.md](../V147_SPEC.md) |
| v0.148 | ✅ | language OffsetFetch topic + metadata | [V148_SPEC.md](../V148_SPEC.md) |
| v0.149 | ✅ | Rust fetch uses ClientConfig fetch_max_bytes | [V149_SPEC.md](../V149_SPEC.md) |
| v0.150 | ✅ | language public InitProducerId | [V150_SPEC.md](../V150_SPEC.md) |
| v0.151 | ✅ | Rust public InitProducerId | [V151_SPEC.md](../V151_SPEC.md) |
| v0.152 | ✅ | language DeleteRecords default wait flag | [V152_SPEC.md](../V152_SPEC.md) |
| v0.153 | ✅ | Rust single-entry OffsetCommit | [V153_SPEC.md](../V153_SPEC.md) |
| v0.154 | ✅ | Rust OffsetFetch topic + metadata | [V154_SPEC.md](../V154_SPEC.md) |
| v0.155 | ✅ | Rust DeleteRecords default wait flag (not Phase 155) | [V155_SPEC.md](../V155_SPEC.md) |
| v0.156 | ✅ | Metadata NotController redirect on Python/Go/Java | [V156_SPEC.md](../V156_SPEC.md) |
| v0.157 | ✅ | Rust Metadata NotController redirect | [V157_SPEC.md](../V157_SPEC.md) |
| v0.158 | ✅ | Go DeleteOffsetsAll | [V158_SPEC.md](../V158_SPEC.md) |
| v0.159 | ✅ | Rust fetch_offsets_all | [V159_SPEC.md](../V159_SPEC.md) |
| v0.160 | ✅ | Go/Python/Rust producer id getters | [V160_SPEC.md](../V160_SPEC.md) |
| v0.161 | ✅ | Go ListAclsAll | [V161_SPEC.md](../V161_SPEC.md) |
| v0.162 | ✅ | Rust list_acls_all | [V162_SPEC.md](../V162_SPEC.md) |
| v0.163 | ✅ | Go ListOffsetsAll | [V163_SPEC.md](../V163_SPEC.md) |
| v0.164 | ✅ | language single-entry DeleteOffset | [V164_SPEC.md](../V164_SPEC.md) |
| v0.165 | ✅ | Rust delete_offsets_all + delete_offset | [V165_SPEC.md](../V165_SPEC.md) |
| v0.166 | ✅ | Rust list_offsets_all | [V166_SPEC.md](../V166_SPEC.md) |
| v0.167 | ✅ | Go ReassignAllPartitions | [V167_SPEC.md](../V167_SPEC.md) |
| v0.168 | ✅ | Rust reassign_partitions_all | [V168_SPEC.md](../V168_SPEC.md) |
| v0.169 | ✅ | language single-entry CreateAcl / DeleteAcl | [V169_SPEC.md](../V169_SPEC.md) |
| v0.170 | ✅ | Rust create_acl / delete_acl | [V170_SPEC.md](../V170_SPEC.md) |
| v0.171 | ✅ | Go AddBrokerNoRack | [V171_SPEC.md](../V171_SPEC.md) |
| v0.172 | ✅ | Rust add_broker_no_rack | [V172_SPEC.md](../V172_SPEC.md) |
| v0.173 | ✅ | Go CreateScramUserDefault | [V173_SPEC.md](../V173_SPEC.md) |
| v0.174 | ✅ | Rust create_scram_user_default | [V174_SPEC.md](../V174_SPEC.md) |
| v0.175 | ✅ | Rust commit_transaction_empty | [V175_SPEC.md](../V175_SPEC.md) |
| v0.176 | ✅ | Go CommitTransactionEmpty | [V176_SPEC.md](../V176_SPEC.md) |
| v0.177 | ✅ | language single-entry AlterConfig | [V177_SPEC.md](../V177_SPEC.md) |
| v0.178 | ✅ | Rust alter_config | [V178_SPEC.md](../V178_SPEC.md) |
| v0.179 | ✅ | language single-entry FetchOffset | [V179_SPEC.md](../V179_SPEC.md) |
| v0.180 | ✅ | Rust fetch_offset | [V180_SPEC.md](../V180_SPEC.md) |
| v0.181 | ✅ | language single-topic MetadataTopic | [V181_SPEC.md](../V181_SPEC.md) |
| v0.182 | ✅ | Rust metadata_topic | [V182_SPEC.md](../V182_SPEC.md) |
| v0.183 | ✅ | Go Addr getter | [V183_SPEC.md](../V183_SPEC.md) |
| v0.184 | ✅ | Go/Java GroupConsumer assignor getter | [V184_SPEC.md](../V184_SPEC.md) |
| v0.185 | ✅ | Go SetEnableIdempotence / Idempotence | [V185_SPEC.md](../V185_SPEC.md) |
| v0.186 | ✅ | Go GroupConsumer HeartbeatCount | [V186_SPEC.md](../V186_SPEC.md) |
| v0.187 | ✅ | Java GroupConsumer heartbeatCount | [V187_SPEC.md](../V187_SPEC.md) |
| v0.188 | ✅ | Python GroupConsumer heartbeat_count | [V188_SPEC.md](../V188_SPEC.md) |
| v0.189 | ✅ | Go/Java GroupConsumer sessionTimeoutMs getter | [V189_SPEC.md](../V189_SPEC.md) |
| v0.190 | ✅ | Go/Java GroupConsumer Leave alias | [V190_SPEC.md](../V190_SPEC.md) |
| v0.191 | ✅ | Go MaxRedirects getter | [V191_SPEC.md](../V191_SPEC.md) |
| v0.192 | ✅ | Go MaxRetries getter | [V192_SPEC.md](../V192_SPEC.md) |
| v0.193 | ✅ | Go RetryBackoff getter | [V193_SPEC.md](../V193_SPEC.md) |
| v0.194 | ✅ | Go TransactionalID getter | [V194_SPEC.md](../V194_SPEC.md) |
| v0.195 | ✅ | language Client timeout getter | [V195_SPEC.md](../V195_SPEC.md) |
| v0.196 | ✅ | Python list_acls_all | [V196_SPEC.md](../V196_SPEC.md) |
| v0.197 | ✅ | Python list_offsets_all | [V197_SPEC.md](../V197_SPEC.md) |
| v0.198 | ✅ | Python reassign_partitions_all | [V198_SPEC.md](../V198_SPEC.md) |
| v0.199 | ✅ | Go/Java CreateTopic default partitions=1 | [V199_SPEC.md](../V199_SPEC.md) |
| v0.200 | ✅ | Go/Java auth token getter | [V200_SPEC.md](../V200_SPEC.md) |
| v0.201 | ✅ | Java heartbeatIntervalMs public | [V201_SPEC.md](../V201_SPEC.md) |
| v0.202 | ✅ | Go/Java SCRAM username getter | [V202_SPEC.md](../V202_SPEC.md) |
| v0.203 | ✅ | Rust create_topic_default | [V203_SPEC.md](../V203_SPEC.md) |
| v0.207 | ✅ | language GroupConsumer SyncGroup peek after join | [V207_SPEC.md](../V207_SPEC.md) |
| v0.208 | ✅ | Rust GroupConsumer SyncGroup peek after join | [V208_SPEC.md](../V208_SPEC.md) |
| v0.209 | ✅ | language first-Join client member_id | [V209_SPEC.md](../V209_SPEC.md) |
| v0.210 | ✅ | Rust first-Join client member_id | [V210_SPEC.md](../V210_SPEC.md) |
| v0.211 | ✅ | JoinGroup members trailer for range | [V211_SPEC.md](../V211_SPEC.md) |
| v0.212 | ✅ | persist membership overlay after openraft joint | [V212_SPEC.md](../V212_SPEC.md) |
| v0.213 | ✅ | IsrUpdate skips homemade 154 when openraft on | [V213_SPEC.md](../V213_SPEC.md) |
| v0.214 | ✅ | gate inbound homemade 154 + lazy raft dir | [V214_SPEC.md](../V214_SPEC.md) |
| v0.215 | ✅ | SyncGroup generation confirm fence | [V215_SPEC.md](../V215_SPEC.md) |
| v0.216 | ✅ | overlay apply artifact from Membership log | [V216_SPEC.md](../V216_SPEC.md) |
| v0.217 | ✅ | in-process add/remove persist after openraft joint | [V217_SPEC.md](../V217_SPEC.md) |
| v0.218 | ✅ | CompletingRebalance group state while fence open | [V218_SPEC.md](../V218_SPEC.md) |
| v0.219 | ✅ | OffsetCommit 9 until SyncGroup confirms | [V219_SPEC.md](../V219_SPEC.md) |
| v0.220 | ✅ | language GroupConsumer Join 9 retry | [V220_SPEC.md](../V220_SPEC.md) |
| v0.221 | ✅ | Rust GroupConsumer Join 9 retry | [V221_SPEC.md](../V221_SPEC.md) |
| v0.222 | ✅ | delete homemade 154 hatch; keep 98/99 decode | [V222_SPEC.md](../V222_SPEC.md) |
| v0.223 | ✅ | language Client.join_group retries error 9 | [V223_SPEC.md](../V223_SPEC.md) |
| v0.224 | ✅ | Rust Client.join_group retries error 9 | [V224_SPEC.md](../V224_SPEC.md) |
| v0.225 | ✅ | Kafka AlterPartitionReassignments key 45 v0 | [V225_SPEC.md](../V225_SPEC.md) |
| v0.226 | ✅ | opt-in txn-state topic records open≡abort | [V226_SPEC.md](../V226_SPEC.md) |
| v0.227 | ✅ | park Join until SyncGroup or session timeout | [V227_SPEC.md](../V227_SPEC.md) |
| v0.228 | ✅ | Kafka ListPartitionReassignments key 46 v0 | [V228_SPEC.md](../V228_SPEC.md) |
| v0.229 | ✅ | Kafka TransactionLog schemas on txn-state topic | [V229_SPEC.md](../V229_SPEC.md) |
| v0.230 | ✅ | PreparingRebalance while Join is parked | [V230_SPEC.md](../V230_SPEC.md) |
| v0.231 | ✅ | Join park uses rebalance timeout, not session | [V231_SPEC.md](../V231_SPEC.md) |
| v0.232 | ✅ | write open/prepared partitions on txn-state log | [V232_SPEC.md](../V232_SPEC.md) |
| v0.233 | ✅ | Kafka Describe/AlterUserScramCredentials 50/51 | [V233_SPEC.md](../V233_SPEC.md) |
| v0.234 | ✅ | native Fetch honors group assignment trailer | [V234_SPEC.md](../V234_SPEC.md) |
| v0.235 | ✅ | Kafka DescribeLogDirs key 35 v0–1 | [V235_SPEC.md](../V235_SPEC.md) |
| v0.236 | ✅ | Kafka ElectLeaders key 43 v0–1 | [V236_SPEC.md](../V236_SPEC.md) |
| v0.237 | ✅ | Kafka DescribeTopicPartitions key 75 v0 | [V237_SPEC.md](../V237_SPEC.md) |
| v0.238 | ✅ | native SCRAM-SHA-512 handshake trailer | [V238_SPEC.md](../V238_SPEC.md) |
| v0.239 | ✅ | native ListOffsets timestamp trailer | [V239_SPEC.md](../V239_SPEC.md) |
| v0.240 | ✅ | native ListOffsets isolation trailer | [V240_SPEC.md](../V240_SPEC.md) |
| v0.241 | ✅ | Kafka Describe/AlterClientQuotas 48/49 | [V241_SPEC.md](../V241_SPEC.md) |
| v0.242 | ✅ | Kafka UnregisterBroker key 64 v0 | [V242_SPEC.md](../V242_SPEC.md) |
| v0.243 | ✅ | warn once if leftover __metadata_raft dir exists | [V243_SPEC.md](../V243_SPEC.md) |
| v0.244 | ✅ | Kafka UpdateFeatures key 57 reject | [V244_SPEC.md](../V244_SPEC.md) |
| v0.245 | ✅ | Kafka DescribeQuorum key 55 v0–1 | [V245_SPEC.md](../V245_SPEC.md) |
| v0.246 | ✅ | Kafka AllocateProducerIds key 67 v0 | [V246_SPEC.md](../V246_SPEC.md) |
| v0.247 | ✅ | ACL TransactionalId on txn APIs | [V247_SPEC.md](../V247_SPEC.md) |
| v0.248 | ✅ | apply SyncGroup assignment when it decodes | [V248_SPEC.md](../V248_SPEC.md) |
| v0.249 | ✅ | Kafka AlterReplicaLogDirs key 34 reject | [V249_SPEC.md](../V249_SPEC.md) |

---

## Still deferred (post–v0.10)

- Full openraft / KRaft (assignment gen majority MVP → **closed by Phase 150**; Metadata lead residual → **closed by Phase 152**; membership overlay → **v0.10**; homemade 154 hatch **deleted v0.222**)

---

## Still deferred (post–Phase 154)

- Full openraft election + InstallSnapshot (metadata log MVP → **closed by Phase 154**; hatch **deleted v0.222**; membership overlay → **v0.10**; leftover `__metadata_raft/` unread)
- Full Kafka Streams EOS / multi-worker 2PC (stream EOS MVP → **closed by Phase 151**; checkpoint → **153**; app fence → **v0.8**; changelog in txn → **v0.9**; residual: one-process topology)
- ~~Metadata gated exclusively on `committed_generation` (150 residual)~~ → **closed by Phase 152**
- ~~EOS + durable state single atomic boundary (151 residual)~~ → **closed by Phase 153** (process-local staging; changelog opt-in **v0.9**)
- Multi-worker stream assignment / broker-held state machine


- Multi-language clients
- Chaos-mesh / long fuzz campaigns (corpus **smoke CI** → **closed by Phase 112**)
- Full KIP-890/939 / Kafka `__transaction_state` topic (multi-broker Enable2Pc MVP → **closed by Phase 114**; Kafka TransactionLog v0 → **v0.229**; not TV2 / default-on)
- Multi-broker session handoff / affinity routing (durable **local** → **115**; owner forward MVP → **closed by Phase 119**; preferred-replica MVP → **closed by Phase 126**; shared mirror + promote MVP → **closed by Phase 138**; mirror polish → **closed by Phase 139**; promote claim fence → **closed by Phase 143**; preferred × session suppress → **closed by Phase 144**; rack-aware create assignment → **closed by Phase 145**; residual: Raft registry / serve-without-promote / incremental put / full preferred throttle)
- Multi-broker session handoff / affinity routing (durable **local** → **115**; owner forward MVP → **closed by Phase 119**; preferred-replica MVP → **closed by Phase 126**; shared mirror + promote MVP → **closed by Phase 138**; mirror polish → **closed by Phase 139**; promote claim fence → **closed by Phase 143**; preferred × session suppress → **closed by Phase 144**; serve-without-promote → **closed by Phase 147**; residual: Raft registry / dual-epoch converge / incremental put / full preferred selector)
- Byte-identical response cache beyond HWM+LSO omit
- Full KRaft epoch state machine / remote-log epochs
- Full Kafka broker catalog / KRaft DynamicBrokerConfig
- Drain native / Kafka / metrics accept loops on shutdown → **closed by Phase 109**
- Multi-broker BROKER config fan-out → **closed by Phase 113** (controller push; homogeneous knobs)
- DeleteRecords follower fan-out → **closed by Phase 113** (best-effort)
- Durable pending DeleteRecords for offline replicas → **closed by Phase 116** (leader outbox + retry)
- Cluster ACL snapshot fan-out → **closed by Phase 113** (controller SoT; not Raft consensus)
- Controller failover / rejoin catch-up for ACL + BROKER config → **closed by Phase 117** (durable gens + heartbeat lag re-push; not Raft)
- Single-flight / idempotent `start_background_tasks` → **closed by Phase 109**
- Non-controller auto-death from alive-set diffs → **closed by Phase 110**
- ISR rejoin after follower recovery + lag-based ISR shrink → **closed by Phase 118**
- Straddle marker clip → **closed by Phase 111**
- cargo-fuzz corpus smoke + CI MVP → **closed by Phase 112**
- Multi-broker Enable2Pc prepare/complete fan-out → **closed by Phase 114**
- Durable local fetch sessions → **closed by Phase 115**
- Multi-broker fetch session forward (owner-encoded id) → **closed by Phase 119**
- Transparent EndTxn forward to txn coordinator → **closed by Phase 120**
- Hash-based sticky FindCoordinator → **closed by Phase 121**
- Transparent AddOffsetsToTxn / TxnOffsetCommit forward → **closed by Phase 122**
- Outbox handoff on leadership change → **closed by Phase 123** (new leader reconcile from log_start; not consensus truncate log)
- Durable Init-owner txn coordinator registry → **closed by Phase 124** (local `__txn_coordinator`; not `__transaction_state`)
- Time-based ISR lag shrink → **closed by Phase 125** (last-caught-up + `replica_lag_max_ms`; not full Kafka replica.lag.time.max.ms)
- PreferredReadReplica / rack-aware Fetch → **closed by Phase 126** (KIP-392 subset; not full selector/throttling; preferred residual orthogonal to Phase 138/139 mirror)
- Preferred selector polish (usable addr + LEO ranking) → **closed by Phase 133**
- Preferred max LEO lag + RC suppress metric → **closed by Phase 140** (still not full Kafka selector/throttling)
- N=2 majority ops / health gauges → **closed by Phase 141** (majority algorithm still configured-N; no live-only flip)
- Metadata ISR lag when leader ≠ controller → **closed by Phase 142** (leader overlay + best-effort IsrUpdate 94/95)
- Shared fetch session mirror + promote → **closed by Phase 138** (best-effort peer mirror; not Raft; no session_id re-encode)
- Session mirror polish (debounce/durable/fence) → **closed by Phase 139** (coalesce + min-interval Puts; optional durable; `mirror_gen`)
- Promote claim fence (lowest-id) → **closed by Phase 143** (best-effort MirrorPut claim; not Raft; brief dual primary until exchange)
- Serve-from-mirror without promote → **closed by Phase 147** (default on owner miss; dual-epoch residual; promote via `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS`)
- Txn coordinator registry TTL GC → **closed by Phase 127** (default 24h; `0` disables; not eager EndTxn GC)
- BROKER config for registry TTL → **closed by Phase 128** (`volant.txn.coordinator.registry.ttl.ms`; env still works; not full DynamicBrokerConfig)
- Per-broker BROKER config overrides / multi-master ACL merge
- Consensus truncate log / controller SoT DeleteRecords journal → **closed by Phase 129** (controller SoT journal MVP; not Raft)
- Multi-controller majority truncate journal consensus → **closed by Phase 130** (Raft-style majority note; not full openraft/KRaft)
- Truncate journal rejoin catch-up / heartbeat lag re-push → **closed by Phase 131** (`applied_journal_generation` + TruncateJournalPush; not Raft)
- Journal catch-up stall / throttle / single-flight hardening → **closed by Phase 132**
- Request-level DeleteRecords majority-wait flag → **closed by Phase 137** (native trailer; Kafka still env/broker only)
- Truncate-journal topic prune on assignment remove + push anti-resurrection → **closed by Phase 137**
- Rollback local truncate on majority fail (still open)
- Peer-to-peer heartbeat mesh → **closed by Phase 134**
- Sync client wait on DeleteRecords majority → **closed by Phase 135** (opt-in; default best-effort)
- Full Kafka preferred-replica selector / throttling (beyond 126/133/140/144; rack-aware create assignment → **closed by Phase 145**)
- Durable stream state store (in-process) → **closed by Phase 149** (`DurableStore` / redb; not distributed workers)
- Stream exactly-once (txn produce + group offset commit) → **closed by Phase 151** (not full KS EOS; durable state not in same txn)
- Durable stream state store (in-process) → **closed by Phase 149** (`DurableStore` / redb; not exactly-once; not distributed workers)
- Broker consensus / openraft → Phase 150 notes + Phase 154 metadata log MVP (not full openraft election; Metadata lead residual → **closed by Phase 152**)
