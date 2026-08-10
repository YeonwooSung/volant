# Phase history index

Ship records for **phases 0–136** (shipped). Binding core contracts are
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

---

## Still deferred (post–Phase 136)

- Multi-language clients
- Chaos-mesh / long fuzz campaigns (corpus **smoke CI** → **closed by Phase 112**)
- Full KIP-890/939 / Kafka `__transaction_state` topic (multi-broker Enable2Pc MVP → **closed by Phase 114**)
- Multi-broker session handoff / affinity routing (durable **local** → **115**; owner forward MVP → **closed by Phase 119**; preferred-replica MVP → **closed by Phase 126**; shared store still open)
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
- PreferredReadReplica / rack-aware Fetch → **closed by Phase 126** (KIP-392 subset; not full selector/throttling; shared session store still open)
- Txn coordinator registry TTL GC → **closed by Phase 127** (default 24h; `0` disables; not eager EndTxn GC)
- BROKER config for registry TTL → **closed by Phase 128** (`volant.txn.coordinator.registry.ttl.ms`; env still works; not full DynamicBrokerConfig)
- Per-broker BROKER config overrides / multi-master ACL merge
- Consensus truncate log / controller SoT DeleteRecords journal → **closed by Phase 129** (controller SoT journal MVP; not Raft)
- Multi-controller majority truncate journal consensus → **closed by Phase 130** (Raft-style majority note; not full openraft/KRaft)
- Truncate journal rejoin catch-up / heartbeat lag re-push → **closed by Phase 131** (`applied_journal_generation` + TruncateJournalPush; not Raft)
- Journal catch-up stall / throttle / single-flight hardening → **closed by Phase 132**
- Preferred selector polish (usable addr + LEO ranking) → **closed by Phase 133**
- Peer-to-peer heartbeat mesh → **closed by Phase 134**
- Sync client wait on DeleteRecords majority → **closed by Phase 135** (opt-in; default best-effort)
- Full Kafka preferred-replica selector / rack-aware partition assignment
