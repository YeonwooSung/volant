# Kafka compatibility matrix

**Living document** for the optional Kafka wire shim (`--kafka-listen`).
Ship history: Phases **23–105** (git HEAD product). Binding deep dives:
`PHASE23_SPEC.md` … `PHASE105_SPEC.md`. Overview: [WHITEPAPER.md](./WHITEPAPER.md).
Semantic rows below describe **shipped** behavior.

## Enable

```bash
volant-server \
  --data-dir ./data \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093
```

Native Volant protocol remains on `--listen`. Shared-token Auth does **not**
apply on the Kafka port (use SASL or anonymous + ACLs).

## Supported APIs (current)

From `SUPPORTED_APIS` in `crates/volant-broker/src/kafka/mod.rs`:

| Key | API | Versions | Notes |
|----:|-----|----------|-------|
| 0 | Produce | 0–13 | Classic 0–8; flex v9+; TopicId v13; KIP-951 CurrentLeader v10+; txn 123 after timeout (Phase 94) |
| 1 | Fetch | 0–18 | Classic 0–11; flex v12–18; TopicId v13+; isolation LSO/aborted (Phase 86); ReplicaState v15+ ignore; NodeEndpoints v16+; CurrentLeader tag v12+; DivergingEpoch + sessions (Phase 88); omit-unchanged incremental (Phase 91); session idle TTL + max/LRU (Phase 95); durable local sessions (Phase 115); multi-broker session forward (Phase 119) |
| 2 | ListOffsets | 0–11 | Flex v6+; specials v7–11; READ_COMMITTED latest = LSO (Phase 86) |
| 3 | Metadata | 0–13 | Flex v9+; TopicId v10–13; top-level ErrorCode v13; live leader_epoch (Phase 87) |
| 8 | OffsetCommit | 0–10 | Flex v8+; TopicId v10 |
| 9 | OffsetFetch | 0–10 | Flex v6+; multi-group v8; TopicId v10 |
| 10 | FindCoordinator | 0–6 | Flex v3; batch v4–6; sticky murmur2 over static membership (Phase 121); txn Init-owner override; no share key_type; never TRANSACTION_ABORTABLE |
| 11 | JoinGroup | 0–9 | Flex v6+; ProtocolType/Reason/SkipAssignment v7–9 |
| 12 | Heartbeat | 0–4 | Flex v4 |
| 13 | LeaveGroup | 0–5 | Flex v4+; Reason v5 |
| 14 | SyncGroup | 0–5 | Flex v4+; ProtocolType/Name v5 |
| 15 | DescribeGroups | 0–6 | Flex v5; ErrorMessage v6 |
| 16 | ListGroups | 0–5 | Flex v3; StatesFilter v4; TypesFilter v5 (`classic`) |
| 17 | SaslHandshake | 0–1 | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 |
| 18 | ApiVersions | 0–5 | Flex v3–5; header always v0; empty feature tags; v5 ClusterId/NodeId ignored |
| 19 | CreateTopics | 0–7 | Flex v5+; TopicId response v7 |
| 20 | DeleteTopics | 0–6 | Flex v4+; ErrorMessage v5; TopicId v6 |
| 21 | DeleteRecords | 0–2 | Flex v2; GC/clip aborted soft markers vs log start (Phase 104/111); best-effort replica fan-out (Phase 113) + durable leader outbox retry for offline peers (Phase 116) + new-leader outbox reconcile on leadership change (Phase 123) |
| 22 | InitProducerId | 0–6 | Flex v2+; v6 Enable2Pc/KeepPreparedTxn (Phase 90 prepared MVP); OngoingTxn* when prepared |
| 23 | OffsetForLeaderEpoch | 0–4 | Flex v4; durable epoch history MVP (Phase 87); prior epochs → transition end |
| 24 | AddPartitionsToTxn | 0–5 | Flex v3; batch v4–5; 123 after timeout (Phase 94) |
| 25 | AddOffsetsToTxn | 0–4 | Flex v3+; v4 = v3 wire; may emit TRANSACTION_ABORTABLE (123) after timeout (Phase 94); multi-broker transparent forward to Init owner (Phase 122) |
| 26 | EndTxn | 0–5 | Flex v3+; v5 pid/epoch echo; 123 after timeout auto-abort (Phase 94); multi-broker transparent forward (Phase 120) |
| 28 | TxnOffsetCommit | 0–6 | Flex v3+; TopicId v6; 123 after timeout when no open (Phase 94); multi-broker transparent forward (Phase 122) |
| 29–31 | ACL admin | 0–3 | Flex v2+; User resource v3; LITERAL only; cluster Create/Delete **controller-only** + snapshot fan-out (Phase 113; **41** NotController) |
| 32 | DescribeConfigs | 0–4 | Flex v4; TOPIC + BROKER (Phase 99–103; name empty or local `node_id`; sparse durable; cluster effective values after Phase 113 push) |
| 33 | AlterConfigs | 0–2 | Flex v2; TOPIC + BROKER SET (empty = product default; name check Phase 103; sparse durable Phase 100/102; BROKER cluster Alter **controller-only** Phase 113 → **41**) |
| 36 | SaslAuthenticate | 0–2 | Flex v2 |
| 37 | CreatePartitions | 0–3 | Flex v2+; v3 = v2 wire (no KIP-599 quota) |
| 42 | DeleteGroups | 0–3 | Flex v2; ErrorMessage v3 |
| 44 | IncrementalAlterConfigs | 0–1 | SET/DELETE only; TOPIC + BROKER (Phase 99–103 name check + sparse durable; BROKER cluster Alter controller-only Phase 113) |
| 47 | OffsetDelete | 0 | Classic only |
| 60 | DescribeCluster | 0–2 | Always flex; IsFenced always false |
| 61 | DescribeProducers | 0 | Always flex |
| 65 | DescribeTransactions | 0 | Always flex |
| 66 | ListTransactions | 0–2 | Pattern = simple `*` glob |

## Wire evolution (summary)

| Band | Phases | Theme |
|------|--------|-------|
| MVP | 23–27 | Produce/Fetch/Metadata/groups/admin surface |
| Formats | 28–34 | Compression, idempotence, SASL, SCRAM-512 |
| Classic max | 35–50 | Version ratchets for classic framing |
| Flexible | 51–66 | KIP-482 compact + modern admin |
| TopicId / modern | 67–85 | UUID topics, ListOffsets specials, KIP-890, KIP-951, group admin, CreatePartitions v3, FindCoordinator v5–6, AddOffsetsToTxn v4, ApiVersions 0–5, Fetch 0–18, ACL admin User resource v3 |
| READ_COMMITTED | 86 | Write-through txn + soft abort markers; true LSO; Fetch isolation filtering |
| Leader epochs | 87 | Durable OffsetForLeaderEpoch history MVP; Metadata live leader_epoch |
| Fetch sessions / DivergingEpoch | 88 | Sessions (create/forgotten/invalid); DivergingEpoch tag 0 on truncation; durable local Phase 115 |
| Control batches | 89 | EndTxn COMMIT/ABORT control RecordBatches on partition log (dual-write with soft markers) |
| Prepared 2PC MVP | 90 | Enable2Pc prepare-then-complete EndTxn; KeepPreparedTxn + OngoingTxn*; durable `__txn_prepared` |
| Omit-unchanged sessions | 91 | Empty-topics incremental omits partitions when HWM+LSO unchanged and records empty |
| Prepared timeout | 92 | Lazy auto-abort of prepared txns after configurable timeout (default 60s) |
| Open txn timeout | 93 | Honor InitProducerId `transaction_timeout_ms` (or broker default) for open write-through txns; lazy auto-abort |
| TRANSACTION_ABORTABLE | 94 | Honest subset: emit 123 after open/prepared timeout on Produce/EndTxn/Add*/TxnOffsetCommit; FindCoordinator never |
| Fetch session TTL / max | 95 | Idle TTL (default 60s) + max concurrent sessions (default 1000, LRU); lazy eviction |
| Durable fetch sessions | 115 | Per-broker `{data_dir}/__fetch_sessions`; restart restores session_id/epoch/omit cache within idle TTL |
| Multi-broker session handoff | 119 | Owner-encoded session_id + transparent Kafka Fetch forward (opcode 82/83); omit/epoch SoT remains owner; dead owner ⇒ 70 |
| Transaction max timeout | 96 | Broker max (default 15m / `VOLANT_TRANSACTION_MAX_TIMEOUT_MS`); InitProducerId rejects over-max with **50**; effective open/prepared clamp |
| Background sweeper + metrics | 97 | Periodic open/prepared/session idle expiry (default 1s / `VOLANT_SWEEP_INTERVAL_MS`; `0` pauses bg); lazy paths remain; expired counters + open/prepared gauges |
| Crash≡abort control | 98 | Open→aborted promote appends ABORT control batches (dual-write) |
| Broker DescribeConfigs knobs | 99 | BROKER resource: `transaction.max.timeout.ms` + `volant.*` open/prepared/session/sweep; Alter + Incremental SET/DELETE |
| Durable broker config | 100 | Full snapshot of six knobs under `{data_dir}/__broker_config/state.json`; load after env; DELETE rewrites file |
| Graceful sweeper enable | 101 | Always spawn sweeper task; `0` pauses; `0→>0` via setter/Alter enables without process restart |
| Background shutdown join | 106 | `BackgroundTasks` stop+join for group/retention/sweeper/cluster loops; server drains on exit/signal |
| Accept drain + single-flight bg | 109 | Native/Kafka/metrics accept loops select shutdown + abort connections (bounded); `start_background_tasks` single-flight per broker |
| Soft-marker GC | 104 | DeleteRecords / retention / load drop aborted soft markers with `end_offset <= log_start`; persist `__txn_markers`; metric `volant_aborted_markers_gc_total` |
| Empty-AddPartitions control | 105 | AddPartitions membership tracked; EndTxn + crash promote append control for empty partitions (no fake soft ranges) |

## Semantic honesty (open)

These are **current** product facts, not temporary docs lag:

| Area | Limitation |
|------|------------|
| Transactions | **Write-through** (Phase 86) + **control batches** (Phase 89/98/105) + **prepared 2PC MVP** (Phase 90) + **prepared timeout** (Phase 92) + **open timeout** (Phase 93) + **TRANSACTION_ABORTABLE subset** (Phase 94) + **max timeout clamp** (Phase 96) + **background sweeper** (Phase 97/101/106) + **broker config surface** (Phase 99) + **sparse durable restart** (Phase 100/102) + **BROKER name vs `node_id`** (Phase 103) + **soft-marker GC/clip** (Phase 104/111): data on log immediately; soft markers still SoT for LSO/aborted list; markers with `end_offset <= log_start` dropped on DeleteRecords / retention / load; straddlers clip `first_offset = log_start` (Phase 111; control batches on log not rewritten); open crash≡abort **with ABORT control batches** (Phase 98) including **empty AddPartitions membership** (Phase 105, control-only); prepared survives restart under `__txn_prepared` until complete or timeout auto-abort; EndTxn control batches on finalize for written **and** empty added partitions; open-txn honors InitProducerId `transaction_timeout_ms` (or `VOLANT_OPEN_TXN_TIMEOUT_MS` default 60s); broker max default **15m** (`VOLANT_TRANSACTION_MAX_TIMEOUT_MS`; `0` = no max) rejects over-max Init with **50** and clamps effective open/prepared; after timeout auto-abort Produce/EndTxn/Add*/TxnOffsetCommit may return **123** until EndTxn clears; open/prepared expiry runs lazy **and** on a background interval (default 1s / `VOLANT_SWEEP_INTERVAL_MS`; `0` = pause bg; always-spawn so 0→>0 without restart, Phase 101; graceful shutdown/join Phase 106); knobs Describe/Alter via BROKER resource (Phase 99); name must be empty or local `node_id` decimal else **42** (Phase 103); Alter persists **sparse** under `__broker_config` (Phase 100/102; only altered keys; DELETE unfreezes env) |
| 2PC | **MVP** (Phase 90/92/94/96/97/**114**/**120**/**121**/**122**): Enable2Pc → first EndTxn prepares; second matching EndTxn finalizes; KeepPreparedTxn + OngoingTxn*; prepared timeout auto-abort (default 60s, env `VOLANT_PREPARED_TXN_TIMEOUT_MS`, clamped by max); timeout → abortable mark → Kafka **123** (Phase 94); background + lazy expiry (Phase 97); **multi-broker** prepare/complete fan-out across partition leaders (Phase 114; controller cluster prepared index; inter-broker opcodes 76–81); **transparent EndTxn / AddOffsets / TxnOffsetCommit forward** to Init-owner coordinator when client hits another broker (Phase 120/122; opcodes 84/85; no dual prepare / dual offset buffer); **sticky FindCoordinator** for group/txn keys (Phase 121; murmur2 static ring + Init-owner override); **not** full KIP-890/939 / Kafka `__transaction_state` topic |
| Epochs | **Durable history MVP** (Phase 87): `{data_dir}/__leader_epochs`; prior epochs return transition end offsets; Metadata advertises live epoch; not a full KRaft epoch state machine; Fetch **DivergingEpoch** on truncation (Phase 88) |
| TopicId | Deterministic UUID from Volant id (`volant` + zeros + u32), not KRaft random |
| Groups | Coordinator-driven assignment; GroupType always `classic`; states Stable/Empty |
| Fetch sessions | **Real MVP** (Phase 88 + **91** + **95** + **97** + **99** + **100** + **102** + **103** + **115** + **119**): create/merge/forgotten; empty-topics re-fetch; errors 70/71; **omit-unchanged** when HWM+LSO unchanged and records empty; **idle TTL** (default 60s / `VOLANT_FETCH_SESSION_IDLE_MS`) + **max sessions** (default 1000 / `VOLANT_FETCH_SESSION_MAX`, LRU at cap); idle also background-swept (Phase 97); knobs on BROKER Describe/Alter (Phase 99) with **sparse** durable restart (Phase 100/102) and name vs `node_id` (Phase 103); **session table durable** under `{data_dir}/__fetch_sessions` (Phase 115); **multi-broker handoff** (Phase 119): cluster session_id embeds owner; non-owner transparent-forwards Fetch to owner over inter-broker (no dual epoch); unreachable owner ⇒ **70**; not preferred-replica / not a shared session store |
| Preferred replica | Always -1 |
| ISR / HWM (cluster) | Kafka-style static ISR (Phase 6): death shrink + HWM recompute (Phase 108/110); **rejoin** when ReplicaFetch LEO ≥ HWM and lag ≤ `replica_lag_max_messages`; lag-shrink of slow-but-alive members (Phase 118); Metadata ISR may lag when leader ≠ controller |
| CreateTopics | Replica assignment arrays ignored; configs response often null |
| Storage | Log stores uncompressed Volant records; Fetch re-encodes |
| Auth | Kafka port: SASL or `kafka-anonymous`; no shared-token on Kafka port |
| ACLs | LITERAL only; host always `*`; User resource (v3) stored only (no SCRAM-admin gating); no TransactionalId/DelegationToken; cluster Create/Delete are **controller-only** with generationed snapshot fan-out (Phase 113) + durable-gen rejoin catch-up (Phase 117) — not Raft multi-master consensus |
| BROKER config (cluster) | Alter / IncrementalAlter for the six Phase 99 knobs: **controller-only** (Kafka **41** NotController elsewhere); push to live peers; Describe on any node returns **local** effective values after apply; offline peers catch up on heartbeat re-push (Phase 117; not Raft) |
| DeleteRecords (cluster) | Leader-only client path; best-effort inter-broker truncate of other replicas (Phase 113); fan-out failure does not fail the client; failed peers recorded in leader-local durable outbox and retried when live (Phase 116); **Phase 123** new leader rebuilds pending targets from local `log_start` after leadership change (not a consensus truncate log) |
| KIP-951 | CurrentLeader on leader errors; Produce NodeEndpoints v10+; Fetch NodeEndpoints v16+; empty tags on success |
| ApiVersions features | Empty SupportedFeatures / FinalizedFeatures / ZkMigrationReady tags; no REBOOTSTRAP_REQUIRED |
| Missing APIs | Large Kafka surface still unsupported (GSSAPI, OAUTH, broker configs, …) |

## Related

- [ops.md](./ops.md) — Kafka listen ops notes  
- [features.md](./features.md) — native txn/ACL/SCRAM behavior  
- [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) — phase index  
