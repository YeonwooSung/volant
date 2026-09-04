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
| 1 | Fetch | 0–18 | Classic 0–11; flex v12–18; TopicId v13+; isolation LSO/aborted (Phase 86); ReplicaState v15+ (ReplicaId gates preferred redirect); NodeEndpoints v16+; CurrentLeader tag v12+; DivergingEpoch + sessions (Phase 88); omit-unchanged incremental (Phase 91); session idle TTL + max/LRU (Phase 95); durable local sessions (Phase 115); multi-broker session forward (Phase 119); best-effort peer mirror (Phase 138) + polish (139) + claim fence (143) + serve-from-mirror without promote (Phase 147 default; not Raft); PreferredReadReplica 126+133+140+144 |
| 2 | ListOffsets | 0–11 | Flex v6+; specials v7–11; READ_COMMITTED latest = LSO (Phase 86) |
| 3 | Metadata | 0–13 | Flex v9+; TopicId v10–13; top-level ErrorCode v13; live leader_epoch (Phase 87) |
| 8 | OffsetCommit | 0–10 | Flex v8+; TopicId v10 |
| 9 | OffsetFetch | 0–10 | Flex v6+; multi-group v8; TopicId v10 |
| 10 | FindCoordinator | 0–6 | Flex v3; batch v4–6; sticky murmur2 over static membership (Phase 121); txn Init-owner override; no share key_type; never TRANSACTION_ABORTABLE |
| 11 | JoinGroup | 0–9 | Flex v6+; ProtocolType/Reason/SkipAssignment v7–9 |
| 12 | Heartbeat | 0–4 | Flex v4 |
| 13 | LeaveGroup | 0–5 | Flex v4+; Reason v5 |
| 14 | SyncGroup | 0–5 | Flex v4+; ProtocolType/Name v5. Native opcode **116/117** is peek/confirm of the Join assignment (broker ignores leader bytes). Kafka key **14** is unchanged. |
| 15 | DescribeGroups | 0–6 | Flex v5; ErrorMessage v6 |
| 16 | ListGroups | 0–5 | Flex v3; StatesFilter v4; TypesFilter v5 (`classic`) |
| 17 | SaslHandshake | 0–1 | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 |
| 18 | ApiVersions | 0–5 | Flex v3–5; header always v0; empty feature tags; v5 ClusterId/NodeId ignored |
| 19 | CreateTopics | 0–7 | Flex v5+; TopicId response v7; assignment wait/rollback same as native (majority miss → **19**) |
| 20 | DeleteTopics | 0–6 | Flex v4+; ErrorMessage v5; TopicId v6; assignment wait/rollback same as native (majority miss → **19**) |
| 21 | DeleteRecords | 0–2 | Flex v2; GC/clip aborted soft markers vs log start (Phase 104/111); best-effort replica fan-out (Phase 113) + durable leader outbox retry for offline peers (Phase 116) + new-leader outbox reconcile on leadership change (Phase 123); optional journal majority wait via broker env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` (Phase 135 — fail → Kafka **19**); Volant **v2 request-level tag 0** = `wait_majority` u8 (0/1/2; **not** a Kafka standard field — librdkafka will not send it); v0–1 stay env/broker only |
| 22 | InitProducerId | 0–6 | Flex v2+; v6 Enable2Pc/KeepPreparedTxn (Phase 90 prepared MVP); OngoingTxn* when prepared |
| 23 | OffsetForLeaderEpoch | 0–4 | Flex v4; durable epoch history MVP (Phase 87); prior epochs → transition end |
| 24 | AddPartitionsToTxn | 0–5 | Flex v3; batch v4–5; 123 after timeout (Phase 94) |
| 25 | AddOffsetsToTxn | 0–4 | Flex v3+; v4 = v3 wire; may emit TRANSACTION_ABORTABLE (123) after timeout (Phase 94); multi-broker transparent forward to Init owner (Phase 122) |
| 26 | EndTxn | 0–5 | Flex v3+; v5 pid/epoch echo; 123 after timeout auto-abort (Phase 94); multi-broker transparent forward (Phase 120) |
| 28 | TxnOffsetCommit | 0–6 | Flex v3+; TopicId v6; 123 after timeout when no open (Phase 94); multi-broker transparent forward (Phase 122) |
| 29–31 | ACL admin | 0–3 | Flex v2+; User resource v3; LITERAL only; cluster Create/Delete **controller-only** + snapshot fan-out (Phase 113; **41** NotController) |
| 32 | DescribeConfigs | 0–4 | Flex v4; TOPIC + BROKER (Phase 99–103; name empty or local `node_id`; sparse durable; cluster effective values after Phase 113 push) |
| 33 | AlterConfigs | 0–2 | Flex v2; TOPIC + BROKER SET (empty = product default; name check Phase 103; sparse durable Phase 100/102; BROKER cluster Alter **controller-only** Phase 113 → **41**) |
| 35 | DescribeLogDirs | 0–1 | Flex v1; local logs only; size = `Log::total_size`; offsetLag = LEO−HWM (0 if unknown); isFuture false; not multi-log.dirs |
| 36 | SaslAuthenticate | 0–2 | Flex v2 |
| 37 | CreatePartitions | 0–3 | Flex v2+; v3 = v2 wire (no KIP-599 quota); assignment wait/rollback same as native (majority miss → **19**) |
| 42 | DeleteGroups | 0–3 | Flex v2; ErrorMessage v3 |
| 43 | ElectLeaders | 0–1 | Classic 0; flex v1; preferred = `elect_leader(ISR∩live)`; unclean type 1 → **87**; TimeoutMs ignored; assignment wait/rollback same as reassign; not live copy / not `preferred.leader` |
| 44 | IncrementalAlterConfigs | 0–1 | SET/DELETE only; TOPIC + BROKER (Phase 99–103 name check + sparse durable; BROKER cluster Alter controller-only Phase 113) |
| 45 | AlterPartitionReassignments | 0 | Always flex; wraps native opcode 114 + assignment wait; TimeoutMs ignored; null replicas → **83** (no pending cancel log); not live copy |
| 46 | ListPartitionReassignments | 0 | Always flex; current assignment as `replicas`; empty `addingReplicas`/`removingReplicas`; TimeoutMs ignored; not live progress; no pending log |
| 47 | OffsetDelete | 0 | Classic only |
| 50 | DescribeUserScramCredentials | 0 | Always flex; wraps `ScramStore`; empty users = all; unknown user → **91**; Cluster DESCRIBE |
| 51 | AlterUserScramCredentials | 0 | Always flex; wraps `ScramStore`; upsert takes `saltedPassword` (not plaintext); Cluster ALTER |
| 60 | DescribeCluster | 0–2 | Always flex; IsFenced always false |
| 61 | DescribeProducers | 0 | Always flex |
| 65 | DescribeTransactions | 0 | Always flex |
| 66 | ListTransactions | 0–2 | Pattern = simple `*` glob |
| 75 | DescribeTopicPartitions | 0 | Always flex; wraps Metadata (same leaders/ISR/epochs/TopicId); no ELR; simple `responsePartitionLimit` truncate; cursor start if topic is in the set else ignored (v0.237) |

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
| Multi-broker session handoff | 119 | Owner-encoded session_id + transparent Kafka Fetch forward (opcode 82/83); omit/epoch SoT remains owner while alive |
| Shared session mirror + promote | 138 | Best-effort peer MirrorPut/Delete (opcodes 90–93); foreign mirror table; put lag/fail still **70**; not Raft; no session_id re-encode |
| Session mirror polish | 139 | Coalesce dirty ops (one per session_id; Delete supersedes Put); debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50; Delete immediate); optional durable `__fetch_session_mirrors` (`VOLANT_FETCH_SESSION_MIRROR_DURABLE`); `mirror_gen` fence; still not Raft |
| Promote claim fence | 143 | `promoted_by` lowest-id on equal-fresh dual-promote; claim travels in MirrorPut JSON; metric `volant_fetch_session_promote_claim_reject_total`; brief dual primary until exchange; not Raft |
| Serve-from-mirror without promote | 147 | Owner miss + mirror → serve foreign mirror by default (no promote); metric `volant_fetch_session_serve_from_mirror_total`; promote opt-in via `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` or legacy `SERVE_MIRROR_WITHOUT_PROMOTE=0`; dual-epoch residual (not single SoT) |
| Preferred selector depth | 140 | Optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (unset = unlimited); `volant_preferred_replica_suppressed_total` on READ_COMMITTED suppress when candidate exists; not full Kafka selector/throttling |
| Preferred × session suppress | 144 | Suppress PreferredReadReplica when `req_session_id != 0` (non-FINAL); metric `volant_preferred_replica_session_suppressed_total`; full fetch `session_id==0` still prefers |
| Rack-aware create assignment | 145 | Multi-rack diversity on create/create-partitions; `VOLANT_RACK_AWARE_ASSIGNMENT` default on; metric `volant_rack_aware_assignment_total`; no rebalance |
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
| 2PC | **MVP** (Phase 90/92/94/96/97/**114**/**120**/**121**/**122**/**124**/**127**/**128**): Enable2Pc → first EndTxn prepares; second matching EndTxn finalizes; KeepPreparedTxn + OngoingTxn*; prepared timeout auto-abort (default 60s, env `VOLANT_PREPARED_TXN_TIMEOUT_MS`, clamped by max); timeout → abortable mark → Kafka **123** (Phase 94); background + lazy expiry (Phase 97); **multi-broker** prepare/complete fan-out across partition leaders (Phase 114; controller cluster prepared index; inter-broker opcodes 76–81); **transparent EndTxn / AddOffsets / TxnOffsetCommit forward** to Init-owner coordinator when client hits another broker (Phase 120/122; opcodes 84/85; no dual prepare / dual offset buffer); **sticky FindCoordinator** for group/txn keys (Phase 121; murmur2 static ring + Init-owner override); **durable Init-owner registry** under `{data_dir}/__txn_coordinator` (Phase 124; restart restore) with **TTL GC** (Phase 127; default 24h / `VOLANT_TXN_COORDINATOR_TTL_MS`; `0` off) + **BROKER Describe/Alter** (Phase 128; `volant.txn.coordinator.registry.ttl.ms`); **not** full KIP-890/939 / Kafka `__transaction_state` topic |
| Epochs | **Durable history MVP** (Phase 87): `{data_dir}/__leader_epochs`; prior epochs return transition end offsets; Metadata advertises live epoch; not a full KRaft epoch state machine; Fetch **DivergingEpoch** on truncation (Phase 88) |
| TopicId | Deterministic UUID from Volant id (`volant` + zeros + u32), not KRaft random |
| Groups | Coordinator-driven assignment; GroupType always `classic`; states Stable/Empty |
| Fetch sessions | **Real MVP** (Phase 88 + **91** + **95** + **97** + **99** + **100** + **102** + **103** + **115** + **119** + **126** + **138** + **139** + **143** + **144**): create/merge/forgotten; empty-topics re-fetch; errors 70/71; **omit-unchanged** when HWM+LSO unchanged and records empty; **idle TTL** (default 60s / `VOLANT_FETCH_SESSION_IDLE_MS`) + **max sessions** (default 1000 / `VOLANT_FETCH_SESSION_MAX`, LRU at cap); idle also background-swept (Phase 97); knobs on BROKER Describe/Alter (Phase 99) with **sparse** durable restart (Phase 100/102) and name vs `node_id` (Phase 103); **session table durable** under `{data_dir}/__fetch_sessions` (Phase 115); **multi-broker handoff** (Phase 119): cluster session_id embeds owner; non-owner transparent-forwards Fetch to owner over inter-broker while owner alive (no dual epoch); **shared mirror + promote** (Phase 138): best-effort peer MirrorPut/Delete (opcodes 90–93); foreign mirror not served while owner up; owner death / forward fail → promote mirror into primary under same session_id and serve locally when present, else **70**; **mirror polish** (Phase 139): coalesce dirty ops (one per session_id; Delete supersedes Put); debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50; Delete immediate); optional durable `{data_dir}/__fetch_session_mirrors` (`VOLANT_FETCH_SESSION_MIRROR_DURABLE`); `mirror_gen` fencing on apply/promote; **promote claim fence** (Phase 143): `promoted_by` lowest-id on equal-fresh dual-promote via MirrorPut; metric `volant_fetch_session_promote_claim_reject_total`; not Raft / not controller SoT; put lag/fail still **70**; brief dual primary until claim exchange; no re-encode of session_id owner bits; no serve-from-mirror without promote; **PreferredReadReplica** → see Preferred replica row (126+133+140+144) |
| Preferred replica | **MVP** (Phase **126** + **133** + **140** + **144**): Fetch v11+ rack → same-rack live ISR peer with usable addr + LEO≥HWM redirect (empty records); **rank highest LEO then lowest id** (133); optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` skips peers over lag vs leader LEO (**unset** = unlimited) (140); **suppressed when isolation=READ_COMMITTED** (leader serves aborted filter); metric `volant_preferred_replica_suppressed_total` when a candidate existed (140); **suppressed when client has established fetch session** (`req_session_id != 0`, non-FINAL) to avoid session-owner thrash after preferred redirect; metric `volant_preferred_replica_session_suppressed_total` (144); full fetch `session_id == 0` may still redirect; gated off for followers (`replica_id` / ReplicaState); Metadata rack from `cluster.toml`; not full Kafka selector/throttling |
| Rack-aware assignment | **MVP** (Phase **145**): create-topic / create-partitions maximize rack diversity when ≥2 distinct configured racks; legacy round-robin when no/single rack or `VOLANT_RACK_AWARE_ASSIGNMENT=0`; metric `volant_rack_aware_assignment_total`; no rebalance of existing topics |
| Fetch sessions | **Real MVP** (Phase 88 + **91** + **95** + **97** + **99** + **100** + **102** + **103** + **115** + **119** + **126** + **138** + **139** + **143** + **144** + **147**): create/merge/forgotten; empty-topics re-fetch; errors 70/71; **omit-unchanged** when HWM+LSO unchanged and records empty; **idle TTL** (default 60s / `VOLANT_FETCH_SESSION_IDLE_MS`) + **max sessions** (default 1000 / `VOLANT_FETCH_SESSION_MAX`, LRU at cap); idle also background-swept (Phase 97); knobs on BROKER Describe/Alter (Phase 99) with **sparse** durable restart (Phase 100/102) and name vs `node_id` (Phase 103); **session table durable** under `{data_dir}/__fetch_sessions` (Phase 115); **multi-broker handoff** (Phase 119): cluster session_id embeds owner; non-owner transparent-forwards Fetch to owner over inter-broker while owner alive (no dual epoch); **shared mirror** (Phase 138): best-effort peer MirrorPut/Delete (opcodes 90–93); foreign mirror not served while owner up; **serve-from-mirror without promote** (Phase 147 default): owner death / forward fail → serve foreign mirror in-place when present (metric `volant_fetch_session_serve_from_mirror_total`; no primary insert); promote opt-in via `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` or legacy `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE=0`; else **70**; **mirror polish** (Phase 139): coalesce dirty ops; debounced Puts; optional durable `__fetch_session_mirrors`; `mirror_gen` fence; **promote claim fence** (Phase 143): `promoted_by` lowest-id on equal-fresh dual-promote; dual-epoch residual when two peers both serve mirrors without single SoT; not Raft / not controller SoT; put lag/fail still **70**; no re-encode of session_id owner bits; **PreferredReadReplica** → see Preferred replica row (126+133+140+144) |
| Preferred replica | **MVP** (Phase **126** + **133** + **140** + **144**): Fetch v11+ rack → same-rack live ISR peer with usable addr + LEO≥HWM redirect (empty records); **rank highest LEO then lowest id** (133); optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` skips peers over lag vs leader LEO (**unset** = unlimited) (140); **suppressed when isolation=READ_COMMITTED** (leader serves aborted filter); metric `volant_preferred_replica_suppressed_total` when a candidate existed (140); **suppressed when client has established fetch session** (`req_session_id != 0`, non-FINAL) to avoid session-owner thrash after preferred redirect; metric `volant_preferred_replica_session_suppressed_total` (144); full fetch `session_id == 0` may still redirect; gated off for followers (`replica_id` / ReplicaState); Metadata rack from `cluster.toml`; not full Kafka selector/throttling / rack-aware partition assignment |

| ISR / HWM (cluster) | Kafka-style static ISR (Phase 6): death shrink + HWM recompute (Phase 108/110); **rejoin** when ReplicaFetch LEO ≥ HWM and lag ≤ `replica_lag_max_messages`; lag-shrink of slow-but-alive members (Phase 118); **time-based lag shrink** via last-caught-up + `replica_lag_max_ms` (Phase 125; not full Kafka `replica.lag.time.max.ms` parity); **Phase 142:** Metadata on the leader overlays local ISR; non-controller leaders best-effort `IsrUpdate` (94/95) to controller — non-leader Metadata may still lag until report + ClusterState |
| CreateTopics | Replica assignment arrays ignored; configs response often null; wait/rollback same as native CreateTopic (majority miss → **19**) |
| Storage | Log stores uncompressed Volant records; Fetch re-encodes |
| Auth | Kafka port: SASL or `kafka-anonymous`; no shared-token on Kafka port |
| ACLs | LITERAL only; host always `*`; User resource (v3) stored only (no SCRAM-admin gating); no TransactionalId/DelegationToken; cluster Create/Delete are **controller-only** with generationed snapshot fan-out (Phase 113) + durable-gen rejoin catch-up (Phase 117) — not Raft multi-master consensus |
| BROKER config (cluster) | Alter / IncrementalAlter for the six Phase 99 knobs: **controller-only** (Kafka **41** NotController elsewhere); push to live peers; Describe on any node returns **local** effective values after apply; offline peers catch up on heartbeat re-push (Phase 117; not Raft) |
| DeleteRecords (cluster) | Leader-only client path; best-effort inter-broker truncate of other replicas (Phase 113); fan-out failure does not fail the client by default; failed peers recorded in leader-local durable outbox and retried when live (Phase 116); **Phase 123** new leader rebuilds pending targets from local `log_start` after leadership change; truncate journal SoT + majority (Phase 129/130) with optional client wait (Phase 135 env; Phase 137 **native** per-request trailer only — Kafka still broker knob); local low not rolled back on wait fail; **Phase 137** assignment prune + known-topic push filter (journal GC / anti-resurrection) — still not a full Raft truncate log |
| KIP-951 | CurrentLeader on leader errors; Produce NodeEndpoints v10+; Fetch NodeEndpoints v16+; empty tags on success |
| ApiVersions features | Empty SupportedFeatures / FinalizedFeatures / ZkMigrationReady tags; no REBOOTSTRAP_REQUIRED |
| ElectLeaders | **v0–1 wrap** (v0.236): key **43** advertised; preferred = `elect_leader(ISR∩live)`; ElectionType **1** unclean refused (**87**); TimeoutMs ignored; assignment wait/rollback same as key 45; not Kafka `preferred.leader`; not live replica copy |
| AlterPartitionReassignments | **v0 wrap** (v0.225): key **45** advertised; apply is native opcode 114 (instant; new replicas start empty); TimeoutMs ignored; `replicas=null` → **83** (no cancel log / no pending state); not live Kafka reassignment |
| ListPartitionReassignments | **v0 list** (v0.228): key **46** advertised; current assignment as `replicas`; empty adding/removing (apply is instant; no pending log); TimeoutMs ignored; not live Kafka reassignment progress |
| Describe/AlterUserScramCredentials | **v0 wrap** (v0.233): keys **50** / **51** advertised; wrap `ScramStore` (native 64–69). Alter upsert is Kafka `saltedPassword = Hi(...)`, not plaintext. Native create still sends password in the clear. Unknown user → **91** `RESOURCE_NOT_FOUND`. Not OAUTH/GSSAPI; not quota keys 48/49 |
| DescribeTopicPartitions | **v0 wrap** (v0.237): key **75** advertised; wraps `Broker::metadata`. Same leaders / ISR / epochs / deterministic TopicId as Metadata. Unknown topic → **3**. ACL Topic DESCRIBE. Empty topics = all. `responsePartitionLimit <= 0` unlimited; simple truncate + `next_cursor` when cut. Cursor honored only when its topic is in the result set (else ignored). No ELR fields (Metadata partition body reused). Not Metadata v13+ |
| Missing APIs | Large Kafka surface still unsupported (GSSAPI, OAUTH, quota keys 48/49, …) |

## Related

- [ops.md](./ops.md) — Kafka listen ops notes  
- [features.md](./features.md) — native txn/ACL/SCRAM behavior  
- [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) — phase index  
