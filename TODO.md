# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–154** + residuals **v0.3–v0.216**; **Phase 155 open**.  
**Last review:** 2026-09-03  

Living roadmap: [ROADMAP.md](./ROADMAP.md).  
Recent specs: [PHASE154](./docs/PHASE154_SPEC.md) · [PHASE153](./docs/PHASE153_SPEC.md) · [PHASE152](./docs/PHASE152_SPEC.md) · [PHASE151](./docs/PHASE151_SPEC.md) · [PHASE150](./docs/PHASE150_SPEC.md) · [PHASE149](./docs/PHASE149_SPEC.md).  
Phase index: [docs/history/PHASE_HISTORY.md](./docs/history/PHASE_HISTORY.md).

---

## Status

| Band | Status |
|------|--------|
| **P0 / P1** | **None open** |
| **P2** (N=2 gauges, Metadata ISR, promote claim, preferred×session) | **Closed** (141–144) |
| **P3** (rack assignment, delta mirror, serve-from-mirror, defer truncate) | **Closed** (145–148) |
| **Product: streams durable + EOS** | **MVP closed** (149, 151, 153) + v0.8 fence + v0.9 changelog |
| **Product: consensus / KRaft-style metadata** | **154 MVP closed**; **Phase 155 open** — openraft cluster SoT (not homemade election) |

**Ceiling:** Phases **0–154** shipped, **155 open**. Native SyncGroup is **116/117**.

---

## Shipped recently (compact)

### Consensus / metadata (150 → 152 → 154)
- [x] **150** — Assignment majority notes (opcodes **96/97**); configured-N majority
- [x] **152** — Metadata **opt-in** committed assignment snapshot (`VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` default **off**; live Metadata)
- [x] **154** — KRaft-style metadata **Raft log** (term/index, AppendEntries **98/99**, commit_index → apply)

### Streams (149 → 151 → 153)
- [x] **149** — `DurableStore` (redb) `KeyValueStore`; `count_reduce_durable`
- [x] **151** — `ProcessingGuarantee::ExactlyOnce` — txn produce + deferred group offsets
- [x] **153** — EOS **checkpoint** boundary: stage durable puts until EndTxn succeeds; abort discards

### Earlier P2 / P3 (still residual-relevant)
- [x] **141** — N=2 majority health gauges (`volant_cluster_*`)
- [x] **142** — Metadata ISR overlay + `IsrUpdate` **94/95**
- [x] **143** — Promote claim fence (`promoted_by` lowest-id)
- [x] **144** — Preferred suppress when `req_session_id != 0`
- [x] **145** — Rack-aware partition assignment on create
- [x] **146** — Delta MirrorPut (`mode=full|delta`)
- [x] **147** — Serve-from-mirror without promote (default)
- [x] **148** — Wait-mode DeleteRecords: majority **before** local truncate

---

## Next candidates (suggested order)

| Pri | Item | Why / notes |
|----:|------|-------------|
| **P2** | **True Raft leader election** for metadata | **frozen (v0.2)** — [docs/V02_FREEZE.md](./docs/V02_FREEZE.md) §3/§4. Not the next slice. |
| **P2** | **InstallSnapshot / log compaction** for metadata Raft | **frozen (v0.2)** — [docs/V02_FREEZE.md](./docs/V02_FREEZE.md) §3/§4. Do not extend 154. |
| **P2** | **Local assignment rollback** on consensus/Raft majority fail | **closed (v0.3)** — wait/committed-only miss restores live `assignment.json` |
| **P2** | **Kafka admin assignment wait/rollback** | **closed (v0.4)** — CreateTopics/DeleteTopics/CreatePartitions share native `complete_assignment_mutation` (Kafka **19**) |
| **P2** | **v0.5 ops confidence** | **closed (v0.5)** — unwritable data dir, minority isolate of the leader, leader die mid in-flight `acks=all` |
| **P3** | **Distributed EOS 2PC** (broker-held stream state) | **closed (v0.9 MVP)** — opt-in changelog in the EOS txn; still one-process |
| **P3** | **Durable window buckets** | **closed (v0.2 PR5)** — `TumblingWindow::durable`; still process-local |
| **P3** | Preferred **throttling / TCP probe** | **closed (v0.7)** — opt-in `throttle_time_ms` + TCP connect probe |
| **P3** | Kafka DeleteRecords **per-request** wait | **closed (v0.6)** — flex v2 tag 0; v0–1 env-only |
| **P3** | Cross-app EOS fencing | **closed (v0.8)** — optional `application_id` fence id |
| **Later** | Full **openraft** crate integration | **v0.11–v0.26 + redb log (v0.35) + joint rollback (v0.34) + follower forward (v0.38)** — not RocksDB/KRaft |
| **Later** | **Dynamic membership** reconfiguration | **overlay v0.10 + joint v0.26 + rollback v0.34 + follower forward v0.38 + reassign rollback v0.39** — overlay still SoT |
| **Later** | Full **KIP-890 / `__transaction_state`** | **log MVP closed (v0.13)** — opt-in JSON topic; not Kafka schemas |
| **Later** | **Multi-language clients** | **Python/Go/Java through v0.211** + Rust GroupConsumer SyncGroup **v0.208**; not kafka-python |
| **Later** | **Long fuzz + chaos-mesh** | **MVP closed (v0.15)** — extended corpus + Chaos Mesh YAML + A→B isolate |
| **Later** | **Perf campaign** vs aspirational targets | **closed (v0.2 PR2)** — measured table published; group-commit **v0.20** (opt-in, no new bench) |

**Default next slice:** Overlay is apply-after-commit when openraft-on (**v0.212/v0.216**); homemade 154 is gated (**v0.213/v0.214**); SyncGroup is a generation fence (**v0.215**). In-process add/remove_broker still persist-first. Homemade 154 code is not deleted. CompletingRebalance / parked Join / Kafka keys / KIP-890 stay frozen. Residual **v0.155** is still DeleteRecords wait.

---

## Closed checklist (was open residual)

- [x] N=2 majority ops tooling / health gauges → **Phase 141**
- [x] Metadata ISR lag (leader ≠ controller) → **Phase 142**
- [x] Promote claim fence (dual-promote) → **Phase 143**
- [x] Preferred × session thrash suppress → **Phase 144**
- [x] Rack-aware partition assignment → **Phase 145**
- [x] Incremental/delta MirrorPut → **Phase 146**
- [x] Serve-from-mirror without promote → **Phase 147**
- [x] Defer local truncate until majority (wait mode) → **Phase 148**
- [x] Durable stream state store → **Phase 149**
- [x] Assignment majority consensus notes → **Phase 150**
- [x] Stream EOS (txn produce + offsets) → **Phase 151**
- [x] Metadata = committed assignment → **Phase 152**
- [x] EOS + durable state atomic boundary → **Phase 153**
- [x] KRaft-style metadata Raft log MVP → **Phase 154**
- [x] Assignment wait-fail local rollback → **v0.3**
- [x] Kafka admin assignment wait/rollback → **v0.4**
- [x] Unwritable dir / isolate leader / in-flight acks=all → **v0.5**
- [x] Kafka DeleteRecords per-request wait (flex v2 tag 0) → **v0.6**
- [x] Preferred redirect throttle + TCP probe → **v0.7**
- [x] Cross-app EOS fencing (`application_id`) → **v0.8**
- [x] EOS changelog-backed durable state (txn 2PC MVP) → **v0.9**
- [x] Dynamic membership overlay (add/remove broker) → **v0.10**
- [x] openraft metadata leader election (opt-in) → **v0.11**
- [x] `__cluster_metadata` topic + per-partition Raft log → **v0.12**
- [x] `__transaction_state` topic (KIP-890 log MVP) → **v0.13**
- [x] Python native client (produce/fetch/metadata) → **v0.14**
- [x] Fuzz corpus expansion + chaos-mesh + asymmetric isolate → **v0.15**
- [x] openraft SetAssignment log apply → **v0.16**
- [x] openraft InstallSnapshot (opcodes 112/113) → **v0.17**
- [x] Partition reassignment after add-broker → **v0.18**
- [x] Go native client (produce/fetch/metadata) → **v0.19**
- [x] Produce group-commit (coalesced fsync) → **v0.20**
- [x] Durable openraft log + hard state → **v0.21**
- [x] Apply assignment from openraft snapshot → **v0.22**
- [x] Java native client (produce/fetch/metadata) → **v0.23**
- [x] Python and Go offset commit/fetch → **v0.24**
- [x] Fetch-session dual-epoch converge → **v0.25**
- [x] openraft joint membership on add/remove → **v0.26**
- [x] TLS for Python/Go/Java native clients → **v0.27**
- [x] JoinGroup/Heartbeat/LeaveGroup on native clients → **v0.28**
- [x] Cluster DeleteRecords wait-off safety → **v0.29**
- [x] Fetch-session mirror-only self-converge → **v0.30**
- [x] Python GroupConsumer → **v0.31**
- [x] Go GroupConsumer → **v0.32**
- [x] Java offsets + GroupConsumer → **v0.33**
- [x] Rollback overlay if openraft joint fails → **v0.34**
- [x] openraft log in redb → **v0.35**
- [x] Static group_instance_id on Python/Go/Java GroupConsumers → **v0.36**
- [x] GroupConsumer background heartbeat (Python/Go/Java) → **v0.37**
- [x] Follower Add/RemoveBroker forward to openraft leader → **v0.38**
- [x] Restore assignment if add-broker joint rolls back → **v0.39**
- [x] Wait for homemade metadata-raft commit before CreateTopic ok → **v0.40**
- [x] Client-side range assignor (Python/Go/Java) → **v0.41**
- [x] Shared-token Auth on Python/Go/Java clients → **v0.42**
- [x] Leader redirect on Python/Go/Java produce/fetch → **v0.43**
- [x] Rust GroupConsumer background heartbeat → **v0.44**
- [x] Clustered DeleteRecords wait-off requires ACK → **v0.45**
- [x] SCRAM-SHA-256 on Python/Go/Java clients → **v0.46**
- [x] Idempotent produce on Python/Go/Java clients → **v0.47**
- [x] GroupConsumer auto-commit on Python/Go/Java → **v0.48**
- [x] ListGroups and DescribeGroup on Python/Go/Java → **v0.49**
- [x] ListOffsets on Python/Go/Java clients → **v0.50**
- [x] CreatePartitions on Python/Go/Java clients → **v0.51**
- [x] DeleteRecords on Python/Go/Java clients → **v0.52**
- [x] DescribeConfigs and AlterConfigs on Python/Go/Java → **v0.53**
- [x] DeleteOffsets on Python/Go/Java clients → **v0.54**
- [x] Create/Delete/ListScramUsers on Python/Go/Java → **v0.55**
- [x] Create/Delete/ListAcls on Python/Go/Java → **v0.56**
- [x] BeginTxn/EndTxn on Python/Go/Java → **v0.57**
- [x] AddBroker/RemoveBroker/ListMembers on Python/Go/Java → **v0.58**
- [x] ReassignPartitions on Python/Go/Java → **v0.59**
- [x] Rust GroupConsumer auto-commit → **v0.60**
- [x] Produce retry on Python/Go/Java → **v0.61**
- [x] GroupConsumer auto_offset_reset on Python/Go/Java → **v0.62**
- [x] TransactionalProducer helper on Python/Go/Java → **v0.63**
- [x] Go/Java Fetch knobs and Produce acks → **v0.64**
- [x] DeleteRecords leader redirect on Python/Go/Java → **v0.65**
- [x] Fetch retry on Python/Go/Java → **v0.66**
- [x] Rust GroupConsumer auto_offset_reset → **v0.67**
- [x] Go/Java ProduceBatch → **v0.68**
- [x] Multi-member range via DescribeGroup → **v0.69**
- [x] GroupConsumer earliest via ListOffsets → **v0.70**
- [x] Rust GroupConsumer earliest via ListOffsets → **v0.71**
- [x] Admin NotController redirect on Python/Go/Java → **v0.72**
- [x] Rust GroupConsumer range via DescribeGroup → **v0.73**
- [x] Heartbeat retry on Python/Go/Java → **v0.74**
- [x] GroupConsumer poll fetch knobs on Python/Go/Java → **v0.75**
- [x] Rust GroupConsumer poll fetch knobs → **v0.76**
- [x] Metadata controller_id trailer → **v0.77**
- [x] OffsetCommit/Fetch/DeleteOffsets retry on Python/Go/Java → **v0.78**
- [x] Rust admin NotController redirect → **v0.79**
- [x] Rust heartbeat retry → **v0.80**
- [x] Language admin-14 prefers Metadata.controller_id → **v0.81**
- [x] ListOffsets retry on Python/Go/Java → **v0.82**
- [x] Rust OffsetCommit/Fetch/DeleteOffsets retry → **v0.83**
- [x] Rust ListOffsets retry → **v0.84**
- [x] SCRAM-admin/ListAcls NotController redirect on Python/Go/Java → **v0.85**
- [x] LeaveGroup retry on Python/Go/Java → **v0.86**
- [x] Rust LeaveGroup retry → **v0.87**
- [x] Rust SCRAM-admin/ListAcls NotController redirect → **v0.88**
- [x] AddBroker/RemoveBroker NotController redirect on Python/Go/Java → **v0.89**
- [x] DescribeGroup/ListGroups retry on Python/Go/Java → **v0.90**
- [x] Rust AddBroker/RemoveBroker NotController redirect → **v0.91**
- [x] Rust DescribeGroup/ListGroups retry → **v0.92**
- [x] Describe/AlterConfigs NotController redirect on Python/Go/Java → **v0.93**
- [x] Rust Describe/AlterConfigs NotController redirect → **v0.94**
- [x] Metadata/ListMembers retry on Python/Go/Java → **v0.95**
- [x] Rust Metadata/ListMembers retry → **v0.96**
- [x] DeleteOffsets NotController redirect on Python/Go/Java → **v0.97**
- [x] Rust DeleteOffsets NotController redirect → **v0.98**
- [x] BeginTxn/EndTxn retry on Python/Go/Java → **v0.99**
- [x] Rust BeginTxn/EndTxn retry → **v0.100**
- [x] InitProducerId retry on Python/Go/Java → **v0.101**
- [x] Rust InitProducerId retry → **v0.102**
- [x] admin_round_trip transient retry on Python/Go/Java → **v0.103**
- [x] Rust admin_round_trip transient retry → **v0.104**
- [x] OffsetCommit/Fetch NotController redirect on Python/Go/Java → **v0.105**
- [x] Auth retry on Python/Go/Java → **v0.106**
- [x] Rust Auth retry → **v0.107**
- [x] SCRAM handshake retry on Python/Go/Java → **v0.108**
- [x] Rust SCRAM handshake retry → **v0.109**
- [x] DeleteRecords transient retry on Python/Go/Java → **v0.110**
- [x] Rust DeleteRecords 13 redirect + transient retry → **v0.111**
- [x] ListOffsets NotLeader redirect on Python/Go/Java → **v0.112**
- [x] Rust ListOffsets NotLeader redirect → **v0.113**
- [x] Rust metadata topic filter → **v0.114**
- [x] public reconnect on Python/Go/Java → **v0.115**
- [x] Go/Java metadata topic filter → **v0.116**
- [x] Go/Java CreateTopic configs → **v0.117**
- [x] OffsetFetch all-group on Python/Go/Java → **v0.118**
- [x] public CommitOffsets batch on Python/Go/Java → **v0.119**
- [x] Rust ListMembers NotController redirect → **v0.120**
- [x] ListMembers NotController redirect on Python/Go/Java → **v0.121**
- [x] OffsetFetch entries on Python/Go/Java → **v0.122**
- [x] Python GroupConsumer batch OffsetCommit → **v0.123**
- [x] DescribeGroup/ListGroups NotController redirect on Python/Go/Java → **v0.124**
- [x] Rust DescribeGroup/ListGroups NotController redirect → **v0.125**
- [x] Go CreateTopicID returns topic id → **v0.126**
- [x] Go/Java JoinGroup with instance id → **v0.127**
- [x] Go/Java OffsetCommit metadata → **v0.128**
- [x] language produce default acks → **v0.129**
- [x] Go/Java Produce headers → **v0.130**
- [x] Go/Java JoinGroup rejoin member_id → **v0.131**
- [x] Go/Java Produce timestamp → **v0.132**
- [x] Go/Java Produce headers + acks → **v0.133**
- [x] Heartbeat NotController redirect on Python/Go/Java → **v0.134**
- [x] Rust Heartbeat NotController redirect → **v0.135**
- [x] LeaveGroup NotController redirect on Python/Go/Java → **v0.136**
- [x] Rust LeaveGroup NotController redirect → **v0.137**
- [x] Go/Java Produce timestamp + headers → **v0.138**
- [x] Go OffsetCommit member + generation → **v0.139**
- [x] Go/Java OffsetFetch entry metadata → **v0.140**
- [x] Go/Java Produce timestamp + acks → **v0.141**
- [x] Go/Java Produce timestamp + headers + acks → **v0.142**
- [x] language Fetch client-level default knobs → **v0.143**
- [x] Rust ClientConfig Fetch knobs → **v0.144**
- [x] Go/Java Fetch high watermark → **v0.145**
- [x] Java JoinGroup member + instance → **v0.146**
- [x] Go/Java ProduceBatch default acks → **v0.147**
- [x] language OffsetFetch topic + metadata → **v0.148**
- [x] Rust fetch uses ClientConfig fetch_max_bytes → **v0.149**
- [x] language public InitProducerId → **v0.150**
- [x] Rust public InitProducerId → **v0.151**
- [x] language DeleteRecords default wait flag → **v0.152**
- [x] Rust single-entry OffsetCommit → **v0.153**
- [x] Rust OffsetFetch topic + metadata → **v0.154**
- [x] Rust DeleteRecords default wait flag → **v0.155**
- [x] Metadata NotController redirect on Python/Go/Java → **v0.156**
- [x] Rust Metadata NotController redirect → **v0.157**
- [x] Go DeleteOffsetsAll → **v0.158**
- [x] Rust fetch_offsets_all → **v0.159**
- [x] Go/Python/Rust producer id getters → **v0.160**
- [x] Go ListAclsAll → **v0.161**
- [x] Rust list_acls_all → **v0.162**
- [x] Go ListOffsetsAll → **v0.163**
- [x] language single-entry DeleteOffset → **v0.164**
- [x] Rust delete_offsets_all + delete_offset → **v0.165**
- [x] Rust list_offsets_all → **v0.166**
- [x] Go ReassignAllPartitions → **v0.167**
- [x] Rust reassign_partitions_all → **v0.168**
- [x] language single-entry CreateAcl / DeleteAcl → **v0.169**
- [x] Rust create_acl / delete_acl → **v0.170**
- [x] Go AddBrokerNoRack → **v0.171**
- [x] Rust add_broker_no_rack → **v0.172**
- [x] Go CreateScramUserDefault → **v0.173**
- [x] Rust create_scram_user_default → **v0.174**
- [x] Rust commit_transaction_empty → **v0.175**
- [x] Go CommitTransactionEmpty → **v0.176**
- [x] language single-entry AlterConfig → **v0.177**
- [x] Rust alter_config → **v0.178**
- [x] language single-entry FetchOffset → **v0.179**
- [x] Rust fetch_offset → **v0.180**
- [x] language single-topic MetadataTopic → **v0.181**
- [x] Rust metadata_topic → **v0.182**
- [x] Go Addr getter → **v0.183**
- [x] Go/Java GroupConsumer assignor getter → **v0.184**
- [x] Go SetEnableIdempotence / Idempotence → **v0.185**
- [x] Go GroupConsumer HeartbeatCount → **v0.186**
- [x] Java GroupConsumer heartbeatCount → **v0.187**
- [x] Python GroupConsumer heartbeat_count → **v0.188**
- [x] Go/Java GroupConsumer sessionTimeoutMs getter → **v0.189**
- [x] Go/Java GroupConsumer Leave alias → **v0.190**
- [x] Go MaxRedirects getter → **v0.191**
- [x] Go MaxRetries getter → **v0.192**
- [x] Go RetryBackoff getter → **v0.193**
- [x] Go TransactionalID getter → **v0.194**
- [x] language Client timeout getter → **v0.195**
- [x] Python list_acls_all → **v0.196**
- [x] Python list_offsets_all → **v0.197**
- [x] Python reassign_partitions_all → **v0.198**
- [x] Go/Java CreateTopic default partitions=1 → **v0.199**
- [x] Go/Java auth token getter → **v0.200**
- [x] Java heartbeatIntervalMs public → **v0.201**
- [x] Go/Java SCRAM username getter → **v0.202**
- [x] Rust create_topic_default → **v0.203**
- [x] language GroupConsumer SyncGroup peek after join → **v0.207**
- [x] Rust GroupConsumer SyncGroup peek after join → **v0.208**
- [x] language first-Join client member_id → **v0.209**
- [x] Rust first-Join client member_id → **v0.210**
- [x] JoinGroup members trailer for range → **v0.211**
- [x] overlay persist after openraft joint → **v0.212**
- [x] IsrUpdate skips homemade 154 when openraft on → **v0.213**
- [x] gate inbound homemade 154 + lazy raft dir → **v0.214**
- [x] SyncGroup generation confirm fence → **v0.215**
- [x] overlay apply artifact from Membership log → **v0.216**

---

## Still open (honest limitations)

### Metadata / consensus
- [x] True **openraft** leader election + term contests (v0.11 opt-in; default still lowest-id)
- [x] **InstallSnapshot** / log truncation on **openraft** (v0.17; homemade 154 still frozen)
- [x] **Dynamic membership** overlay (v0.10) + joint (v0.26) + rollback on raft fail (v0.34) + follower forward (v0.38) + reassign rollback (v0.39)
- [x] Rollback **local** assignment file when wait/committed-only majority misses (v0.3 residual; `!must_wait` still retains local)
- [x] Kafka CreateTopics / DeleteTopics / CreatePartitions honor the same wait/rollback (v0.4; majority miss → Kafka **19**)
- [x] v0.5 ops confidence (unwritable dir / isolate leader / in-flight acks=all)
- [x] Per-partition Raft / `__cluster_metadata` topic MVP (v0.12; not Kafka KRaft schemas; ISR still SoT)

### Streams
- [x] **Distributed** EOS changelog MVP (v0.9; state in the EOS txn; still one-process)
- [x] Durable **window** state (in-process `TumblingWindow::durable`; not cluster EOS)
- [x] Exactly-once **cross-app** fencing via `application_id` (v0.8; not Kafka Streams assignment)

### Kafka / txn / ops
- [x] `__transaction_state` log MVP (v0.13; Volant JSON; not Kafka KIP-890/939 schemas)
- [x] Kafka DeleteRecords **per-request** wait flag (v0.6 flex v2 tag 0; v0–1 env-only)
- [x] Preferred selector **throttling** / TCP probe (v0.7; opt-in, not Kafka quota)
- [x] Multi-language clients — Python/Go/Java through v0.211 (GroupConsumer SyncGroup peek, first-Join member_id, Join members trailer) + Rust **v0.208/v0.210**; not kafka-python
- [x] Long fuzz campaigns + chaos-mesh MVP (v0.15; corpus + YAML + A→B isolate; not multi-hour CI)
- [x] Published perf numbers vs aspirational table; group-commit **v0.20** (opt-in)

### Wait-off / best-effort paths (by design)
- DeleteRecords **wait off**: cluster upgrades to wait-on unless **both** `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1` **and** `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK=1` (v0.45). Single-node wait-off, and both-envs-on, are still irreversible
- Session mirror: dual unclaimed primary (v0.25) + mirror-only (v0.30) converge on MirrorPut / helper; still not Raft
- Metadata raft wait-commit default **on** (v0.40): CreateTopic waits for `commit_index` else rollback + **15**; escape `VOLANT_METADATA_RAFT_WAIT_COMMIT=0` (Phase 154 tests)
- Local `assignor="range"` uses DescribeGroup members (language **v0.69**, Rust **v0.73**); describe failure falls back. JoinGroup still has no member list / no SyncGroup
- Language-client SCRAM handshake is **SHA-256** only (v0.46); admin Create/Delete/ListScramUsers are native **64–69** (v0.55); password is sent in the clear on create (use TLS); not Kafka SASL / AlterUserScramCredentials
- Idempotent produce is native **32/33** (v0.47); BeginTxn/EndTxn are native **50–53** (v0.57); `TransactionalProducer` is a thin helper (v0.63). Not Kafka txn API keys
- Produce/fetch/heartbeat/offset-admin/ListOffsets/LeaveGroup/DescribeGroup/ListGroups/Metadata/ListMembers/BeginTxn/EndTxn/InitProducerId/admin_round_trip/Auth/SCRAM handshake/DeleteRecords retry (v0.61 / v0.66 / v0.74 / v0.78 / v0.82 / v0.86 / v0.90 / v0.95 / v0.99 / v0.101 / v0.103 / v0.106 / v0.108 / v0.110 / v0.111 / Rust **v0.80/v0.83/v0.84/v0.87/v0.92/v0.96/v0.100/v0.102/v0.104/v0.107/v0.109/v0.111**) is default **0**; transient codes 6/7/15/16 + TCP I/O only. LeaveGroup error **10** is success (already left). InvalidTxnState (22) is not retried. Error 21 on Init itself is not retried. Auth / SCRAM **17 / 18** are not retried. SCRAM first+final is one unit (new nonce on restart). Error 13 stays on `max_redirects` (Produce/Fetch/DeleteRecords/ListOffsets; language **v0.112**, Rust **v0.113**). Error **14** stays on `max_redirects` (independent of retry). ListMembers follows 14 (language **v0.121**, Rust **v0.120**; hunt uses a no-14 helper). DescribeGroup / ListGroups follow 14 (language **v0.124**, Rust **v0.125**; broker may still not return 14). JoinGroup is not retried. Language `reconnect` is public (**v0.115**). Go/Java Metadata can filter topics (**v0.116**). Go/Java CreateTopic can send configs (**v0.117**); Go `CreateTopicID` returns the topic id (**v0.126**; `CreateTopic` still discards it). OffsetFetchAll returns the whole group (**v0.118**); `fetch_offsets` can send wire entries (**v0.122**). CommitOffsets batch is public (**v0.119**); Python GroupConsumer commit is one RPC (**v0.123**). Go/Java JoinGroup can send instance id (**v0.127**). Go/Java OffsetCommit can send entry metadata (**v0.128**). Language produce default acks is 1 (**v0.129**; Rust `ClientConfig.acks`). Go/Java convenience Produce can send headers (**v0.130**), timestamp (**v0.132**), headers+acks (**v0.133**), timestamp+headers (**v0.138**), timestamp+acks (**v0.141**), and timestamp+headers+acks (**v0.142**). Language 3-arg Fetch uses client fetch knobs (**v0.143**; default 128 / 4MiB / 0). Rust `fetch_default` uses `ClientConfig` fetch knobs (**v0.144**). Go/Java `FetchResult` / `fetchResult` expose high watermark (**v0.145**; `Fetch` / `fetch` still return records only). Java `joinGroupMemberWithInstance` sends member+instance (**v0.146**). Go `ProduceBatchDefault` / Java list `produce` without acks use the client default (**v0.147**). Language `OffsetFetchEntries` keeps topic-filtered metadata (**v0.148**; `OffsetFetch` still partition+offset). Rust `fetch` uses `ClientConfig.fetch_max_bytes` (**v0.149**). Language public InitProducerId pre-allocates a pid (**v0.150**; produce / BeginTxn still init implicitly). Rust `init_producer_id` is public (**v0.151**). Language 3-arg DeleteRecords uses client `delete_records_wait` (**v0.152**; default 0). Rust has one-entry `commit_offset*` (**v0.153**) and `fetch_offsets_for_topic` (**v0.154**). Rust `delete_records` uses `ClientConfig.delete_records_wait` (**v0.155**; not Phase 155). Metadata follows 14 (language **v0.156**, Rust **v0.157**; hunt uses a no-14 helper). Go `DeleteOffsetsAll` deletes the whole group (**v0.158**). Rust `fetch_offsets_all` is the all-group helper (**v0.159**). Go/Python/Rust producer id/epoch getters do not force Init (**v0.160**; Java already had them). Go `ListAclsAll` lists every ACL (**v0.161**; same as `ListAcls("", 255, "")`). Rust `list_acls_all` is the unfiltered helper (**v0.162**). Go `ListOffsetsAll(topic)` lists every partition (**v0.163**; same as `ListOffsets(topic, nil)`). Language `DeleteOffset` / `deleteOffset` / `delete_offset` deletes one committed offset (**v0.164**). Rust `delete_offsets_all` / `delete_offset` wrap `delete_offsets` (**v0.165**). Rust `list_offsets_all` lists every partition (**v0.166**; same as `list_offsets(topic, vec![])`). Go `ReassignAllPartitions` reassigns every partition (**v0.167**; same as `ReassignPartitions(topic, replicas, nil)`). Rust `reassign_partitions_all` is the all-partition helper (**v0.168**). Language `CreateAcl` / `createAcl` / `create_acl` and `DeleteAcl` / `deleteAcl` / `delete_acl` wrap one binding (**v0.169**). Rust `create_acl` / `delete_acl` wrap the batch ACL APIs (**v0.170**). Go `AddBrokerNoRack` adds a broker with no rack (**v0.171**; same as `AddBroker(id, host, port, nil)`). Rust `add_broker_no_rack` is the no-rack helper (**v0.172**). Go `CreateScramUserDefault` uses iterations 0 (**v0.173**; broker default 4096). Rust `create_scram_user_default` is the same (**v0.174**). Rust `commit_transaction_empty` commits with no deferred offsets (**v0.175**). Go `CommitTransactionEmpty` is the same (**v0.176**; `CommitTransaction(nil)` unchanged). Language `AlterConfig` / `alterConfig` / `alter_config` wraps one key (**v0.177**). Rust `alter_config` is the single-key helper (**v0.178**). Language `FetchOffset` / `fetchOffset` / `fetch_offset` fetches one partition (**v0.179**). Rust `fetch_offset` wraps one `OffsetEntry` (**v0.180**). Language `MetadataTopic` / `metadataTopic` / `metadata_topic` filters one topic (**v0.181**). Rust `metadata_topic` is the same (**v0.182**). Go `Addr()` exposes the current broker address (**v0.183**). Go/Java GroupConsumer `Assignor` / `assignor` expose the join-time assignor (**v0.184**; Python already had it). Go `SetEnableIdempotence` / `Idempotence` match Java's setter/getter (**v0.185**; `EnableIdempotence()` still turns it on). Language GroupConsumer `HeartbeatCount` / `heartbeatCount` / `heartbeat_count` counts poll + background Heartbeat attempts, not JoinGroup (**v0.186–v0.188**). Go/Java `SessionTimeoutMs` / `sessionTimeoutMs` expose the join-time timeout (**v0.189**; Python already had it). Go `Leave` / Java `leave` alias `Close` / `close` (**v0.190**; Python already had `leave`). Go `MaxRedirects` / `MaxRetries` / `RetryBackoff` / `TransactionalID` match Java getters (**v0.191–v0.194**). Language `Timeout` / `timeoutMs` / `timeout` expose the dial/RPC timeout (**v0.195**). Python `list_acls_all` lists every ACL (**v0.196**; same as `list_acls()`). Python `list_offsets_all(topic)` lists every partition (**v0.197**; same as `list_offsets(topic)`). Python `reassign_partitions_all` reassigns every partition (**v0.198**; same as `reassign_partitions(topic, replicas)`). Go `CreateTopicDefault` / Java `createTopic(name)` default partitions to 1 (**v0.199**; Python already had it; Go `CreateTopic` still discards the id). Go `AuthToken` / Java `authToken` expose the stored shared-token (**v0.200**; Python `.auth_token` already public; SCRAM password stays private). Java `heartbeatIntervalMs` is public (**v0.201**; same clamp as Go `HeartbeatInterval` / Python `heartbeat_interval_ms`). Go `ScramUser` / Java `scramUsername` expose the stored SCRAM username (**v0.202**; Python `.scram_username` already public; password stays private). Rust `create_topic_default` creates a topic with 1 partition (**v0.203**; same as Python `create_topic(name)` / Go `CreateTopicDefault` / Java `createTopic(name)`). Go/Java JoinGroup can rejoin with member_id (**v0.131**). Heartbeat follows 14 (language **v0.134**, Rust **v0.135**; broker may still not return 14). LeaveGroup follows 14 (language **v0.136**, Rust **v0.137**; error **10** stays success). Go `OffsetCommitMember` sends member+generation (**v0.139**; `OffsetCommit` stays admin-only). Go/Java public OffsetFetchEntry carries metadata (**v0.140**).
- Auto-commit is poll-tied and default **off** (language **v0.48**, Rust **v0.60**); not Kafka `enable.auto.commit`
- GroupConsumer `auto_offset_reset`: `earliest` is ListOffsets earliest (language **v0.70**, Rust **v0.71**); `latest` is LEO. Not Kafka timestamp reset
- Go/Java convenience Produce is still one message; `ProduceBatch` / `produce(..., messages, acks)` sends N in one RPC (v0.68)
- ListOffsets is native **48/49** (v0.50); `latest` is LEO; no isolation / timestamp
- DeleteRecords error 13 redirects like Produce/Fetch (language **v0.65**, Rust **v0.111**). ListOffsets error 13 redirects the same way (language **v0.112**, Rust **v0.113**; first requested partition or 0). CreateTopic / DeleteTopic / CreatePartitions / Reassign / CreateAcls / DeleteAcls follow error **14** (language **v0.72**, Rust **v0.79**). ListMembers follows 14 (language **v0.121**, Rust **v0.120**). DescribeGroup / ListGroups follow 14 (language **v0.124**, Rust **v0.125**). SCRAM-admin / ListAcls follow 14 (language **v0.85**, Rust **v0.88**). Add/RemoveBroker follow 14 when broker forward is unavailable (language **v0.89**, Rust **v0.91**). Describe/AlterConfigs follow 14 (language **v0.93**, Rust **v0.94**; broker may still not return 14 on local-readable topic configs). DeleteOffsets follow 14 (language **v0.97**, Rust **v0.98**; broker may still not return 14 on group-local offsets). OffsetCommit / OffsetFetch follow 14 (language **v0.105**; Rust inherited via `offset_admin_round_trip` in **v0.98**). Heartbeat follows 14 (language **v0.134**, Rust **v0.135**; broker may still not return 14 on group-local heartbeat). LeaveGroup follows 14 (language **v0.136**, Rust **v0.137**; broker may still not return 14 on group-local leave; error **10** stays success). Metadata follows 14 (language **v0.156**, Rust **v0.157**; hunt uses a no-14 helper; broker may still not return 14 on Metadata). Metadata `controller_id` trailer (**v0.77**; `0` = unknown) is preferred when the 14 message has no hint (Rust splice + language **v0.81**)
- GroupConsumer poll fetch size is tunable (language **v0.75**, Rust **v0.76**, default 100 / 4MiB). Client 3-arg Fetch knobs are separate (language **v0.143**, Rust `fetch_default` **v0.144**, default 128 / 4MiB / 0)
- CreatePartitions **46/47** (v0.51) cannot shrink; Describe/AlterConfigs **40–43** (v0.53) are topic-only; DeleteOffsets **38/39** (v0.54) has no DeleteGroups opcode
- ACLs are native **54–59** (v0.56), exact-match delete only. Membership **102–107** (v0.58) and Reassign **114/115** (v0.59) do not change overlay-as-SoT

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 154 | **Shipped** — metadata log + AppendEntries; not full openraft election |
| Phase 153 | **Shipped** — EOS durable checkpoint; process-local only |
| Phase 152 | **Shipped (opt-in)** — committed-only Metadata; v0.2 default **off** (live) |
| Phase 151 | **Shipped** — stream ExactlyOnce via Volant txns |
| Phase 150/149 | **Shipped** — majority notes + redb DurableStore |
| Phases 141–148 | **Shipped** — prior P2/P3 residuals |
| P0 / P1 code | **None open** |
| v0.6–v0.10 | **Shipped** — Kafka DR wait tag; preferred throttle/probe; app fence; changelog EOS; membership overlay |
| v0.11–v0.15 | **Shipped** — openraft election; cluster-metadata + partition raft; txn-state topic; Python client; fuzz/chaos |
| v0.16–v0.20 | **Shipped** — openraft apply + snapshot; reassign; Go client; group-commit |
| v0.21–v0.25 | **Shipped** — durable openraft; snapshot apply; Java client; client offsets; dual-epoch |
| v0.26–v0.30 | **Shipped** — openraft joint; client TLS; JoinGroup; wait-off safety; mirror converge |
| v0.31–v0.35 | **Shipped** — Python/Go/Java GroupConsumer; joint rollback; openraft redb |
| v0.36–v0.40 | **Shipped** — static membership; bg heartbeat; follower forward; reassign rollback; raft wait-commit |
| v0.41–v0.45 | **Shipped** — client range assignor; shared-token Auth; leader redirect; Rust heartbeat; wait-off ACK |
| v0.46–v0.50 | **Shipped** — client SCRAM-SHA-256; idempotent produce; auto-commit; List/DescribeGroup; ListOffsets |
| v0.51–v0.55 | **Shipped** — CreatePartitions; DeleteRecords; Describe/AlterConfigs; DeleteOffsets; SCRAM admin |
| v0.56–v0.60 | **Shipped** — client ACLs; BeginTxn/EndTxn; membership admin; ReassignPartitions; Rust auto-commit |
| v0.61–v0.65 | **Shipped** — produce retry; auto_offset_reset; TransactionalProducer; Fetch knobs / Produce acks; DeleteRecords redirect |
| v0.66–v0.70 | **Shipped** — fetch retry; Rust auto_offset_reset; Go/Java ProduceBatch; DescribeGroup range; earliest via ListOffsets |
| v0.71–v0.75 | **Shipped** — Rust earliest via ListOffsets; admin 14 redirect; Rust range via DescribeGroup; heartbeat retry; poll fetch knobs |
| v0.76–v0.80 | **Shipped** — Rust poll knobs; Metadata controller_id; offset-admin retry; Rust admin 14; Rust heartbeat retry |
| v0.81–v0.85 | **Shipped** — language Metadata.controller_id hunt; ListOffsets retry; Rust offset/ListOffsets retry; SCRAM/ListAcls 14 |
| v0.86–v0.90 | **Shipped** — LeaveGroup retry; Rust LeaveGroup; Rust SCRAM 14; Add/RemoveBroker 14; Describe/ListGroups retry |
| v0.91–v0.95 | **Shipped** — Rust Add/RemoveBroker 14; Rust Describe/ListGroups retry; configs 14; Rust configs 14; Metadata/ListMembers retry |
| v0.96–v0.100 | **Shipped** — Rust Metadata retry; DeleteOffsets 14; Rust DeleteOffsets 14; BeginTxn/EndTxn retry; Rust BeginTxn/EndTxn retry |
| v0.101–v0.105 | **Shipped** — InitProducerId retry; Rust InitProducerId retry; admin_round_trip retry; Rust admin_round_trip retry; OffsetCommit/Fetch 14 |
| v0.106–v0.110 | **Shipped** — Auth retry; Rust Auth retry; SCRAM handshake retry; Rust SCRAM handshake retry; DeleteRecords retry |
| v0.111–v0.115 | **Shipped** — Rust DeleteRecords 13+retry; ListOffsets 13; Rust ListOffsets 13; Rust metadata_topics; language public reconnect |
| v0.116–v0.120 | **Shipped** — Go/Java metadata topics; Go/Java CreateTopic configs; OffsetFetchAll; CommitOffsets batch; Rust ListMembers 14 |
| v0.121–v0.125 | **Shipped** — language ListMembers 14; OffsetFetch entries; Python GroupConsumer batch commit; language Describe/ListGroups 14; Rust Describe/ListGroups 14 |
| v0.126–v0.130 | **Shipped** — Go CreateTopicID; JoinGroup instance; OffsetCommit metadata; produce default acks; Produce headers |
| v0.131–v0.135 | **Shipped** — JoinGroup rejoin; Produce timestamp; Produce headers+acks; language Heartbeat 14; Rust Heartbeat 14 |
| v0.136–v0.140 | **Shipped** — language LeaveGroup 14; Rust LeaveGroup 14; Produce timestamp+headers; Go OffsetCommit member; OffsetFetch metadata |
| v0.141–v0.145 | **Shipped** — Produce timestamp+acks; Produce timestamp+headers+acks; language Fetch knobs; Rust Fetch config; Fetch high watermark |
| v0.146–v0.150 | **Shipped** — Java JoinGroup member+instance; ProduceBatch default acks; OffsetFetch topic metadata; Rust fetch max_bytes; public InitProducerId |
| v0.151–v0.155 | **Shipped** — Rust InitProducerId; language DeleteRecords wait default; Rust OffsetCommit helpers; Rust OffsetFetch topic; Rust DeleteRecords wait (not Phase 155) |
| v0.156–v0.160 | **Shipped** — language Metadata 14; Rust Metadata 14; Go DeleteOffsetsAll; Rust fetch_offsets_all; producer id getters |
| v0.161–v0.165 | **Shipped** — Go ListAclsAll; Rust list_acls_all; Go ListOffsetsAll; language DeleteOffset; Rust delete_offset helpers |
| v0.166–v0.170 | **Shipped** — Rust list_offsets_all; Go ReassignAllPartitions; Rust reassign_partitions_all; language CreateAcl/DeleteAcl; Rust create_acl/delete_acl |
| v0.171–v0.175 | **Shipped** — Go AddBrokerNoRack; Rust add_broker_no_rack; Go CreateScramUserDefault; Rust create_scram_user_default; Rust commit_transaction_empty |
| v0.176–v0.180 | **Shipped** — Go CommitTransactionEmpty; language AlterConfig; Rust alter_config; language FetchOffset; Rust fetch_offset |
| v0.181–v0.185 | **Shipped** — language MetadataTopic; Rust metadata_topic; Go Addr; Go/Java assignor getter; Go SetEnableIdempotence |
| v0.186–v0.190 | **Shipped** — Go HeartbeatCount; Java heartbeatCount; Python heartbeat_count; Go/Java sessionTimeoutMs; Go/Java Leave |
| v0.191–v0.195 | **Shipped** — Go MaxRedirects; Go MaxRetries; Go RetryBackoff; Go TransactionalID; language timeout getter |
| v0.196–v0.200 | **Shipped** — Python list_acls_all; Python list_offsets_all; Python reassign_partitions_all; Go/Java CreateTopic default partitions=1; Go/Java auth token getter |
| v0.201–v0.203 | **Shipped** — Java heartbeatIntervalMs public; Go/Java SCRAM username getter; Rust create_topic_default |
| v0.207–v0.211 | **Shipped** — language/Rust GroupConsumer SyncGroup peek; first-Join client member_id; JoinGroup members trailer |
| v0.212–v0.216 | **Shipped** — overlay persist-after-joint; IsrUpdate skips 154; inbound 154 gated; SyncGroup generation fence; overlay Membership apply |

**How to use this file:** mark new work by phase number in ROADMAP + PHASE*_SPEC; fold completed rows into “Closed checklist”; keep “Still open” as the only honesty surface for operators and contributors.
