# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–154** + residuals **v0.3–v0.50**.  
**Last review:** 2026-09-02  

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
| **Product: consensus / KRaft-style metadata** | **MVP closed** (150, 152, 154); overlay **v0.10**; openraft election **v0.11** (opt-in); `__cluster_metadata` **v0.12** |

**Ceiling:** Phases **0–154**. Next free inter-broker opcode after **114/115** is **116+**.

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
| **Later** | **Multi-language clients** | **Python/Go/Java GroupConsumer v0.31–v0.33 + static v0.36 + bg heartbeat v0.37 + range assignor v0.41 + Auth v0.42 + redirect v0.43 + SCRAM v0.46 + idempotent produce v0.47 + auto-commit v0.48 + List/DescribeGroup v0.49 + ListOffsets v0.50**; not kafka-python / SyncGroup / txn produce |
| **Later** | **Long fuzz + chaos-mesh** | **MVP closed (v0.15)** — extended corpus + Chaos Mesh YAML + A→B isolate |
| **Later** | **Perf campaign** vs aspirational targets | **closed (v0.2 PR2)** — measured table published; group-commit **v0.20** (opt-in, no new bench) |

**Default next slice:** language-client CreatePartitions / DeleteRecords / Describe-AlterConfigs / DeleteOffsets / SCRAM admin, or openraft RocksDB if redb is not enough. Homemade Raft election / InstallSnapshot-on-154 / Phase 155 is **not** the next product bet. Do not open Phase 155.

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
- [x] Multi-language clients — Python/Go/Java GroupConsumer (v0.31–v0.33) + static (v0.36) + bg heartbeat (v0.37) + range assignor (v0.41) + Auth (v0.42) + redirect (v0.43) + SCRAM (v0.46) + idempotent produce (v0.47) + auto-commit (v0.48) + List/DescribeGroup (v0.49) + ListOffsets (v0.50); not kafka-python / SyncGroup / txn produce
- [x] Long fuzz campaigns + chaos-mesh MVP (v0.15; corpus + YAML + A→B isolate; not multi-hour CI)
- [x] Published perf numbers vs aspirational table; group-commit **v0.20** (opt-in)

### Wait-off / best-effort paths (by design)
- DeleteRecords **wait off**: cluster upgrades to wait-on unless **both** `VOLANT_DELETE_RECORDS_ALLOW_IRREVERSIBLE=1` **and** `VOLANT_DELETE_RECORDS_IRREVERSIBLE_ACK=1` (v0.45). Single-node wait-off, and both-envs-on, are still irreversible
- Session mirror: dual unclaimed primary (v0.25) + mirror-only (v0.30) converge on MirrorPut / helper; still not Raft
- Metadata raft wait-commit default **on** (v0.40): CreateTopic waits for `commit_index` else rollback + **15**; escape `VOLANT_METADATA_RAFT_WAIT_COMMIT=0` (Phase 154 tests)
- Local `assignor="range"` is solo-member only (JoinGroup has no member list / no SyncGroup)
- Language-client SCRAM is **SHA-256** only (v0.46); not Kafka SASL; Create/Delete/ListScramUsers stay Rust/CLI
- Idempotent produce is native **32/33** with empty `transactional_id` (v0.47); no BeginTxn / EndTxn
- Auto-commit is poll-tied and default **off** (v0.48); not Kafka `enable.auto.commit`; Rust GroupConsumer is still explicit-only
- ListOffsets is native **48/49** (v0.50); `latest` is LEO; no isolation / timestamp. CreatePartitions, DeleteRecords, Describe/AlterConfigs, DeleteOffsets stay Rust/CLI

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

**How to use this file:** mark new work by phase number in ROADMAP + PHASE*_SPEC; fold completed rows into “Closed checklist”; keep “Still open” as the only honesty surface for operators and contributors.
