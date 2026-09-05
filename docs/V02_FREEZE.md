# Volant v0.2 product freeze

| Field | Value |
|-------|-------|
| Status | Shipped |
| Date | 2026-08-14 |
| Author | Volant maintainers |
| Ceiling | Phases 0–154 shipped; **Phase 155 open** |
| Crate | 0.2.0 (workspace `Cargo.toml`) |
| SoT | this document + [PHASE155_SPEC.md](./PHASE155_SPEC.md) |

## 1. Decision

v0.2 shipped the Phase 6 product: lowest-id controller,
`{data_dir}/cluster/assignment.json` as metadata SoT, and the ISR data
plane. That remains the **v0.2 shipped** story and the single-node
path.

**Phase 155** (open) replaces the cluster quorum bet: homemade
150/152/154 is **not** finished. Cluster mode defaults to **openraft**
(`VOLANT_OPENRAFT_METADATA` on). CreateTopic success is
`client_write(SetAssignment)` commit+apply. `assignment.json` is the
apply artifact. Homemade 154 hatch is **deleted** (v0.222): no
`metadata_raft.rs`, inbound 98 always disabled, leftover
`__metadata_raft/` unread. Protocol 98/99 still decode.

The Kafka shim is HEAD `SUPPORTED_APIS` (**91 keys**, v0.291). SyncGroup
API key **14** was already in the 38-key table; Phase 155 added
**native** opcodes 116/117, not a Kafka key. Residuals added
AlterPartitionReassignments **45** v0 (v0.225) and
ListPartitionReassignments **46** v0 (v0.228).

## 2. What v0.2 IS

| In | Meaning (HEAD) |
|----|----------------|
| Native produce / fetch | mmap log (`volant-storage`); acks 0/1/all; HWM = min ISR LEO |
| Groups | Join / heartbeat / leave / offsets; sticky + cooperative MVP |
| Static ISR `acks=all` | Acknowledged data survives leader kill when `min_insync_replicas ≥ 2` |
| In-process streams | ALO + process-local EOS (149/151/153); optional [`TumblingWindow::durable`](../crates/volant-stream/src/window.rs) buckets |
| Security MVP | Token, TLS/mTLS, SCRAM, ACLs |
| One-binary ops | Metrics, TLS, Helm (`deploy/`) |
| Kafka shim | Optional `--kafka-listen`; 91 keys at HEAD |

## 3. Frozen

| Item | Freeze |
|------|--------|
| Homemade Raft election | Hatch **deleted** (v0.222). No RequestVote, term contests, or leader campaigns. Controller = `Membership::controller_id` (lowest live id) when openraft is off. |
| InstallSnapshot / compaction | Homemade 154 module gone. Openraft InstallSnapshot is v0.17. Leftover `__metadata_raft/` unread. |
| Metadata SoT (v0.2 / single-node) | Phase 6 `assignment.json` + live assignment — not the 152 committed snapshot. |
| Metadata SoT (Phase 155 cluster) | Openraft committed `SetAssignment`. See [PHASE155_SPEC.md](./PHASE155_SPEC.md). |
| Kafka `SUPPORTED_APIS` | 91 keys (`kafka/mod.rs`), including SyncGroup **14**, WriteTxnMarkers **27**, AlterReplicaLogDirs **34** (reject), Create/Renew/Expire/DescribeDelegationToken **38**/**39**/**40**/**41**, DescribeLogDirs **35**, ElectLeaders **43**, quotas **48**/**49** (empty/reject), Vote **52** (reject), BeginQuorumEpoch **53** (reject), EndQuorumEpoch **54** (reject), Alter/ListPartitionReassignments **45**/**46**, Describe/AlterUserScramCredentials **50**/**51**, DescribeQuorum **55**, AlterPartition **56**, UpdateFeatures **57** (reject), Envelope **58** (reject), FetchSnapshot **59** (reject), BrokerRegistration **62** (reject), BrokerHeartbeat **63** (reject), UnregisterBroker **64**, AllocateProducerIds **67**, ConsumerGroupHeartbeat **68** (reject), ConsumerGroupDescribe **69**, ControllerRegistration **70** (reject), GetTelemetrySubscriptions **71**, PushTelemetry **72** (reject), AssignReplicasToDirs **73** (reject), ListClientMetricsResources **74**, DescribeTopicPartitions **75**, ShareGroupHeartbeat **76** (reject), ShareGroupDescribe **77** (reject), ShareFetch **78** (reject), ShareAcknowledge **79** (reject), Add/Remove/UpdateRaftVoter **80**/**81**/**82** (reject), InitializeShareGroupState **83** (reject), Read/Write/DeleteShareGroupState **84**/**85**/**86** (reject), ReadShareGroupStateSummary **87** (reject), StreamsGroupHeartbeat/Describe **88**/**89** (reject), DescribeShareGroupOffsets **90** (reject), Alter/DeleteShareGroupOffsets **91**/**92** (reject), StreamsGroupTopologyDescriptionUpdate **93** (reject), UnregisterController **94** (reject). Native 116/117 is not a Kafka key. |
| Distributed EOS | 153 is process-local staging. Not broker-held 2PC. |
| Durable-window *promise* | In-process buckets landed (`TumblingWindow::durable`). Do not claim cluster / distributed window durability. |
| Dynamic membership / full KIP-890/939 / preferred TCP probe / published SLAs | Overlay `membership.json` stays membership SoT. Txn-state Kafka TransactionLog v0 is opt-in (v0.229). |
| Phase 155 | **Open.** Scope locked in [PHASE155_SPEC.md](./PHASE155_SPEC.md). Not a license for homemade RequestVote or new Kafka keys. |

## 4. Metadata story (choice A)

**SoT:** Phase-6 lowest-id controller + `{data_dir}/cluster/assignment.json` + ISR data plane. `CreateTopic` success = controller `save_assignment`. Metadata may lag a new controller briefly; that is allowed (`docs/consistency.md`).

| Knob | HEAD default | v0.2 shipped default | Role |
|------|--------------|----------------------|------|
| `VOLANT_METADATA_RAFT` | **ignored** (v0.222) | **off** | Warn-once if set on. Inbound 98 always disabled. |
| `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` | **off** (`broker/mod.rs`) | **off** | 152 Metadata = committed snapshot when `assignment_consensus_enabled && assignment_metadata_committed_only` (150+152, `broker/cluster.rs`). Default is live Metadata. |
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** | **on** (best-effort) | ClusterState-style push, opcodes 96/97. Must **not** gate Metadata or fail CreateTopic. |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** | **off** | Must stay off. |

HEAD path (do not grow): openraft preferred when on (`net/fanout.rs`); CreateTopic mutate-first (`net/dispatch.rs`). `maybe_fanout_assignment_consensus` is openraft → else Phase 150 notes. Homemade 154 hatch is deleted (`cluster/metadata_raft.rs` gone, v0.222).

Post-v0.2 the **only** allowed quorum bet is **replace** 150/152/154 with **openraft** — not finish homemade Raft. That replace is **Phase 155**.

## 5. Kafka shim freeze

| Band | Content |
|------|---------|
| IN | HEAD `SUPPORTED_APIS` (91 keys): Produce 0–13, Fetch 0–18, Metadata 0–13, groups **including SyncGroup 14 v0–5**, SASL, txn MVP, ACL admin, configs, DescribeCluster / DescribeProducers / Describe+ListTransactions, **WriteTxnMarkers 27**, **AlterReplicaLogDirs 34** (reject), **Create/Renew/Expire/DescribeDelegationToken 38/39/40/41**, **DescribeLogDirs 35**, **ElectLeaders 43**, **quotas 48/49** (empty/reject), **Vote 52** (reject), **BeginQuorumEpoch 53** (reject), **EndQuorumEpoch 54** (reject), **Alter/ListPartitionReassignments 45/46**, **SCRAM 50/51**, **DescribeQuorum 55**, **AlterPartition 56**, **UpdateFeatures 57** (reject), **Envelope 58** (reject), **FetchSnapshot 59** (reject), **BrokerRegistration 62** (reject), **BrokerHeartbeat 63** (reject), **UnregisterBroker 64**, **AllocateProducerIds 67**, **ConsumerGroupHeartbeat 68** (reject), **ConsumerGroupDescribe 69**, **ControllerRegistration 70** (reject), **GetTelemetrySubscriptions 71**, **PushTelemetry 72** (reject), **AssignReplicasToDirs 73** (reject), **ListClientMetricsResources 74**, **DescribeTopicPartitions 75**, **ShareGroupHeartbeat 76** (reject), **ShareGroupDescribe 77** (reject), **ShareFetch 78** (reject), **ShareAcknowledge 79** (reject), **Add/Remove/UpdateRaftVoter 80/81/82** (reject), **InitializeShareGroupState 83** (reject), **Read/Write/DeleteShareGroupState 84/85/86** (reject), **ReadShareGroupStateSummary 87** (reject), **StreamsGroupHeartbeat/Describe 88/89** (reject), **DescribeShareGroupOffsets 90** (reject), **Alter/DeleteShareGroupOffsets 91/92** (reject), **StreamsGroupTopologyDescriptionUpdate 93** (reject), **UnregisterController 94** (reject). |
| FROZEN | No further keys. No max-version ratchets. No session / txn / preferred depth unless a real client is proven broken. |
| Do not claim | librdkafka, kafka-python, kcat, or Java client compatibility. CI is `cargo test --workspace` + protocol fuzz corpus (`.github/workflows/ci.yml`). Shim tests use `boot_kafka` + codec (`phase23_kafka_shim.rs`), not those clients. |

## 6. Shipped (this order; max 5)

1. **Flip metadata defaults + ungate CreateTopic + docs honesty** — choice A defaults; `!must_wait` must not fail the client; update `consistency.md` / `ops.md`.
2. **Storage truth** — re-ran `volant-bench`; publish measured rows; demote aspirational numbers (`ROADMAP.md` performance table).
3. **ISR / chaos confidence** — leader kill + `acks=all`; follower death/rejoin; controller death; N=2 `majority_impossible`; close test/runbook gaps.
4. **Split `broker.rs` / `net.rs`** — now `broker/mod.rs` + `net/{dispatch,fanout}.rs`. Structural, not a feature.
5. **Streams durable window buckets** — in-process `TumblingWindow::durable`; no distributed 2PC.

## 7. Phase 155 (open) — what it is / is not

**In (see [PHASE155_SPEC.md](./PHASE155_SPEC.md)):** openraft as
cluster metadata SoT; native SyncGroup **116/117** peek; JoinGroup
retry when `member_id` or instance id is set; Go `CreateTopic`
returns the topic id.

**Still out of 155:** homemade RequestVote / 154 (hatch **deleted**
v0.222) · overlay-as-raft-membership · full KIP-890/939 / TV2 ·
PreparingRebalance · live reassignment progress · distributed streams ·
preferred TCP probe · session Raft registry.

Leftover TODO/ROADMAP lists are **not** a license to grow homemade
154 or invent Kafka keys beyond the approved 64.

## Key Decisions

- **Choice A is the v0.2 shipped SoT** (single-node and `VOLANT_OPENRAFT_METADATA=0`). Phase 6 controller + `assignment.json`. Committed-only Metadata is 150+152.
- **Phase 155 cluster SoT is openraft.** Unset env → on when `--cluster-config` is set. CreateTopic waits on `client_write`. Homemade 154 hatch is deleted (v0.222).
- **Stop extending homemade Raft.** Module gone. Finishing RequestVote + snapshot stays rejected.
- **`VOLANT_ASSIGNMENT_CONSENSUS` stays on as push** when openraft is off. Openraft-on cluster uses `client_write` as the gate.
- **Kafka surface is 91 keys** (… + v0.285 **88**; v0.286 **89**; v0.287 **91**; v0.288 **92**; v0.289 **53**; v0.290 **54**; v0.291 **93**). SyncGroup **14** was already listed. Native 116/117 is not a Kafka key.
- **Distributed EOS is not a v0.2 or 155 claim.** 153 is process-local staging.

## Alternatives Considered

| Option | What | Why not (or why yes) |
|--------|------|----------------------|
| **A — keep Phase 6 (chosen)** | Lowest-id controller + `assignment.json` + ISR. 154 optional, defaults off. | Matches shipped data-plane honesty. CreateTopic = local write. Operators already run this when flags are off. |
| **C — grow 154** | RequestVote, InstallSnapshot, compaction, term contests on `__metadata_raft`. | Homemade Raft without election is not “almost done.” CreateTopic is already mutate-first. High cost, still not openraft/KRaft. |
| **B — openraft now** | Replace 150/152/154 with openraft embed. | Rejected as the *v0.2* bet. **Accepted as Phase 155** (after v0.2 leftovers closed). |

## PR Plan

Merged (independently; product priority, not a git stack).

| PR | Scope | Status |
|----|-------|--------|
| 1 | Flip `default_metadata_raft_enabled` → false; `default_assignment_metadata_committed_only` → false. Fix `maybe_fanout_assignment_consensus`: completed fan-out with `!must_wait` returns `None` so handlers do not fail the client (`net/fanout.rs`). Keep `VOLANT_ASSIGNMENT_CONSENSUS` on; wait stays off. Docs: this freeze + `consistency.md` + `ops.md`. Miss-path test: cluster, raft off, committed-only off, wait off, 96/97 miss (N=2 one dead) → CreateTopic / DeleteTopic / CreatePartitions `error_code=0` and `assignment.json` written. | Merged |
| 2 | Re-run `volant-bench` (release). Record numbers. Decide group-commit vs current flush. Publish or demote ROADMAP aspirational table. | Merged — published measured; aspirational demoted. No group-commit. |
| 3 | ISR/chaos: leader kill + `acks=all`; follower death/rejoin; controller death; N=2 majority_impossible. Tests + `ops.md` runbook. | Merged |
| 4 | Split `broker.rs` / `net.rs` into modules. No protocol or flag change. | Merged — `broker/mod.rs`, `net/dispatch.rs`, `net/fanout.rs` |
| 5 | In-process durable window buckets (replace `TumblingWindow` `HashMap`). No distributed 2PC. | Merged — `TumblingWindow::durable` |

## Open Questions

Closed: after PR 2 benches, product owner **published** the measured `volant-bench` table and **demoted** aspirational ROADMAP rows. This freeze does not invent SLAs.

## References

- `docs/consistency.md` — HWM / ISR / acks; refuses linearizable metadata
- `docs/PHASE6_SPEC.md` — static membership + lowest-id controller
- `docs/PHASE150_SPEC.md` / `PHASE152_SPEC.md` / `PHASE154_SPEC.md` — majority notes, committed snapshot, homemade log (frozen)
- `docs/PHASE153_SPEC.md` — process-local EOS staging
- `docs/KAFKA_COMPAT.md` — shim matrix (frozen at HEAD)
- `crates/volant-broker/src/cluster/{membership,state}.rs` (homemade `metadata_raft.rs` deleted v0.222)
- `crates/volant-broker/src/broker/mod.rs` — flag defaults
- `crates/volant-broker/src/net/{dispatch,fanout}.rs` — CreateTopic fan-out
- `crates/volant-broker/src/kafka/mod.rs` — `SUPPORTED_APIS`
- `.github/workflows/ci.yml` — `cargo test` + corpus smoke
