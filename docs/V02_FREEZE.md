# Volant v0.2 product freeze

| Field | Value |
|-------|-------|
| Status | Shipped |
| Date | 2026-08-14 |
| Author | Volant maintainers |
| Ceiling | Phases 0–154 shipped |
| Crate | 0.2.0 (workspace `Cargo.toml`) |
| SoT | this document |

## 1. Decision

v0.2 ships the Phase 6 product: lowest-id controller, `{data_dir}/cluster/assignment.json` as metadata SoT, and the ISR data plane. Homemade metadata Raft (150/152/154) stays in-tree behind flags and is **not** the next slice. The Kafka shim is frozen at HEAD `SUPPORTED_APIS` (38 keys). CreateTopic success is “controller wrote `assignment.json`”; brief Metadata staleness on failover is documented honesty, not a bug.

## 2. What v0.2 IS

| In | Meaning (HEAD) |
|----|----------------|
| Native produce / fetch | mmap log (`volant-storage`); acks 0/1/all; HWM = min ISR LEO |
| Groups | Join / heartbeat / leave / offsets; sticky + cooperative MVP |
| Static ISR `acks=all` | Acknowledged data survives leader kill when `min_insync_replicas ≥ 2` |
| In-process streams | ALO + process-local EOS (149/151/153); optional [`TumblingWindow::durable`](../crates/volant-stream/src/window.rs) buckets |
| Security MVP | Token, TLS/mTLS, SCRAM, ACLs |
| One-binary ops | Metrics, TLS, Helm (`deploy/`) |
| Kafka shim | Optional `--kafka-listen`; 38 keys frozen at HEAD |

## 3. Frozen

| Item | Freeze |
|------|--------|
| Homemade Raft election | No RequestVote, term contests, or leader campaigns. Controller = `Membership::controller_id` (lowest live id). |
| InstallSnapshot / compaction | 154 log may remain; **stop extending**. No snapshot install, no compaction. |
| Metadata SoT | Phase 6 `assignment.json` + live assignment — not the 152 committed snapshot. |
| Kafka `SUPPORTED_APIS` | 38 keys (`kafka/mod.rs`). No new keys, no version ratchets, no session/txn/preferred depth unless a real client is proven broken. |
| Distributed EOS | 153 is process-local staging. Not broker-held 2PC. |
| Durable-window *promise* | In-process buckets landed (`TumblingWindow::durable`). Do not claim cluster / distributed window durability. |
| Dynamic membership / KIP-890 / preferred TCP probe / multi-lang / published SLAs | Out of v0.2. SLAs wait on measured benches (published; aspirational rows demoted). |
| Phase 155 | Do not open. See §7. |

## 4. Metadata story (choice A)

**SoT:** Phase-6 lowest-id controller + `{data_dir}/cluster/assignment.json` + ISR data plane. `CreateTopic` success = controller `save_assignment`. Metadata may lag a new controller briefly; that is allowed (`docs/consistency.md`).

| Knob | HEAD default | v0.2 shipped default | Role |
|------|--------------|----------------------|------|
| `VOLANT_METADATA_RAFT` | **off** (`broker/mod.rs`) | **off** | 154 AppendEntries 98/99. Code may stay; do not extend. |
| `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` | **off** (`broker/mod.rs`) | **off** | 152 Metadata = committed snapshot when `assignment_consensus_enabled && assignment_metadata_committed_only` (150+152, `broker/cluster.rs`). Default is live Metadata. |
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** | **on** (best-effort) | ClusterState-style push, opcodes 96/97. Must **not** gate Metadata or fail CreateTopic. |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** | **off** | Must stay off. |

HEAD path (do not grow): Raft preferred when on (`net/fanout.rs`); CreateTopic mutate-first (`net/dispatch.rs`). `maybe_fanout_assignment_consensus` returns `Some(false)` only when miss **and** wait or committed-only; `!must_wait` returns `None` so a 96/97 miss does **not** fail the client. Homemade Raft has no election (`cluster/metadata_raft.rs`).

Post-v0.2 the **only** allowed quorum bet is **replace** 150/152/154 with **openraft** — not finish homemade Raft.

## 5. Kafka shim freeze

| Band | Content |
|------|---------|
| IN | HEAD `SUPPORTED_APIS` (38 keys): Produce 0–13, Fetch 0–18, Metadata 0–13, groups, SASL, txn MVP, ACL admin, configs, DescribeCluster / DescribeProducers / Describe+ListTransactions. |
| FROZEN | No new API keys. No max-version ratchets. No session / txn / preferred depth unless a real client is proven broken. |
| Do not claim | librdkafka, kafka-python, kcat, or Java client compatibility. CI is `cargo test --workspace` + protocol fuzz corpus (`.github/workflows/ci.yml`). Shim tests use `boot_kafka` + codec (`phase23_kafka_shim.rs`), not those clients. |

## 6. Shipped (this order; max 5)

1. **Flip metadata defaults + ungate CreateTopic + docs honesty** — choice A defaults; `!must_wait` must not fail the client; update `consistency.md` / `ops.md`.
2. **Storage truth** — re-ran `volant-bench`; publish measured rows; demote aspirational numbers (`ROADMAP.md` performance table).
3. **ISR / chaos confidence** — leader kill + `acks=all`; follower death/rejoin; controller death; N=2 `majority_impossible`; close test/runbook gaps.
4. **Split `broker.rs` / `net.rs`** — now `broker/mod.rs` + `net/{dispatch,fanout}.rs`. Structural, not a feature.
5. **Streams durable window buckets** — in-process `TumblingWindow::durable`; no distributed 2PC.

## 7. Do not open as Phase 155

RequestVote · InstallSnapshot · openraft-now · new Kafka API · dynamic membership · KIP-890 · multi-lang · distributed streams · preferred TCP probe · session Raft registry.

Leftover TODO/ROADMAP lists are **not** a license to open Phase 155.

## Key Decisions

- **Choice A is v0.2 SoT.** Phase 6 controller + `assignment.json` is what operators can reason about. Committed-only Metadata is 150+152 (`assignment_consensus_enabled && assignment_metadata_committed_only`), not “both 154/152.”
- **Stop extending homemade Raft.** 154 has no election (`cluster/metadata_raft.rs`). Finishing RequestVote + snapshot is a multi-phase trap; the only later quorum bet is replace-with-openraft.
- **`VOLANT_ASSIGNMENT_CONSENSUS` stays on as push, not as gate.** Opcodes 96/97 may still fan out. `maybe_fanout_assignment_consensus` returns `None` (ignore) when `!must_wait`, including on a 96/97 miss. Do not turn consensus off to paper over this.
- **Kafka surface is frozen at 38 keys.** Breadth without real-client CI is a claim we will not make. No ratchet unless a proven client break.
- **v0.2 work was ops/honesty, not a 155 feature.** Defaults, benches, ISR chaos, file split, then in-process windows.
- **Distributed EOS is not a v0.2 claim.** 153 is process-local staging. In-process durable window buckets shipped; they are not cluster EOS.

## Alternatives Considered

| Option | What | Why not (or why yes) |
|--------|------|----------------------|
| **A — keep Phase 6 (chosen)** | Lowest-id controller + `assignment.json` + ISR. 154 optional, defaults off. | Matches shipped data-plane honesty. CreateTopic = local write. Operators already run this when flags are off. |
| **C — grow 154** | RequestVote, InstallSnapshot, compaction, term contests on `__metadata_raft`. | Homemade Raft without election is not “almost done.” CreateTopic is already mutate-first. High cost, still not openraft/KRaft. |
| **B — openraft now** | Replace 150/152/154 with openraft embed as the v0.2 bet. | Correct long-term quorum, wrong next slice: new crate, new failure model, blocked items 1–3. Allowed **after** v0.2 as a replace, not a finish. |

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
- `crates/volant-broker/src/cluster/{membership,state,metadata_raft}.rs`
- `crates/volant-broker/src/broker/mod.rs` — flag defaults
- `crates/volant-broker/src/net/{dispatch,fanout}.rs` — CreateTopic fan-out
- `crates/volant-broker/src/kafka/mod.rs` — `SUPPORTED_APIS`
- `.github/workflows/ci.yml` — `cargo test` + corpus smoke
