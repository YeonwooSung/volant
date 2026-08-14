# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–154** shipped.  
**Last review:** 2026-08-13  

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
| **Product: streams durable + EOS** | **MVP closed** (149, 151, 153) |
| **Product: consensus / KRaft-style metadata** | **MVP closed** (150, 152, 154) |

**Ceiling:** Phases **0–154**. Next free inter-broker opcode after **98/99** is **100+**.

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
| **P2** | **Kafka admin assignment wait/rollback** | **this slice (v0.4)** — CreateTopics/DeleteTopics/CreatePartitions share native `complete_assignment_mutation` (Kafka **19**) |
| **P3** | **Distributed EOS 2PC** (broker-held stream state) | 153 is **process-local** staging only |
| **P3** | **Durable window buckets** | **closed (v0.2 PR5)** — `TumblingWindow::durable`; still process-local |
| **P3** | Preferred **throttling / TCP probe** | Beyond 140/144/145 |
| **Later** | Full **openraft** crate integration | Replace custom `__metadata_raft` when ready |
| **Later** | **Dynamic membership** reconfiguration | Static `cluster.toml` N only |
| **Later** | Full **KIP-890 / `__transaction_state`** | Txn depth beyond 2PC MVP |
| **Later** | **Multi-language clients** | Ecosystem |
| **Later** | **Long fuzz + chaos-mesh** | Phase 112 is corpus smoke only |
| **Later** | **Perf campaign** vs aspirational targets | **closed (v0.2 PR2)** — measured table published; aspirational demoted; no group-commit |

**Default next slice:** v0.4 Kafka admin wait/rollback (this slice). Homemade Raft election / InstallSnapshot is **not** the next product bet. Do not open Phase 155.

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

---

## Still open (honest limitations)

### Metadata / consensus
- [ ] True **openraft** leader election + term contests (154: lowest-id controller)
- [ ] **InstallSnapshot** / log truncation for metadata Raft
- [ ] **Dynamic membership** (add/remove brokers without static N)
- [x] Rollback **local** assignment file when wait/committed-only majority misses (v0.3 residual; `!must_wait` still retains local)
- [x] Kafka CreateTopics / DeleteTopics / CreatePartitions honor the same wait/rollback (v0.4; majority miss → Kafka **19**)
- [ ] Per-partition Raft / full KRaft `__cluster_metadata` topic parity

### Streams
- [ ] **Distributed** EOS (state coordinated with broker, not only process staging)
- [x] Durable **window** state (in-process `TumblingWindow::durable`; not cluster EOS)
- [ ] Exactly-once with **cross-app** fencing beyond single `transactional_id`

### Kafka / txn / ops
- [ ] Full **KIP-890 / 939** / `__transaction_state`
- [ ] Kafka DeleteRecords **per-request** wait flag (native has trailer; Kafka env-only)
- [ ] Full preferred selector **throttling** / TCP probe
- [ ] Multi-language clients
- [ ] Long fuzz campaigns + chaos-mesh
- [x] Published perf numbers vs aspirational table; group-commit **not** implemented

### Wait-off / best-effort paths (by design)
- DeleteRecords **wait off**: still local-first (irreversible truncate)
- Session mirror: dual-epoch if two peers serve without promote (147)
- Metadata raft **uncommitted** local mutate may lead commit until majority (Metadata committed-only hides when on)

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

**How to use this file:** mark new work by phase number in ROADMAP + PHASE*_SPEC; fold completed rows into “Closed checklist”; keep “Still open” as the only honesty surface for operators and contributors.
