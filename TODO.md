# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–150** shipped (durable stream state + assignment majority consensus).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–150 shipped**).  
Recent specs: [PHASE150](./docs/PHASE150_SPEC.md) · [PHASE149](./docs/PHASE149_SPEC.md) · [PHASE148](./docs/PHASE148_SPEC.md) · [PHASE147](./docs/PHASE147_SPEC.md).

---

## Shipped recently

### Phase 150 — Assignment majority consensus (MVP)
- [x] Opcodes **96/97** `AssignmentConsensusNote`; configured-N majority
- [x] Durable `__assignment_consensus/state.json` committed/pending gen
- [x] Fan-out after create/delete topic / create partitions (+ IsrUpdate best-effort)
- [x] Env `VOLANT_ASSIGNMENT_CONSENSUS` (default on) / `_WAIT` (default off)
- [x] Metrics success/fail + committed_generation gauge
- [x] Tests `phase150_assignment_consensus`
- **Honesty:** not full openraft/KRaft; Metadata may lead committed_gen; static membership

### Phase 149 — Durable stream state store
- [x] `DurableStore` via **redb** (`KeyValueStore`); Immediate commit durability
- [x] `count_reduce_durable` / `StreamBuilder::state_dir` + `reduce_count_durable`
- [x] Tests `phase149_durable_state` (CRUD, restart, reduce)
- **Honesty:** durable state ≠ exactly-once; at-least-once runtime unchanged

### Phases 145–148 (P3)
- [x] Rack-aware assignment; delta MirrorPut; serve-from-mirror; defer truncate wait

### Phases 141–144 (P2)
- [x] Majority gauges; Metadata ISR; promote claim; preferred×session suppress

---

## P1 / P2 / listed-P3 residual

_(none open)_

---

## Next candidates

| Pri | Item | Notes |
|----:|------|-------|
| Later | Full openraft / KRaft metadata + dynamic membership | Beyond 150 majority notes |
| Later | Exactly-once streams / transactional sinks | Beyond durable state |
| Later | Durable window buckets | Phase 149 is reduce KV only |
| Later | Full KIP-890 / `__transaction_state` | Txn depth |
| Later | Multi-language clients | Ecosystem |
| Later | Long fuzz + chaos-mesh | Phase 112 smoke only |
| Later | Perf campaign | Publish numbers |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 150 | **Shipped** — assignment majority consensus MVP |
| Phase 149 | **Shipped** — redb durable stream KV |
| P0–P3 listed | **None open** |

**Default next slice:** exactly-once streams, full openraft, multi-lang, or hardening (chaos/perf).
