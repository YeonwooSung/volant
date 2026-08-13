# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–153** shipped (EOS + durable checkpoint + consensus-depth Metadata).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–153 shipped**).  
Recent specs: [PHASE153](./docs/PHASE153_SPEC.md) · [PHASE152](./docs/PHASE152_SPEC.md) · [PHASE151](./docs/PHASE151_SPEC.md) · [PHASE149](./docs/PHASE149_SPEC.md).

---

## Shipped recently

### Phase 153 — EOS + durable stream state atomic boundary
- [x] `KeyValueStore` checkpoint defaults (`begin` / `commit` / `abort` / `in_checkpoint`)
- [x] `DurableStore` staging overlay; single Immediate txn on commit; abort discards
- [x] `Operator` / `Reduce` / `Pipeline` checkpoint hooks
- [x] EOS step: begin_checkpoint → process → EndTxn → commit_checkpoint (abort on fail/empty)
- [x] ALO path: no checkpoint (immediate durable put)
- [x] Tests `phase153_eos_durable_atomic` + phase149/151 green
- **Honesty:** process-local staging, not distributed 2PC with broker

### Phase 152 — Assignment consensus depth
- [x] Durable committed assignment snapshot for Metadata SoT
- [x] `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` (default **on**): Metadata from committed snap
- [x] Admin create/delete/partitions wait-like when committed_only (majority fail → 15)
- [x] Metrics generation_lag + committed_only gauge
- [x] Tests `phase152_consensus_depth` + phase150 green
- **Honesty:** local uncommitted assignment not rolled back; not full KRaft

### Phase 151 — Stream exactly-once (EOS) MVP
- [x] `ProcessingGuarantee::ExactlyOnce { transactional_id }`
- [x] EOS step: begin → produce → add_offsets → commit_transaction
- [x] ALO default unchanged; builder `exactly_once(id)`
- [x] Tests `phase151_exactly_once` (live + empty step)
- **Honesty:** write-through txns + soft markers; Phase 153 stages durable state after EndTxn

### Phase 150 / 149
- [x] Assignment majority notes; redb durable stream KV

---

## Open residual

| Pri | Item | Notes |
|----:|------|-------|
| Later | Full openraft / KRaft + dynamic membership | Beyond 150/152 |
| Later | Local assignment rollback on consensus fail | Phase 152 residual |
| Later | Distributed 2PC durable state ↔ broker | Phase 153 residual (local staging only) |
| Later | Full KIP-890 / multi-lang / chaos / perf | Ecosystem & hardening |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 153 | **Shipped** — EOS + durable checkpoint boundary |
| Phase 152 | **Shipped** — Metadata serves committed assignment |
| Phase 151 | **Shipped** — stream EOS via Volant transactions |
| P0–P3 listed earlier | **None open** |

**Default next slice:** openraft depth (Phase 154 sibling), multi-lang, or chaos/perf hardening.
