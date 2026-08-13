# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–154** shipped (KRaft-style metadata log MVP).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–154 shipped**).  
Recent specs: [PHASE154](./docs/PHASE154_SPEC.md) · [PHASE152](./docs/PHASE152_SPEC.md) · [PHASE151](./docs/PHASE151_SPEC.md) · [PHASE150](./docs/PHASE150_SPEC.md).

---

## Shipped recently

### Phase 154 — KRaft-style metadata Raft log (MVP)
- [x] Durable `{data_dir}/__metadata_raft/{log,hard_state}.json` with `(term, index)`
- [x] Opcodes **98/99** `MetadataRaftAppend` (simplified AppendEntries)
- [x] Majority match_index → `commit_index`; apply only when commit advances
- [x] `VOLANT_METADATA_RAFT` default **on** cluster / **off** single-node
- [x] Prefer Raft over AssignmentConsensusNote when enabled; Phase 152 snap updated
- [x] Metrics term / commit_index / last_applied / append_{success,fail}_total
- [x] Tests `phase154_metadata_raft` + phase150/152 + protocol green
- **Honesty:** not full openraft election/snapshot; lowest-id controller; static N

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
- **Honesty:** write-through txns + soft markers; durable state not in same atomic txn

### Phase 150 / 149
- [x] Assignment majority notes; redb durable stream KV

---

## Open residual

| Pri | Item | Notes |
|----:|------|-------|
| Later | Full openraft election + InstallSnapshot + dynamic membership | Phase 154 residual |
| Later | EOS + durable state single atomic boundary | Phase 151 residual |
| Later | Local assignment rollback on consensus fail | Phase 152 residual |
| Later | Full KIP-890 / multi-lang / chaos / perf | Ecosystem & hardening |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 154 | **Shipped** — KRaft-style metadata log MVP (not full openraft) |
| Phase 152 | **Shipped** — Metadata serves committed assignment |
| Phase 151 | **Shipped** — stream EOS via Volant transactions |
| P0–P3 listed earlier | **None open** |

**Default next slice:** openraft election depth, multi-lang, or chaos/perf hardening.
