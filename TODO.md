# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–154** shipped (EOS↔durable atomic boundary + KRaft-style metadata Raft log).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–154 shipped**).  
Recent specs: [PHASE154](./docs/PHASE154_SPEC.md) · [PHASE153](./docs/PHASE153_SPEC.md) · [PHASE152](./docs/PHASE152_SPEC.md) · [PHASE151](./docs/PHASE151_SPEC.md).

---

## Shipped recently

### Phase 154 — KRaft-style metadata Raft log (MVP)
- [x] Ordered log `(term, index)` + AppendEntries opcodes **98/99**
- [x] Majority match_index → commit_index → apply SetAssignment
- [x] Durable `__metadata_raft/{log,hard_state}.json`
- [x] Env `VOLANT_METADATA_RAFT` (default on multi-node)
- [x] Metrics term/commit/last_applied/append success|fail
- [x] Tests `phase154_metadata_raft`
- **Honesty:** not full openraft election/InstallSnapshot; lowest-id controller still leader; static N

### Phase 153 — EOS + durable state atomic boundary
- [x] DurableStore staging checkpoint (begin/commit/abort)
- [x] EOS step: checkpoint → process → txn commit → durable commit_checkpoint
- [x] Abort path discards staged state
- [x] Tests `phase153_eos_durable_atomic` + 149/151 green
- **Honesty:** process-local staging, not distributed 2PC with broker

### Phase 152 / 151
- [x] Metadata committed-only; stream ExactlyOnce txn produce+offsets

---

## Open residual

| Pri | Item | Notes |
|----:|------|-------|
| Later | True openraft leader election + InstallSnapshot | Beyond 154 log MVP |
| Later | Dynamic membership reconfiguration | Static N only |
| Later | Distributed EOS 2PC with broker-held state | Beyond process-local checkpoint |
| Later | Multi-lang / chaos / perf | Ecosystem |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 154 | **Shipped** — KRaft-style metadata log MVP |
| Phase 153 | **Shipped** — EOS durable checkpoint boundary |
| Prior P2/P3/product slices | **None open** for listed residuals |

**Default next slice:** openraft election, multi-lang, or chaos/perf.
