# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–145 + 148** (rack-aware assignment + defer truncate majority). Phases 146–147 merging next.  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md).  
Recent specs: [PHASE148](./docs/PHASE148_SPEC.md) · [PHASE145](./docs/PHASE145_SPEC.md) · [PHASE144](./docs/PHASE144_SPEC.md) · [PHASE143](./docs/PHASE143_SPEC.md).

---

## Shipped recently

### Phase 148 — Defer local truncate until journal majority
- [x] Wait-on: majority note first; fail → no local truncate
- [x] Wait-off: local-first unchanged
- [x] Metrics majority_first success/fail; tests phase148 + phase135/137

### Phase 145 — Rack-aware partition assignment
- [x] Multi-rack diversity placement; env `VOLANT_RACK_AWARE_ASSIGNMENT` (default on)
- [x] Metric `volant_rack_aware_assignment_total`
- [x] Tests `phase145_rack_aware_assignment`

### Phase 144 — Preferred × session thrash suppress
- [x] Suppress preferred when `req_session_id != 0`
- [x] Metric `volant_preferred_replica_session_suppressed_total`

### Phase 143 — Promote claim fence
- [x] `promoted_by` lowest-id claim; MirrorPut converge

### Phase 142 / 141
- [x] Metadata ISR overlay + IsrUpdate; N=2 majority health gauges

---

## P1 / P2 residual

_(none open)_

---

## Next candidates

| Pri | Item | Notes |
|----:|------|-------|
| P3 | Incremental/delta MirrorPut | Phase 146 (in progress merge) |
| P3 | Serve-from-mirror without promote | Phase 147 (in progress merge) |
| Later | Full preferred throttling / TCP probe | Beyond 145 assignment |
| Later | openraft / KRaft; KIP-890; durable streams; multi-lang; chaos; perf |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 148 | **Shipped** — wait mode majority-first, no irreversible truncate on fail |
| Phase 145 | **Shipped** — rack-diverse create assignment MVP |
| P0/P1/P2 | **None open** |
