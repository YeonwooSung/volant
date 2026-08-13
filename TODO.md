# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–148** shipped (all prior P2 + P3 MVPs).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–148 shipped**).  
Recent specs: [PHASE148](./docs/PHASE148_SPEC.md) · [PHASE147](./docs/PHASE147_SPEC.md) · [PHASE146](./docs/PHASE146_SPEC.md) · [PHASE145](./docs/PHASE145_SPEC.md).

---

## Shipped recently (P3)

### Phase 148 — Defer local truncate until journal majority
- [x] Wait-on: majority note **before** local truncate; fail → log unchanged
- [x] Wait-off: local-first unchanged
- [x] Tests `phase148_defer_truncate_majority` + phase135/137

### Phase 147 — Serve-from-mirror without promote
- [x] Owner miss + mirror → serve without `promote_from_mirror` (default)
- [x] `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` restores promote path
- [x] Metric `volant_fetch_session_serve_from_mirror_total`
- [x] Tests `phase147_serve_from_mirror`

### Phase 146 — Incremental/delta MirrorPut
- [x] JSON `mode=full|delta` + `remove_topic_keys`; opcode 90 unchanged
- [x] Fan-out prefers delta via `last_mirrored` cache
- [x] Metric `volant_fetch_session_mirror_delta_puts_total`
- [x] Tests `phase146_mirror_put_delta`

### Phase 145 — Rack-aware partition assignment
- [x] Multi-rack diversity on create; env `VOLANT_RACK_AWARE_ASSIGNMENT` (default on)
- [x] Metric `volant_rack_aware_assignment_total`
- [x] Tests `phase145_rack_aware_assignment`

### Phases 141–144 (P2)
- [x] N=2 majority gauges; Metadata ISR; promote claim fence; preferred×session suppress

---

## P1 / P2 / P3 residual

_(none open for the prior P3 list)_

---

## Next candidates

| Pri | Item | Notes |
|----:|------|-------|
| Later | Full preferred throttling / TCP probe | Beyond 145 assignment |
| Later | Dual-epoch mirror SoT / Raft session registry | 147 residual |
| Later | Wait-off truncate rollback / Raft truncate log | 148 residual wait-off path |
| Later | Full openraft / KRaft metadata | Product bet |
| Later | Full KIP-890 / `__transaction_state` | Product bet |
| Later | Durable stream state store | `volant-stream` in-memory |
| Later | Multi-language clients | Ecosystem |
| Later | Long fuzz + chaos-mesh | Phase 112 is smoke only |
| Later | Perf campaign vs aspirational targets | Publish numbers |

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 148 | **Shipped** — wait mode majority-first |
| Phase 147 | **Shipped** — serve mirror without promote (default) |
| Phase 146 | **Shipped** — delta MirrorPut wire |
| Phase 145 | **Shipped** — rack-aware create assignment |
| P0–P3 (listed) | **None open** |

**Default next slice:** product bet (streams / Raft / multi-lang) or hardening (chaos/perf).
