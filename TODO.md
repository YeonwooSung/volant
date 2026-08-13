# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 0–147** shipped (serve-from-mirror without promote).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–147 shipped**).  
Recent specs: [PHASE147](./docs/PHASE147_SPEC.md) · [PHASE144](./docs/PHASE144_SPEC.md) · [PHASE143](./docs/PHASE143_SPEC.md) · [PHASE142](./docs/PHASE142_SPEC.md).

---

## Shipped recently

### Phase 147 — Serve-from-mirror without promote (MVP)
- [x] `begin_incremental_from_any` / `has_servable_session` / `mirror_session_clone`
- [x] Mirror-only mutations in-place (no `queue_mirror_put` / no primary insert)
- [x] Owner miss default: serve mirror; metric `volant_fetch_session_serve_from_mirror_total`
- [x] Knobs: `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE` (default on),
  `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS` (default off)
- [x] Tests `phase147_serve_from_mirror`; phase138/139 adjusted; phase143 green
- [x] Honest dual-epoch residual documented

### Phase 144 — Preferred × session thrash suppress
- [x] Suppress PreferredReadReplica when `req_session_id != 0` (non-FINAL epoch)
- [x] Metric `volant_preferred_replica_session_suppressed_total`
- [x] Full fetch (`session_id == 0`) still prefers; RC path unchanged (Phase 140)
- [x] Tests `phase144_preferred_session_suppress` + phase126/133/140 green

### Phase 143 — Fetch session promote claim fence (lowest-id)
- [x] `promoted_by` on session + MirrorPut JSON (default `0` for old mirrors)
- [x] `session_claim_wins` after `mirror_gen`/epoch/activity; lowest non-zero claim
- [x] `promote_from_mirror` stamps claim; dual-promote converges via MirrorPut
- [x] Metric `volant_fetch_session_promote_claim_reject_total`
- [x] Tests `phase143_promote_claim_fence` + phase138/139 green

### Phase 142 — Metadata ISR freshness (leader ≠ controller)
- [x] Leader Metadata overlays local ISR / epoch / HWM
- [x] Inter-broker `IsrUpdate` opcodes **94/95**; controller fence + gen bump
- [x] Best-effort non-controller leader report + local gen align
- [x] Tests `phase142_metadata_isr`

### Phase 141 — N=2 majority ops tooling
- [x] Gauges: `volant_cluster_configured_brokers` / `_live_brokers` / `_majority_quorum` / `_majority_impossible`
- [x] Broker helpers: `configured_broker_count` / `live_broker_count` / `majority_quorum_size` / `majority_impossible`
- [x] Tests `phase141_n2_majority_ops`
- [x] Docs: ops sharp edge + metrics scrape list; majority algorithm **unchanged**

### Phase 140 — Preferred-replica selector depth
- [x] Optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (unset = unlimited)
- [x] Metric `volant_preferred_replica_suppressed_total` (READ_COMMITTED when candidate exists)
- [x] Tests `phase140_preferred_selector`
- [x] Usable-addr + LEO rank remain Phase 133 (not re-shipped)

### Phase 139 — Session mirror polish
- [x] Coalesce dirty ops (one per `session_id`; Delete supersedes Put)
- [x] Debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50); Delete immediate
- [x] Optional durable mirrors (`VOLANT_FETCH_SESSION_MIRROR_DURABLE` → `__fetch_session_mirrors`)
- [x] `mirror_gen` fencing on apply/promote
- [x] Tests `phase139_*` + `phase139_session_mirror_polish`

### Phase 138 — Shared fetch session mirror + promote (MVP)
- [x] Opcodes **90–93**; promote on owner miss; happy path still Phase 119 forward

### Phase 137 — DeleteRecords wait trailer + journal topic GC
- [x] Native `wait_majority` trailer; assignment prune; push anti-resurrection

### Earlier residuals closed
- [x] N=2 majority sharp-edge docs; txn coordinator registry TTL docs
- [x] Non-blocking admin catch-up (Phase 136)

---

## P1 residual

_(none open)_

---

## Next candidates (suggested order)

| Pri | Item | Notes |
|----:|------|-------|
| P3 | Full preferred selector / throttling / rack-aware assignment | Beyond 126/133/140/144 |
| P3 | Incremental/delta MirrorPut wire | 139 coalesces full snapshots only |
| P3 | Dual-epoch converge / Raft session SoT | 147 serves mirrors without single SoT |
| P3 | Rollback / defer local truncate until majority | Hard; segment delete is irreversible today |
| Later | Heterogeneous per-broker BROKER overrides | Controller-only homogeneous push today |
| Later | True multi-master ACL merge | Or lock “controller SoT forever” |
| Later | Full openraft / KRaft metadata + truncate log | Replaces static ISR + journal majority MVP |
| Later | Full KIP-890 / 939 / `__transaction_state` | Replaces 2PC MVP + soft markers |
| Later | Durable stream state store | `volant-stream` still in-memory |
| Later | Multi-language clients | Ecosystem |
| Later | Long fuzz + chaos-mesh | Phase 112 is corpus smoke only |
| Later | Perf campaign vs aspirational targets | Publish numbers; group-commit decision |

---

## P2 / later deferred (checklist)

- [ ] Full openraft / KRaft-style metadata + truncate log
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [x] Shared fetch session store MVP (Phase 138)
- [x] Debounced / durable peer mirrors + `mirror_gen` fence (Phase 139)
- [x] Preferred lag ceiling + RC suppress metric (Phase 140)
- [x] N=2 majority ops tooling / health gauges (Phase 141)
- [x] Metadata ISR overlay + leader→controller IsrUpdate (Phase 142)
- [x] Promote claim fence lowest-id (Phase 143)
- [x] Preferred × session thrash light suppress (Phase 144)
- [ ] Full preferred-replica selector / rack-aware partition assignment (beyond 140/144)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [x] Request-level DeleteRecords wait flag (Phase 137; Kafka still env-only)
- [ ] Rollback local truncate on majority fail
- [ ] Incremental/delta MirrorPut wire
- [ ] Serve from mirror without promote / dual-master sessions

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 144 | **Shipped** — preferred suppress on established fetch session; session suppress metric |
| Phase 143 | **Shipped** — `promoted_by` lowest-id claim fence; best-effort MirrorPut; not Raft |
| Phase 142 | **Shipped** — leader Metadata overlay + IsrUpdate 94/95; best-effort report |
| Phase 141 | **Shipped** — majority health gauges; majority algo unchanged |
| Phase 140 | **Shipped** — lag knob + RC suppress; not full Kafka selector |
| Phase 139 | **Shipped** — coalesce/debounce, optional durable, `mirror_gen` fence |
| Phase 138 | **Shipped** — best-effort mirror + promote-on-miss |
| Phase 137 | **Shipped** — native wait trailer + journal topic GC |
| P0 / P1 code | **None open** |
| P2 residual | **None open** |

**Default next slice:** pick a P3 product residual (incremental MirrorPut, serve-from-mirror, full preferred selector) or a larger product bet (streams durable state / Raft / multi-lang). Larger Raft/KIP only with a clear product goal.
