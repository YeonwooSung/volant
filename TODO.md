# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 141** shipped (N=2 majority ops health gauges).  
**Last review:** 2026-08-13  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–141 shipped**).  
Recent specs: [PHASE141](./docs/PHASE141_SPEC.md) · [PHASE140](./docs/PHASE140_SPEC.md) · [PHASE139](./docs/PHASE139_SPEC.md) · [PHASE138](./docs/PHASE138_SPEC.md).

---

## Shipped recently

### Phase 141 — N=2 majority ops tooling
- [x] Gauges: `volant_cluster_configured_brokers` / `_live_brokers` / `_majority_quorum` / `_majority_impossible`
- [x] Broker helpers: `configured_broker_count` / `live_broker_count` / `majority_quorum_size` / `majority_impossible`
- [x] Tests `phase141_n2_majority_ops`
- [x] Docs: ops sharp edge + metrics scrape list; majority algorithm **unchanged**
- [ ] Metadata ISR lag (separate residual — not in this phase)

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
| P2 | **Metadata ISR lag** when leader ≠ controller | Clients can see stale ISR |
| P2 | **Promote claim fence** (lowest-id / seq) | Dual-promote of *identical* snapshots still possible after 139 |
| P2 | **Preferred × session thrash** doc or light suppress | Client may session-on-leader then follow PreferredReadReplica → forward |
| P3 | Full preferred selector / throttling / rack-aware assignment | Beyond 126/133/140 |
| P3 | Incremental/delta MirrorPut wire | 139 coalesces full snapshots only |
| P3 | Serve-from-mirror without promote | Dual-epoch design required |
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
- [ ] Full preferred-replica selector / rack-aware partition assignment (beyond 140)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [x] Request-level DeleteRecords wait flag (Phase 137; Kafka still env-only)
- [ ] Rollback local truncate on majority fail
- [ ] Incremental/delta MirrorPut wire
- [ ] Serve from mirror without promote / dual-master sessions
- [x] N=2 majority ops tooling / health gauges (Phase 141)
- [ ] Metadata ISR lag when leader ≠ controller

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 141 | **Shipped** — majority health gauges; majority algo unchanged; Metadata ISR lag still open |
| Phase 140 | **Shipped** — lag knob + RC suppress; not full Kafka selector |
| Phase 139 | **Shipped** — coalesce/debounce, optional durable, `mirror_gen` fence |
| Phase 138 | **Shipped** — best-effort mirror + promote-on-miss |
| Phase 137 | **Shipped** — native wait trailer + journal topic GC |
| P0 / P1 code | **None open** |

**Default next slice:** Metadata ISR lag (stale ISR when leader ≠ controller) *or* promote claim fence if dual-promote in the wild is painful. Larger Raft/KIP only with a clear product goal.
