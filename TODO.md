# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 140** shipped (preferred max LEO lag + RC suppress metric).  
**Last review:** 2026-08-12  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–140 shipped**). Specs: [docs/PHASE139_SPEC.md](./docs/PHASE139_SPEC.md), [docs/PHASE140_SPEC.md](./docs/PHASE140_SPEC.md).

---

## Shipped recently

### Phase 140 — Preferred-replica selector depth
- [x] Optional `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (unset = unlimited; skip peers over lag vs leader LEO)
- [x] Metric `volant_preferred_replica_suppressed_total` (READ_COMMITTED suppress when candidate exists)
- [x] Tests `phase140_preferred_selector` (multi-rack, lag, dead, RC suppress)
- [x] Not full Kafka selector/throttling; usable-addr + LEO rank remain Phase 133

### Phase 139 — Session mirror polish (debounce + durable + fence)
- [x] Coalesce dirty mirror ops (one per `session_id`; Delete supersedes Put)
- [x] Debounced Puts: `VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default **50**; Delete immediate
- [x] Optional durable mirrors: `VOLANT_FETCH_SESSION_MIRROR_DURABLE` → `__fetch_session_mirrors/state.json`
- [x] `mirror_gen` fencing on apply/promote
- [x] Metrics: coalesced, stale put rejects, promote supersede, restored
- [x] Tests `phase139` unit + `phase139_session_mirror_polish`
- [x] Not Raft; dual-promote residual; no incremental wire / serve-without-promote

### Phase 138 — Shared fetch session mirror + promote (MVP)
- [x] Native inter-broker opcodes **90–93** (`FetchSessionMirrorPut` / `FetchSessionMirrorDelete`)
- [x] Foreign mirror table + `promote_from_mirror` on owner forward miss
- [x] Dirty-queue best-effort fan-out after primary session mutations
- [x] Happy path still Phase 119 forward while owner alive
- [x] Metrics: mirror puts/deletes applied, promote total, mirrored gauge
- [x] Tests `phase138_shared_fetch_sessions` + `fetch_session` unit (export/install/promote)
- [x] Not Raft; dual-promote race honest; no session_id re-encode

### Phase 137 — DeleteRecords request wait flag + journal GC hygiene
- [x] Native optional trailer `wait_majority: u8` (0=broker default, 1=force wait, 2=force no-wait)
- [x] `Client::delete_records_with_wait_flag` + CLI `--wait-majority` / `--no-wait-majority`
- [x] `apply_cluster_state` prunes removed topics from truncate journal
- [x] Push apply filters to known topics (anti-resurrection)
- [x] Tests `phase137_delete_records_request_wait_flag` + `phase137_journal_topic_gc`
- [x] Kafka path still env/broker knob only (no per-request wire field)

### Residual docs (P1)
- [x] N=2 static membership + one down → permanent journal majority fail (ops/features/PHASE130/135)
- [x] Txn coordinator registry wall-clock TTL sharp edge (ops/features/PHASE127/128)

---

## P1 residual

_(none open)_

---

## P2 / later deferred

- [ ] Full openraft / KRaft-style metadata + truncate log
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [x] Shared fetch session store MVP (Phase 138 mirror + promote; best-effort, not Raft)
- [x] Debounced / durable peer mirrors + `mirror_gen` fence (Phase 139; still best-effort)
- [ ] Full preferred-replica selector / rack-aware partition assignment (beyond 126/133/140 lag+suppress metric)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [x] Request-level DeleteRecords wait flag on wire (Phase 137 native trailer; Kafka still env-only)
- [ ] Rollback local truncate on majority fail
- [ ] Incremental/delta MirrorPut wire (139 coalesces full snapshots only)
- [ ] Serve from mirror without promote / dual-master sessions

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 140 | **Shipped** — optional max LEO lag + RC suppress counter; not full Kafka selector |
| Phase 139 | **Shipped** — coalesce/debounce Puts, optional durable mirrors, `mirror_gen` fence |
| Phase 138 | **Shipped** — best-effort peer mirror + promote-on-owner-miss; happy path still 119 forward |
| Phase 137 | **Shipped** — native per-request wait + journal topic prune / anti-resurrection |
| N=2 / TTL docs | **Done** |
| P0 code | **Done** |

**Next candidates:** serve-from-mirror / Raft session registry residual, rollback-on-majority-fail (hard), full preferred selector (throttling / assignment), or larger Raft/KIP work.
