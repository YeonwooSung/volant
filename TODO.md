# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 138** shipped (best-effort shared fetch session mirror + promote).  
**Last review:** 2026-08-12  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–138 shipped**). Spec: [docs/PHASE138_SPEC.md](./docs/PHASE138_SPEC.md).

---

## Shipped recently

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

### Phase 136 — Non-blocking admin catch-up
- [x] `schedule_catch_up_peer_admin_state` (single-flight + min-interval)
- [x] `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS` (default 500ms)
- [x] Metric `volant_admin_catchup_skipped_total`
- [x] Tests `phase136_admin_catchup_hardening` (4) + phase117 green

### Residual docs (P1)
- [x] N=2 static membership + one down → permanent journal majority fail (ops/features/PHASE130/135)
- [x] Txn coordinator registry wall-clock TTL sharp edge (ops/features/PHASE127/128)

### Phase 135 — DeleteRecords majority wait (optional)
- [x] Wait knob default off; native 15 / Kafka 19 on majority miss

---

## P1 residual

_(none open)_

---

## P2 / later deferred

- [ ] Full openraft / KRaft-style metadata + truncate log
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [x] Shared fetch session store MVP (Phase 138 mirror + promote; best-effort, not Raft)
- [ ] Full preferred-replica selector / rack-aware partition assignment (beyond 133)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [x] Request-level DeleteRecords wait flag on wire (Phase 137 native trailer; Kafka still env-only)
- [ ] Rollback local truncate on majority fail
- [ ] Debounced / incremental mirror put (Phase 138 full snapshot put is chatty)
- [ ] Serve from mirror without promote / dual-master sessions

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 138 | **Shipped** — best-effort peer mirror + promote-on-owner-miss; happy path still 119 forward |
| Phase 137 | **Shipped** — native per-request wait + journal topic prune / anti-resurrection |
| Phase 136 | **Shipped** — admin catch-up no longer stalls HeartbeatBroker |
| N=2 / TTL docs | **Done** |
| P0 code | **Done** |

**Next candidates:** full preferred selector polish (beyond 133), rollback-on-majority-fail (hard), or larger Raft/KIP work.
