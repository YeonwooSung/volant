# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 136** shipped (non-blocking admin catch-up) + residual docs (N=2 majority, registry TTL).  
**Last review:** 2026-08-10  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–136 shipped**). Spec: [docs/PHASE136_SPEC.md](./docs/PHASE136_SPEC.md).

---

## Shipped recently

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
- [ ] Shared fetch session store / full preferred selector (beyond 133)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [ ] Request-level DeleteRecords wait flag on wire (Phase 135 env-only)
- [ ] Rollback local truncate on majority fail

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 136 | **Shipped** — admin catch-up no longer stalls HeartbeatBroker |
| N=2 / TTL docs | **Done** |
| P0 code | **Done** |

**Next candidates:** shared fetch sessions, request-level majority-wait flag, or larger Raft/KIP work.
