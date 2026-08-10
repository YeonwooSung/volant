# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 135** shipped (optional DeleteRecords majority wait) on top of 133–134.  
**Last review:** 2026-08-10  

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–135 shipped**). Spec: [docs/PHASE135_SPEC.md](./docs/PHASE135_SPEC.md).

---

## Shipped recently

### Phase 135 — DeleteRecords majority wait (optional)
- [x] `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` default **off**
- [x] Native fail → `NotEnoughReplicas` (15); Kafka → **19**
- [x] `DeleteRecordsFanoutResult { majority_ok }`; wait metrics
- [x] Tests `phase135_delete_records_majority_wait` (6)
- [x] Living docs

### Phases 132–134
- [x] Journal catch-up hardening; preferred polish; p2p heartbeat mesh

---

## P1 residual

- [ ] N=2 static membership + one peer down → permanent journal majority fail (document)
- [ ] Registry GC wall-clock TTL can drop long-lived txn Init-owner mappings (document)
- [ ] Optional: non-blocking admin catch-up (Phase 117 path still awaits on HeartbeatBroker)

---

## P2 / later deferred

- [ ] Full openraft / KRaft-style metadata + truncate log
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [ ] Shared fetch session store / full preferred selector (beyond 133)
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge
- [ ] Request-level wait flag on wire (Phase 135 is env/broker knob only)
- [ ] Rollback local truncate on majority fail (honest residual: no undo)

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 135 | **Shipped** — opt-in majority wait; default best-effort preserved |
| P0 code | **Done** |

**Next candidates:** docs sharp edges, non-blocking admin catch-up, shared fetch sessions, or larger Raft/KIP work.
