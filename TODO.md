# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phases 133–134** shipped (preferred polish + p2p heartbeat mesh) on top of Phase 132.  
**Last review:** 2026-08-10 (A+B parallel ship)  

**Note:** Open code P0 closed. Honest residual: current-epoch forge under weak auth; push 88 unfenced by design; best-effort DeleteRecords fan-out; preferred still no load/throttling / no READ_COMMITTED on followers. Residual IT names `phase132_journal_note_fence` / `phase133_journal_auth` ≠ formal product phases.

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–134 shipped**). Specs: [PHASE133](./docs/PHASE133_SPEC.md), [PHASE134](./docs/PHASE134_SPEC.md).

---

## Shipped recently (mark done)

### Phase 134 — Peer-to-peer heartbeat mesh
- [x] `heartbeat_mesh` to all configured peers (period session/3)
- [x] Alive-set / ClusterState only when peer is controller
- [x] Non-controller journal catch-up via mesh IT (`phase134_heartbeat_mesh`)
- [x] Living docs

### Phase 133 — Preferred selector polish
- [x] Usable endpoint gate + highest-LEO then lowest-id ranking
- [x] Tests `phase133_preferred_selector` (+ phase126 green)
- [x] Living docs

### Phase 132 — Journal catch-up hardening
- [x] Non-blocking schedule, single-flight, min-interval, skipped metric, ITs

### Residual P0 (post-131)
- [x] Journal note fence / auth ITs / preferred+isolation / fan-out achieved-low

---

## P1 residual

- [ ] N=2 static membership + one peer down → permanent journal majority fail (document)
- [ ] Registry GC wall-clock TTL can drop long-lived txn Init-owner mappings (document)
- [ ] Optional: non-blocking admin catch-up (same pattern as Phase 132 journal schedule)

---

## P2 / later deferred

- [ ] Full openraft / KRaft-style metadata + truncate log
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [ ] Shared fetch session store / full preferred selector (beyond 133 polish)
- [ ] Sync client wait on DeleteRecords majority
- [ ] Multi-language clients
- [ ] Full chaos-mesh / long fuzz campaigns
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge

---

## Review notes

| Area | Verdict |
|------|---------|
| Phase 133 | **Shipped** — LEO-desc + usable-addr preferred ranking |
| Phase 134 | **Shipped** — mesh HB; alive-set only vs controller |
| P0 code | **Done** |

**Stop condition:** A+B complete. Next candidates: docs-only sharp edges, DeleteRecords majority wait, or larger Raft/KIP work.
