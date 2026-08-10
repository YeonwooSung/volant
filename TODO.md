# Volant residual TODO (review loop)

**Baseline:** HEAD product = **Phase 132** shipped (journal catch-up hardening) on top of Phase 131 + residual P0 fixes.  
**Last review:** 2026-08-10 (Phase 132 implementation)  
**Source reviews:** `docs/reviews/REVIEW_6708f1fe_phases126-130.md` + Phase 131/132  

**Note:** All open **code P0** residuals closed. Honest residual remains: current-epoch forge under weak auth; push (88) max-merge unfenced by design (ACL/auth is the gate); peers clamp independently; journal max-merge SoT; best-effort fan-out. Residual ITs named `phase132_journal_note_fence` / `phase133_journal_auth` are **post-131 residual fix tests**, not formal product phases 132/133. Formal Phase 132 tests live in `phase132_journal_catchup_hardening`.

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–132 shipped**). Spec: [docs/PHASE132_SPEC.md](./docs/PHASE132_SPEC.md).

---

## Shipped recently (mark done)

### Phase 132 — Truncate journal catch-up hardening
- [x] Non-blocking catch-up (`schedule_catch_up_peer_truncate_journal` from HeartbeatBroker)
- [x] Per-peer single-flight + min-interval (`VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`, default 500ms)
- [x] Metric `volant_journal_catchup_skipped_total`
- [x] Wire IT depth: push (88), majority note, schedule path — `phase132_journal_catchup_hardening`
- [x] Living docs 0–132

### Phase 126–130 fix pass + hardening (`64d8413`…`82dcd88`)
- [x] Reconcile freeze, ACL gate 86/88, fan-out outbox safety, always full push, registry/journal persist locks, preferred v15+, journal caps, timeouts

### Phase 131 — journal rejoin catch-up (`3e1d7ed`)
- [x] `HeartbeatBroker` trailer `applied_journal_generation`
- [x] Lag-driven `TruncateJournalPush` catch-up + metrics + ITs + living docs 0–131

### Residual P0 fixes (post-131)
- [x] Journal note ingress fence (`phase132_journal_note_fence`)
- [x] Auth/ACL journal ITs (`phase133_journal_auth`)
- [x] Preferred + READ_COMMITTED suppress
- [x] Fan-out at achieved low watermark

---

## P0 residual (correctness / security — closed)

- [x] Journal note fence — honest residual: current-epoch forge under weak auth; push 88 unfenced by design  
- [x] Preferred + isolation  
- [x] Fan-out achieved-low  

---

## P1 residual (after Phase 132)

- [ ] Preferred selector MVP polish: lowest-id tiebreak; no endpoint usability check → candidate **Phase 133**
- [ ] N=2 static membership + one peer down → permanent journal majority fail (document)
- [ ] Registry GC wall-clock TTL can drop long-lived txn Init-owner mappings (document)

---

## P2 / later deferred (not near-term P0)

- [ ] Full openraft / KRaft-style metadata + truncate log (journal remains max-merge SoT)
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [ ] Shared fetch session store / full preferred-replica selector
- [ ] Peer-to-peer heartbeat mesh — candidate after 132
- [ ] Sync client wait on DeleteRecords majority — candidate after 132
- [ ] Multi-language clients
- [ ] Full chaos-mesh suites / long fuzz campaigns (corpus smoke CI = Phase 112 MVP only)
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge

---

## Review notes (this fire)

| Area | Verdict |
|------|---------|
| Phase 132 | **Shipped** — non-blocking schedule, single-flight, min-interval, skipped metric, hardening ITs |
| Phase 131 | Heartbeat trailer + lag push; catch-up no longer stalls membership |
| P0 code | **Done** |
| Residual IT naming | `phase132_journal_note_fence` / `phase133_journal_auth` ≠ formal product phases |

**Stop condition:** Phase 132 exit criteria met. Next formal candidate: preferred selector polish (Phase 133) or cluster mesh / majority wait (P2).
