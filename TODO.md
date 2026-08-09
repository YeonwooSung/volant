# Volant residual TODO (review loop)

**Baseline:** `main` @ `3e1d7ed` (Phase 131) + local residual fixes (journal note fence + preferred isolation) + Auth/ACL journal ITs  
**Last review:** 2026-08-09 (journal fence residual + preferred×isolation)  
**Source reviews:** `docs/reviews/REVIEW_6708f1fe_phases126-130.md` + Phase 131 delta  

**Note:** Journal note fence closed for existence/stale/negative epoch; fanout never stamps `-1`. Preferred redirect suppressed under READ_COMMITTED. Auth/ACL 86/88 locked by `phase133`. Honest residual: current-epoch forge under weak auth; push 88 unfenced by design. **Remaining open P0:** fan-out achieved-low.

Living roadmap: [ROADMAP.md](./ROADMAP.md) (Phases **0–131 shipped**). This file tracks **residual correctness / security / quality** work and long-horizon deferred items so review loops stay honest.

---

## Shipped recently (mark done)

### Phase 126–130 fix pass + hardening (`64d8413`…`82dcd88`)
- [x] **#1** Reconcile freeze: `last_reconcile` only advances when local `log_start >= target`
- [x] **#3** ACL gate TruncateJournalNote/Push: inter-broker auth principal **or** Cluster Alter
- [x] **#4–#6** Pre-enqueue DeleteRecords peers before JoinSet; budget/JoinError safe outbox
- [x] **#7** Always full-snapshot journal push to live peers (no selective note-acker skip)
- [x] **#8** Txn coordinator registry `persist_lock` + unique tmp
- [x] **#9** Journal `apply_push` generation mono (`fetch_max`-style)
- [x] **#10** Fetch v15+ ReplicaState parse for preferred follower gate
- [x] **#11–#13** Journal prune on `delete_topic` + 100k/4MiB caps; env timeout clamp [1ms, 10min]
- [x] **#14–#15** phase126 preferred tests de-vacuous; phase130 `consensus_fail`; phase99 key
- [x] Inter-broker RPC default **5s**; DeleteRecords fan-out budget formula (~**20s** default)
- [x] Registry age-0 dual-lock; preferred `replica_id < 0` gate through Fetch ≤14
- [x] Journal `persist_lock` + unique tmp

### Phase 131 — journal rejoin catch-up (`3e1d7ed`)
- [x] `HeartbeatBroker` trailer `applied_journal_generation` (legacy/Phase117 decode defaults)
- [x] Lag-driven `TruncateJournalPush` catch-up (any node with newer journal; not controller-gated)
- [x] Metrics `volant_journal_catchup_success_total` / `_errors_total`
- [x] Tests: `phase131_journal_catchup` (direct + offline rejoin) + protocol round-trips
- [x] Living docs 0–131 (`README`, `ROADMAP`, `PHASE131_SPEC`, features/ops/history)

### Journal note ingress fence (P0 residual fix, post-131)
- [x] Empty topic → InvalidArg; `before_offset == 0` → no-op success
- [x] Unknown topic/partition → NotFound (no orphan journal keys)
- [x] Stale epoch: local > req → InvalidProducerEpoch (19); no max-merge, gen unchanged
- [x] Negative epoch (`leader_epoch < 0`) for non-zero offset → InvalidArg (3); fanout never stamps `-1` (skip note if not leading)
- [x] Future epochs (`req >= local`) still accepted (multi-controller lag); exact-match rejected as unsafe
- [x] Does **not** require receiver is leader (Phase 130 multi-controller preserved)
- [x] Proposer still uses `local_note_truncate_journal` directly; ACL/auth for 86/88 unchanged
- [x] ITs: `phase132_journal_note_fence` (incl. `-1` reject, future accept, TCP)

### Auth / ACL journal tests (P0 residual, post-131)
- [x] `phase133_journal_auth`: unauth → AuthenticationRequired (18), watermark not raised
- [x] Wrong token → AuthenticationFailed (17)
- [x] Wrong principal → AuthorizationFailed (23); watermark/gen unchanged
- [x] Inter-broker principal allow without Cluster Alter (ACLs on, empty rules)
- [x] Cluster Alter allow for non-ib principal
- [x] Optional push (88) deny/allow

---

## P0 residual (correctness / security — still open)

These are the known residual bands after the 126–130 fix pass + journal note ingress fence + auth journal ITs + preferred isolation suppress. None block the Phase 131 MVP claim, but they remain **honest open P0-class** items.

- [x] **Journal note fence (epoch/existence + negative-epoch closed)**  
  Ingress: empty topic; zero offset no-op; unknown TP → NotFound; `leader_epoch < 0` → InvalidArg; stale local > req → InvalidProducerEpoch; future epochs accepted. Fanout stamps only while leading (never `-1`). ITs: `phase132` + `phase133`.  
  **Still open (honest residual):** **current-epoch** forge + huge `before_offset` under **auth/ACL off** (or any Cluster Alter / ib principal) still max-merges; **TruncateJournalPush (88)** max-merge intentionally unfenced for rejoin catch-up — ACL/auth is the gate on 88. Keep cluster auth on for 86/88 in production.

- [x] **Preferred + isolation**  
  Fetch preferred redirect gated by `!read_committed` (`version >= 11 && replica_id < 0 && !read_committed`). READ_COMMITTED stays on the leader; READ_UNCOMMITTED still redirects. IT: `phase126_preferred_replica::read_committed_suppresses_preferred_redirect`. Full marker/LSO parity on preferred candidates deferred.

- [ ] **Fan-out achieved-low**  
  Client DeleteRecords success path fans out journal note + ReplicaDeleteRecords at **requested** `before_offset`, not achieved `low_watermark` after whole-segment clamp (`net.rs` DeleteRecords handler). Peers/journal can be told a watermark the leader never reached until later segment drops.  
  **Direction:** fan-out / note at `low_watermark` (achieved); keep client response honest.

---

## P1 residual (quality / edge / ops)

- [ ] Catch-up await inside `HeartbeatBroker` handler can stall membership under large snapshot / slow peer (Phase 131); consider spawn + timeout or rate-limit per peer.
- [ ] No journal catch-up throttle (every lagging heartbeat re-pushes full snapshot; 4 MiB cap bounds worst case).
- [ ] Journal note TCP wire IT for opcode **86** fence landed (`phase132`); still thin coverage for 87–89 / push path.
- [ ] Preferred selector MVP: process-local ISR + LEO only; lowest-id tiebreak; no endpoint usability check (documented).
- [ ] N=2 static membership + one peer down → permanent journal majority fail (documented sharp edge).
- [ ] Registry GC wall-clock TTL can drop long-lived txn Init-owner mappings (documented).
- [ ] Docs: keep `PHASE130_SPEC` / review notes aligned with “always full push” + Phase 131 catch-up (mostly updated; re-check on next edit).

---

## P2 / later deferred (not near-term P0)

- [ ] Full openraft / KRaft-style metadata + truncate log (journal remains max-merge SoT)
- [ ] Full KIP-890 / 939 / `__transaction_state`
- [ ] Shared fetch session store / full preferred-replica selector
- [ ] Peer-to-peer heartbeat mesh (heartbeats remain controller-centric)
- [ ] Sync client wait on DeleteRecords majority
- [ ] Multi-language clients
- [ ] Full chaos-mesh suites / long fuzz campaigns (corpus smoke CI = Phase 112 MVP only)
- [ ] Heterogeneous per-broker BROKER overrides without controller
- [ ] True multi-master ACL merge

---

## Review notes (this fire)

| Area | Verdict |
|------|---------|
| Journal note ingress fence | **Partial done** — stale epoch / unknown TP / empty topic / zero offset; residual current-epoch forge + `leader_epoch < 0` under weak auth |
| Auth / ACL journal tests | **Done** — `phase133_journal_auth` locks 86/88 unauth/wrong-token/wrong-principal deny + ib principal / Cluster Alter allow; watermark/gen unchanged on deny |
| Phase 131 correctness | Heartbeat trailer + lag push + metrics + ITs look sound; limitations documented |
| Prior P0 #1/#3–#9 | Landed in code |
| Prior P0 #2 journal fence | **Partial** (epoch/existence landed; equal-epoch / `-1` still open — see P0 list) |
| Other P0 | Preferred+isolation **done** (suppress when READ_COMMITTED); fan-out achieved-low still open |

**Stop condition:** **not met** — open P0 residual remains (fan-out achieved-low; journal current-epoch forge under weak auth is honest residual, not a code hole in epoch math).
