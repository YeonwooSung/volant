# Full issue review — Phases 126–130 (+ timeouts)

**HEAD:** `fcff341` (`ba6a763..HEAD`)  
**Review ID:** `6708f1fe`  
**Date:** 2026-08-03  
**Mode:** Read-only multi-agent review (correctness, concurrency, protocol/tests/docs, security/resources)  
**Status:** P0/P1 fix pass applied on main (uncommitted as of post-fix review); residual P2/P3 remain

---

## Executive summary

Five named post-review fixes largely landed (registry age-0 note lock, preferred `replica_id < 0` gate through Fetch ≤14, reconcile local truncate hook, journal `persist_lock`, formalized 5s/15s timeouts). Residual work clusters into:

| Priority | Theme |
|----------|--------|
| P0 | Reconcile progress freeze; journal note unfenced → data deletion; ACL-bypass journal RPCs |
| P1 | Fan-out budget abort skips outbox; selective push hole; registry persist race; gen regression |
| P2 | Preferred v15+; false-green tests; docs/code “always push”; unbounded journal |
| P3 | Ops knobs, isolation gaps, nits |

---

## P0 — Bugs that lose correctness or enable destructive abuse

### 1. Reconcile marks progress even when local journal apply fails or is incomplete
**Severity:** bug  
**Where:** `broker.rs:1117–1147`

On `err != 0`, `Err(...)`, or `low < target` (whole-segment deletes), code still:
- enqueues peers at full journal `target` (comment claims use `low`)
- writes `last_reconcile[(topic,partition)] = (epoch, target)`

Next ticks hit early `continue` and **never retry** local truncate for the same `(epoch, target)`. Leader can remain permanently below the journal watermark while believing reconcile is done.

**Fix direction:** Only advance `last_reconcile` when `log_start >= target` (or store achieved low and re-run while lagging); use achieved low for peer enqueue when partial.

---

### 2. Truncate journal note is unfenced → durable watermark → real truncate
**Severity:** bug (security / integrity)  
**Where:** `broker.rs:1030–1045`, `1060–1136`; `net.rs` handlers for opcodes 86/88

`handle_truncate_journal_note` always durable-merges any `(topic, partition, before_offset)` — no leader check, no epoch fence. Background reconcile then calls local `delete_records` when `target > log_start`.

`ReplicaDeleteRecords` has epoch fencing; journal→reconcile **bypasses** that path.

**Attack (auth off or shared token known):** high `before_offset` notes → within ~500ms leaders truncate real logs and drive peers.

---

### 3. TruncateJournalNote/Push are ACL-bypassed
**Severity:** bug (security surface)  
**Where:** `net.rs:2771–2789`, `2860–2875`

Same class as other inter-broker admin RPCs. With **auth off**: any client on the volant port can raise watermarks. With **auth on**: any principal that can Auth still bypasses topic Delete ACLs. Journal survives leadership and drives new leaders — expanding log-start commitment beyond ReplicaDeleteRecords.

**Fix direction:** Require inter-broker auth or ClusterAction ACL for 86/88; document as cluster-root surface.

---

## P1 — Bugs that break fan-out / durability under concurrency

### 4. Overall fan-out budget abort does not enqueue remaining peers
**Severity:** bug  
**Where:** `net.rs:1426–1449`, `1452–1569`

On budget expiry the inner future is dropped → in-flight JoinSets aborted → **no** outbox enqueue for unfinished peers. Warn text claims “left to outbox/reconcile”; outbox half is often false.

Mitigation: 500ms reconcile (if leadership still held and issue #1 doesn’t freeze it). Window of up to ~500ms (or forever if leadership lost) with no durable peer-retry.

---

### 5. Default budget = worst-case sum of three sequential RPC phases
**Severity:** bug  
**Where:** `net.rs:2218–2255`, fanout structure

Note (≤5s) + selective push (≤5s) + ReplicaDeleteRecords (≤5s) = default **15s** budget. Any slow phase races/exceeds outer budget → issue #4 on later phases. Auth RTTs make it tighter.

**Fix direction:** Budget ≥ f(phases × RPC timeout), or remaining-time per phase; pre-enqueue peers.

---

### 6. JoinSet JoinError path loses peer identity → no outbox
**Severity:** bug  
**Where:** `net.rs:1564–1567` (and journal note/push join errors)

RPC Err / non-zero correctly enqueue. Task panic/cancel only increments a metric. Peer may have applied, partially applied, or never been contacted.

---

### 7. Selective push skips note-ackers → incomplete journal snapshot catch-up
**Severity:** bug (correctness + docs/code mismatch)  
**Where:** `net.rs:1238–1239`, `1349–1355` vs `docs/PHASE130_SPEC.md` goal 3

Note max-merges **one** TP. Full journal only on **push**. Skipping push for note-ackers means peers missing **other** watermarks never get them. No heartbeat journal re-push.

Concrete: journal has T1+T2; DeleteRecords on T1; all ack note; push set empty; peers never learn T2; leadership moves → reconcile cannot rebuild from journal.

Spec claims “always snapshot-push to live peers.” Code does not.

---

### 8. Txn coordinator registry concurrent persist race
**Severity:** bug  
**Where:** `txn_coordinator_registry.rs:172–196`, `223–267`, `324–342`, `408–424`

Age-0 map/timestamp race fixed, but:
- No `persist_lock`
- Fixed tmp `state.json.tmp`
- Concurrent note + expire_stale can resurrect GC’d keys or lose notes (last rename wins with stale full snapshot)
- `persist()` re-reads four maps under independent read locks (torn snapshot)

Journal already has unique-tmp + persist serialization; registry does not.

---

### 9. `apply_push` generation can regress under concurrent `note`
**Severity:** bug  
**Where:** `truncate_journal.rs:237–241`, `287–295`

`load` then `if generation > cur { store }` races with concurrent `fetch_add` from local note → generation goes backward. Entries still max-merge (good); gen used for push headers / metrics / freshness.

**Fix direction:** `fetch_max`-style CAS or update gen under same lock as entries.

---

## P2 — Protocol, preferred replica, tests, resources

### 10. Preferred redirect gating incomplete for Kafka Fetch v15+
**Severity:** bug (Kafka follower / ReplicaState clients)  
**Where:** `produce_fetch.rs:809–823`, `1014–1017`, `1221–1223`

v15+ drops top-level ReplicaId; code defaults `replica_id = -1` and ignores ReplicaState. Followers using Fetch v15+ with rack can get PreferredReadReplica redirects. Volant’s own ReplicaFetch path is fine; wire hole for Kafka-compatible followers.

---

### 11. Unbounded truncate journal growth
**Severity:** bug (resource / ops)  
**Where:** `truncate_journal.rs:201–244`, `303–312`

No prune, tombstone, or delete-on-DeleteTopic. Every TP ever noted remains forever in memory + `state.json`. Couples with snapshot push size.

---

### 12. Snapshot push size DoS (full JSON every note miss)
**Severity:** bug (resource / DoS)  
**Where:** `truncate_journal.rs:246–251`, `258–300`; `net.rs:1373–1415`

`snapshot_bytes()` full journal; clone per peer; apply merges all entries (capped only by 16 MiB MAX_PAYLOAD). Memory O(peers × snapshot); authenticated (or unauth) peer can push large snapshot.

---

### 13. Env timeout extremes (`0 → 1ms`, no upper bound)
**Severity:** bug (ops footgun)  
**Where:** `net.rs:2218–2255`

`0` becomes 1ms (easy “disable” mis-set). No max → multi-hour values undo formalization. Re-read every call; mid-flight env change can skew deadlines. Tests in `phase_rpc_timeouts` set env without process mutex/Drop → parallel test pollution.

---

### 14. False-green / vacuous preferred-replica tests
**Severity:** bug (test honesty)  
**Where:** `phase126_preferred_replica.rs`

- `follower_fetch_no_preferred_redirect`: single-node → preferred always None; does **not** prove `replica_id < 0` gate
- `follower_serves_fetch_after_redirect`: body under `if let Some(pref)`; with racks can pass without any Fetch assert
- Soft coverage when leader alone in rack (`expected_pref = None` allowed)

---

### 15. phase129 “fanout” test overclaims; phase130 missing fail/selective-push cases
**Severity:** suggestion / test gap  
**Where:** `phase129_truncate_journal.rs:178–201`, `phase130_truncate_journal_consensus.rs`

phase129 has no TCP wire for 86–89 (in-process only). phase130 missing: `consensus_fail`, selective-push multi-key hole, concurrent multi-proposer, gen mono under race, N=2 quorum impossibility, budget-abort→outbox.

---

### 16. Docs/code drift: “always push” / PHASE_HISTORY / API names
**Severity:** bug (docs) / suggestion  
**Where:** `PHASE130_SPEC.md`, ROADMAP, `PHASE_HISTORY.md:211`, protocol request comments, `controller_note_*` names

Spec “always snapshot-push”; code selective. History still “closed by 129 (not Raft)” without 130 majority nuance. Protocol comments still “Leader → controller.”

---

### 17. Fan-out / journal note the **requested** `before_offset`, not achieved low
**Severity:** suggestion (edge correctness)  
**Where:** `acl_api.rs:203–209`, `net.rs:3850–3857`

On local success, fanout uses client offset even when storage only advanced to lower low watermark. Couples with #1.

---

### 18. Preferred redirect ignores isolation_level
**Severity:** suggestion  
**Where:** `produce_fetch.rs:1221–1252`, `broker.rs:1237–1280`

Eligibility: rack + live + ISR + LEO≥HWM only. For READ_COMMITTED, followers may lack full aborted-marker view → different filter results after redirect.

---

### 19. Preferred selector: process-local ISR + LEO only
**Severity:** nit / residual MVP  
**Where:** `broker.rs:1251–1274`

Stale live membership can redirect to just-dead broker; no endpoint usability check; lowest-id tiebreak only.

---

## P3 — Suggestions, nits, ops

### 20. Async + parking_lot + sync fsync on request paths
**Severity:** suggestion  
**Where:** truncate journal persist, outbox, reconcile

`parking_lot` does not yield to Tokio; holding across `sync_all` can stall worker threads under multi-controller concurrent notes.

---

### 21. Majority of configured N vs live membership
**Severity:** suggestion (documented, sharp edges)  
**Where:** `net.rs:1252–1267`, `broker.rs:985–990`

N=2 one peer down → permanent consensus_fail. Offline voters never note/push until later paths. Optimistic live → full timeout tax on dead peers.

---

### 22. Partial peer journal under consensus_fail (by design)
**Severity:** suggestion  
Peers durable-note before proposer reaches majority. Max-merge never shrinks; journals can diverge until later success. Combined with #4/#7, long-lived watermark splits possible.

---

### 23. Registry GC is wall-clock; long txn + short TTL
**Severity:** nit (documented)  
Long-lived txns that don’t re-note within TTL lose sticky Init-owner mappings.

---

### 24. Client-visible internal leaks (native protocol)
**Severity:** suggestion  
`map_error` + timeout strings include peer `addr` / Io paths for native clients. Kafka paths use codes.

---

### 25. JoinSet spawn storms; DeleteRecords awaits full fan-out
**Severity:** suggestion  
No global concurrency cap. Client DeleteRecords holds up to budget (15s) even though “never fails client.” Attackers with Delete ACL can pin tasks.

---

### 26. Temp file cleanup on failed rename
**Severity:** suggestion (truncate unique tmp leak); bug for txn fixed tmp (covered in #8)

---

### 27. phase99 seventh key half-integrated
**Severity:** suggestion  
`map.len() == 7` without asserting `KEY_TXN_COORDINATOR_TTL_MS` name/value; product-default env gate omits `VOLANT_TXN_COORDINATOR_TTL_MS`.

---

### 28. Coverage gaps (protocol / rack)
**Severity:** suggestion  
No flexible Fetch rack IT; no DescribeCluster/NodeEndpoints rack IT; no dedicated auth-on journal RPC test; phase129 no TCP for 86–89.

---

### 29. Small nits
- Protocol comments Leader→controller; trailing garbage decode (pre-existing)
- `env_duration_ms` 0→1ms silent
- `cluster_member_count` empty → N=1
- note always inserts pid 0 into by_pid
- Pretty JSON inflates journal size
- Non-constant-time auth token compare
- Reconcile re-enqueues successful peers (at-least-once OK)

---

## What looks solid

- Preferred redirect: empty records, `omit: false`, metric; leader-only; self excluded; LEO≥HWM + ISR + rack
- Follower gate through Fetch ≤14; rack on Metadata / DescribeCluster / NodeEndpoints
- Journal max-merge never shrinks; `persist_lock` + unique tmp for journal
- Registry dual-lock order for note/expire (age-0 fixed)
- Opcodes 86–89 encode/decode consistent; unit round-trip
- 5s RPC / 15s budget formalized with env overrides + timeout tests
- DeleteRecords client ACL-gated; journal consensus never fails client (by design)
- Phase130 majority math, 3/3 success, 1-down, max-merge unit tests

---

## Suggested fix order

1. **#1** Reconcile `last_reconcile` / partial-low retry  
2. **#2 + #3** Fence journal note + ACL/auth gate (security)  
3. **#4 + #5 + #6** Budget abort / JoinError → always enqueue targeted peers; budget sizing  
4. **#7** Always full snapshot push (or push when local journal has other keys) + fix docs  
5. **#8** Registry `persist_lock` + unique tmp  
6. **#9** Atomic gen advance in `apply_push`  
7. **#10** Parse ReplicaState / don’t default-redirect followers on Fetch v15+  
8. **#11–#13** Journal GC, push size caps, env timeout bounds + test isolation  
9. **#14–#16** False-green tests + docs honesty  
10. Remaining suggestions as capacity allows

---

*Generated from four parallel read-only reviewers; verified highest-severity sites against HEAD `fcff341`.*

---

## Fix pass (6 subagents)

| # | Status | Notes |
|---|--------|--------|
| #1 reconcile freeze | **Fixed** | `last_reconcile` only when local ≥ target |
| #2 journal fence | **Partial** | empty topic + reconcile epoch fence; note merge still open when ACL off |
| #3 ACL TruncateJournal | **Fixed** | inter-broker principal OR Cluster Alter |
| #4–#6 outbox/budget/JoinError | **Fixed** | pre-enqueue + 20s budget formula |
| #7 always full push | **Fixed** | push all live peers |
| #8 registry persist | **Fixed** | `persist_lock` + unique tmp |
| #9 apply_push gen | **Fixed** | `atomic_fetch_max` |
| #14–#15 tests | **Fixed** | phase126 rewrite; phase130 `consensus_fail`; phase99 key |
| #10–#13 | **Open** | v15+ ReplicaState, journal GC, snapshot DoS, env bounds |
