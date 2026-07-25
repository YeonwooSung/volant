# Phase 123 — DeleteRecords outbox leadership handoff (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** new-leader reconcile from local `log_start` + epoch stamp — **landed**  
- **PR2** background reconcile + metrics — **landed**  
- **PR3** multi-node leadership-change / kill-old-leader tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Cluster correctness — when the partition leader that held a durable
DeleteRecords outbox is demoted or dies, **pending truncates for offline peers
are not permanently lost**. The new leader rebuilds pending targets from its
local log start so peers still catch up via at-least-once `ReplicaDeleteRecords`.

## Goals

1. **Close permanent loss on leadership change:** If leader A truncated and
   enqueued peer C while C was offline, then A dies (or is demoted) and B
   becomes leader, B still drives C's log start catch-up when C returns.
2. **Smallest honest design:** New leader **reconciles** pending truncate
   targets from **local `log_start`** for partitions it leads — no controller
   SoT queue, no Raft, no bulk outbox file transfer required for MVP.
3. **Reuse Phase 113/116 transport:** opcode 70/71 `ReplicaDeleteRecords` only;
   existing outbox merge/drain/idempotent apply.
4. **At-least-once OK:** re-delivery and double reconcile remain safe (log start
   only advances).
5. **Client semantics unchanged:** DeleteRecords success still based on **local
   leader** truncate only.
6. Integration test: enqueue outbox while follower down → leadership change /
   kill old leader → new leader drains / peer catch-up still happens.
7. Living docs honesty (still not a consensus truncate log).

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Raft / dynamic membership | Out of scope |
| Controller SoT truncate journal | Extra hop; not required for MVP |
| Sync 2PC truncate (client waits on all replicas) | Latency; rejected in 113 |
| Multi-lang clients / chaos-mesh / long fuzz | Orthogonal |
| Full KIP-890/939 / `__transaction_state` | Orthogonal |
| Cross-DC async replication | Out of scope |
| Bulk outbox RPC transfer from demoted leader | Optional stretch; reconcile covers death |

## Problem (today — post Phase 116)

```text
  client DeleteRecords → leader A local truncate OK
       → ReplicaDeleteRecords to live peers
       → peer C offline → outbox on A for C
                              │
                    A dies / leadership → B
                    A's outbox orphaned (disk gone or demoted)
                              │
                    C restarts later with **stale** log start forever
                    (until operator re-issues DeleteRecords or retention)
```

Phase 116 closed offline-peer retry **while the same leader stays up**.
Leadership change permanently lost the pending set.

## Design principles

1. **New leader rebuilds** pending targets from **its own** advanced `log_start`
   (the usual case: B was online during DeleteRecords and already truncated).
2. **No new public APIs** — internal reconcile + existing outbox drain.
3. **Idempotent peers** — log start only advances; re-apply is safe.
4. **Stamp current leader epoch** on reconcile so fenced stale-epoch drains
   from a demoted leader cannot block the new leader's retries.
5. **Bounded re-work** — reconcile once per `(leader_epoch, log_start)` per
   led partition (in-memory); re-run when either advances.
6. **Single-node unchanged** — no cluster ⇒ no reconcile traffic.
7. **Honest gaps** — if the **new** leader itself never applied the truncate
   (was offline during DeleteRecords and is elected anyway), its local
   `log_start` is stale and reconcile cannot invent a higher watermark;
   operator re-issue / retention still apply. Not multi-DC / not consensus.

---

## Architecture

### Chosen design: **reconcile from local log_start (Option A)**

| Piece | Role |
|-------|------|
| Leader-local `log_start` | Desired truncate target for peers |
| `reconcile_delete_records_outbox` | Enqueue outbox rows for assigned peers |
| Existing outbox + drain | At-least-once `ReplicaDeleteRecords` |
| Background loop (~500ms) | Reconcile then drain (cluster mode) |

Alternatives **not** chosen for MVP:

| Option | Why deferred |
|--------|----------------|
| B — demoted leader transfers outbox via RPC | Helps demotion-without-death only; hard kill still needs A |
| C — controller pending set | Extra SoT; not required once leaders hold advanced log start |

### Reconcile algorithm

```text
// cluster mode; run on background tick + tests may call explicitly
for each (topic, partition) where this node is leader:
  log_start = local log_start_offset
  epoch     = local leader_epoch
  if log_start == 0: continue
  if last_reconcile[(topic, partition)] == (epoch, log_start): continue
  for each replica_id in assignment replicas except self:
    if broker_addr(replica_id) known:
      enqueue(replica_id, topic, partition, before_offset=log_start, leader_epoch=epoch)
        // max-merge existing keys (Phase 116)
  last_reconcile[(topic, partition)] = (epoch, log_start)
  reconcile_total += 1  // per partition reconcile pass that enqueued work path
```

`last_reconcile` is **in-memory only** (per process). Restart as leader re-runs
reconcile (safe; may briefly re-enqueue already-applied peers — drain succeeds
idempotently and removes keys).

### Drain epoch refresh (small honesty fix)

When draining an outbox entry, if this node still **leads** that partition,
stamp the **current** local `leader_epoch` on the RPC (not a stale stored epoch).
Avoids self-fencing after an epoch bump on the same leader. Demoted leaders may
still drop fenced entries (Phase 116); the new leader's reconcile re-creates them.

### When reconcile runs

| Trigger | Action |
|---------|--------|
| Background cluster loop (~500ms) | `reconcile` then `drain` (existing drain if depth > 0) |
| Explicit test / ops helper | `Broker::reconcile_delete_records_outbox()` |
| Local DeleteRecords fan-out | Unchanged Phase 113/116 path (immediate + enqueue) |

### Metrics

| Metric | Type | Meaning |
|--------|------|---------|
| existing Phase 116 gauges/counters | — | depth / enqueue / retry / drops |
| `volant_delete_records_outbox_reconcile_total` | counter | Partition reconcile passes that advanced `last_reconcile` |

### Client / Kafka surface

Unchanged: DeleteRecords (key 21) → local leader; fan-out + outbox + reconcile
internal. No new public API keys. No new inter-broker opcodes.

## Contract preserved

- Phase 113 peer apply + epoch fence
- Phase 116 durable outbox merge / capacity / live-set drain
- Client success = local leader only
- Single-node DeleteRecords bitwise-unchanged (no outbox traffic)
- At-least-once + idempotent log_start advance

## Tests

`crates/volant-broker/tests/phase123_delete_records_outbox_handoff.rs`:

1. **Leadership change handoff:** 3-node RF=3; produce + replicate; kill follower C;
   DeleteRecords on leader A → outbox for C; kill A / `on_broker_death` → B leads;
   propagate assignment; B `reconcile` → outbox for C with B's log_start/epoch;
   restart C; drain → C log_start ≥ leader low
2. **Reconcile unit smoke:** leader with advanced log_start enqueues peers; second
   reconcile with same (epoch, log_start) is a no-op for `last_reconcile`
3. **Phase 116 regression path still works** when same leader stays up (covered by
   `phase116_*` band)

Regression band: `phase116_*`, `phase113_delete_records_fanout`.

## Exit criteria

1. Multi-node handoff test green (offline peer catch-up after leadership change)  
2. `cargo test -p volant-broker --test phase123_delete_records_outbox_handoff` green  
3. `phase116_*` + `phase113_delete_records_fanout` green  
4. Spec + ROADMAP / PHASE_HISTORY / INDEX / consistency / ops / features /
   KAFKA_COMPAT honest updates  
5. Workspace builds; commit on `main`

---

## Honest limitations (after ship)

- **Not** a consensus truncate log; reconcile uses the **new leader's** local
  log start only  
- If the elected leader never applied the original truncate (offline during
  DeleteRecords), pending peers may remain stale until re-issue / retention  
- Demoted leader's on-disk outbox is not transferred; new leader rebuilds  
- Bounded outbox may still drop distinct keys under extreme backlog  
- Whole-segment DeleteRecords only (Phase 14 storage rule)  
- Inter-broker RPCs still not ACL-gated (shared-token / TLS)  
- No multi-DC / async replication story  

---

## Still deferred after Phase 123

- Multi-lang clients / chaos-mesh / long fuzz  
- Full KIP-890/939 / `__transaction_state`  
- Raft / dynamic membership  
- Controller SoT truncate journal / bulk outbox transfer  
- Per-broker BROKER config overrides  
- Preferred replica / shared session store  

---

## Decision log

| Decision | Choice | Alternative rejected |
|----------|--------|----------------------|
| Handoff mechanism | New leader reconcile from log_start | Outbox RPC transfer only; controller queue |
| Transport | Reuse `ReplicaDeleteRecords` | New handoff opcode |
| Dedup | In-memory last (epoch, log_start) per TP | Re-enqueue every tick (RPC spam) |
| Drain epoch | Refresh to current if still leader | Always use stored epoch (self-fence risk) |
| Death of old leader | Rebuild on B | Require A alive for transfer |

---

## Document map

| Doc | Role after ship |
|-----|-----------------|
| This file | Binding ship record for Phase 123 |
| [consistency.md](./consistency.md) | Leadership handoff honesty |
| [ops.md](./ops.md) | Reconcile metric + failover note |
| [features.md](./features.md) | Close “no outbox handoff” limitation |
| [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) | DeleteRecords cluster row |
| [../ROADMAP.md](../ROADMAP.md) | Phase 123 entry + deferred list |
| [history/PHASE_HISTORY.md](./history/PHASE_HISTORY.md) | One-line index |

---

## Getting started

```bash
cargo test -p volant-broker --test phase123_delete_records_outbox_handoff
cargo test -p volant-broker --test phase116_delete_records_outbox
cargo test -p volant-broker --test phase113_delete_records_fanout
```
