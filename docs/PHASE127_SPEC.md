# Phase 127 — Txn coordinator registry TTL GC (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** durable last-touch timestamps (file v2) + `expire_stale` — **landed**  
- **PR2** sweeper / env TTL + metrics — **landed**  
- **PR3** unit + `phase127_txn_coordinator_gc` tests — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Phase 124 durable Init-owner maps no longer grow without bound —
stale completed-txn entries expire after a configurable TTL.

## Goals

1. **Last-touch timestamps** on every `note` (transactional_id + producer_id).
2. **Durable v2 snapshot** with `id_last_ms` / `pid_last_ms`; v1 files load with
   “now” timestamps (full TTL grace from restart).
3. **TTL GC:** drop entries with `last_ms <= now - ttl`; persist after removals.
4. **Config:** `VOLANT_TXN_COORDINATOR_TTL_MS` (default **24h**; `0` disables).
5. **Sweeper hook:** `Broker::sweep_timeouts` also runs registry GC.
6. **Metric:** `volant_txn_coordinator_registry_gc_total`.
7. Tests + living-docs honesty.

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Full `__transaction_state` / KIP-890/939 | Separate design |
| Controller-shared registry | Orthogonal |
| GC on EndTxn complete (eager) | TTL sufficient for MVP |
| BROKER Describe/Alter surface for TTL | Env-only MVP |
| Shared session store / preferred selector parity | Orthogonal |

## Design

```text
note(id/pid, coord) → maps + last_ms = now → persist
sweep / expire_stale(ttl):
  remove keys where last_ms + ttl <= now
  if any removed → gc_total += n; persist
```

## Honest limitations

- Wall-clock based (not monotonic); clock skew may delay/advance GC
- **Long-lived open txns can lose Init-owner mapping:** GC drops
  `__txn_coordinator` entries when `last_touch` age exceeds TTL even if the
  transaction is still open. Only **re-note** (re-Init / open fan-out that
  touches the registry) refreshes `last_ms`. After drop, FindCoordinator
  override and EndTxn / AddOffsets / TxnOffsetCommit forward fall back to the
  hash ring only → risk of **wrong coordinator** until re-Init.
  **Operators / clients:** re-note within TTL; set
  `VOLANT_TXN_COORDINATOR_TTL_MS=0` (disable GC); lengthen the TTL; or Alter
  `volant.txn.coordinator.registry.ttl.ms` (Phase 128)
- Pid-only orphans after re-Init still expire only via TTL
- Not cluster-wide GC coordination

## Exit criteria

1. Stale entries removed after TTL; fresh retained  
2. `0` TTL disables GC  
3. v1 snapshot still loads  
4. GC persists across reload  
5. `phase127_txn_coordinator_gc` + unit tests pass  
6. Living docs 0–127 honest  
