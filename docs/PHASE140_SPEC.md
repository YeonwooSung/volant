# Phase 140 — Preferred-replica selector depth

**Status:** ✅ Done  

**Theme:** Observability + optional freshness lag on top of Phase 126/133.
Does **not** re-ship usable-addr or LEO ranking (already 133).

## Goals

1. **Optional max LEO lag vs leader:** env
   `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` — when set (parsed u64), skip peers
   with `leader_leo - follower_leo > lag` in addition to LEO≥HWM. **Unset** =
   unlimited (126/133 behavior unchanged).
2. **Suppress metric:** `volant_preferred_replica_suppressed_total` — increment
   when Fetch would have a preferred candidate but is suppressed
   (READ_COMMITTED + selector `Some`).
3. **Tests:** multi-rack never selects other rack; lag env excludes stale;
   default unlimited regression; RC increments suppress.
4. Living docs + KAFKA_COMPAT honesty catch-up (126+133+140).

## Non-goals

| Deferred | Why |
|----------|-----|
| Re-implement usable-addr / LEO ranking | Phase 133 |
| Full Kafka selector / throttling | Product residual |
| Preferred × session thrash suppress | Orthogonal 119/138 |
| Rack-aware partition assignment | Orthogonal |
| TCP probe | Config+liveness only |

## Ranking (fixed policy)

```text
candidates = ISR − self ∩ live ∩ usable_addr ∩ same_rack
             ∩ LEO≥HWM ∩ (optional lag ≤ max_leo_lag)
rank (leo desc, id asc)
```

## Exit criteria

1. Lag env unset → phase126/133 green, behavior unchanged  
2. Lag env set → over-lag peers not preferred  
3. Different rack never preferred  
4. READ_COMMITTED increments suppressed when candidate exists  
5. Docs honesty  
