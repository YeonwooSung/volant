# Phase 128 — BROKER config for txn coordinator registry TTL (MVP)

**Status:** ✅ Shipped (MVP vertical slice)  
**Implementation:**  
- **PR1** `volant.txn.coordinator.registry.ttl.ms` on BROKER Describe/Alter — **landed**  
- **PR2** live AtomicU64 + sparse durable + fan-out path reuse — **landed**  
- **PR3** `phase128_txn_coordinator_ttl_config` + phase99 key-count update — **landed**  
- **PR4** living docs honesty — **landed**  
**Theme:** Phase 127 TTL is no longer env-only — operators can Describe/Alter it
like other Phase 99 knobs, with sparse durable restart and cluster controller
fan-out.

## Goals

1. **New BROKER key:** `volant.txn.coordinator.registry.ttl.ms`
2. **Product default:** 24h (same as Phase 127); `0` disables GC
3. **Precedence:** product default → `VOLANT_TXN_COORDINATOR_TTL_MS` at open →
   sparse durable file → runtime Alter
4. **Live GC** uses the process AtomicU64 (not re-read env on every sweep)
5. Describe returns the seventh knob; phase99 map len → **7**
6. Tests + living-docs honesty

## Non-goals

| Deferred | Why / next home |
|----------|-----------------|
| Per-broker heterogeneous overrides without controller | Phase 113 still homogeneous fan-out |
| Full Kafka DynamicBrokerConfig catalog | Orthogonal |
| Eager EndTxn registry GC | Orthogonal |
| Shared session store | Orthogonal |

## Design

```text
Broker.txn_coordinator_ttl_ms: AtomicU64
  init: effective_txn_coordinator_ttl_ms()  // env or 24h
  Describe: include key
  Alter / IncrementalAlter / durable load / peer apply: set atomic
  expire_txn_coordinator_registry: read atomic (not env)
```

## Honest limitations

- Cluster alters remain **controller-only** with generationed push (homogeneous)
- Env after process start is ignored until restart unless Alter/setter used
- Not a full Kafka broker config catalog entry
- **Same Phase 127 sharp edge:** wall-clock registry TTL can still drop
  long-lived Init-owner mappings that are not re-noted within TTL (set `0` /
  longer TTL via env or this key; clients re-Init / re-note)

## Exit criteria

1. Describe shows key at default 24h  
2. Alter 120000 survives restart via sparse durable  
3. Alter `0` disables GC  
4. DELETE restores product default  
5. Tests pass; docs 0–128 honest  
