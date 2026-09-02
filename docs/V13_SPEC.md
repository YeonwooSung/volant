# v0.13 — `__transaction_state` coordinator log (KIP-890 MVP)

**Status:** Shipped (bounded MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Give the txn coordinator a **real internal log topic**, opt-in.

**Honesty:** this is **not** full KIP-890 / KIP-939, **not** Kafka
`__transaction_state` record format, and **not** a KRaft control topic.
Records are **Volant JSON**. Enable2Pc wire is unchanged. No new Kafka API
keys. `__txn_coordinator` (Phase 124) remains the FindCoordinator routing
map — this slice is the **state log**, not that map.

## Goals

1. Topic `__transaction_state`, 1 partition, created on first
   InitProducerId / first prepare when `VOLANT_TRANSACTION_STATE_TOPIC=1`.
2. RF = `min(3, N)` in cluster; RF = 1 on single-node.
3. JSON records keyed by `transactional_id`; last-write-wins per key.
4. Replay on broker start when the flag is on.
5. Default **off** so Phase 90 / 114 tests stay green with no env.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka KIP-890 / KRaft record schemas | Volant JSON only |
| Kafka `TxnState` / new API keys | `SUPPORTED_APIS` frozen at 38 |
| Replacing `__txn_prepared` | File snapshot still holds ranges + LSO |
| Replacing `__txn_coordinator` | Routing map (Phase 124), not a txn log |
| KIP-939 share groups | Out of scope |
| Multi-broker log replication | Topic RF follows create; no extra 2PC |

## Default

| Knob | Default | Notes |
|------|---------|-------|
| `VOLANT_TRANSACTION_STATE_TOPIC` | **off** | `1` / `true` / `yes` enables. Snapshotted at `Broker::new` / `with_cluster`. |

## Topic

- **Name:** `__transaction_state` (constant `TRANSACTION_STATE_TOPIC`)
- **Partitions:** 1
- **Create:** lazy on first InitProducerId (non-empty transactional id) or
  first prepare / complete / fence write when the flag is on
- **RF:** `min(3, N)` or 1 on single-node
- Flag **off:** topic is **not** auto-created; Phase 90 `__txn_prepared`
  path is unchanged

## Record format (Volant JSON v1)

| Field | Meaning |
|-------|---------|
| **key** | `transactional_id` bytes |
| **value** | `{"v":1,"state":"…","producer_id":…,"epoch":…,"txn_start_ms":…}` |
| **header** | `volant-txn-state` = `1` (ASCII) |

`state` is one of:

`empty` | `ongoing` | `prepare_commit` | `prepare_abort` |
`complete_commit` | `complete_abort`

### When we append

| Event | State |
|-------|--------|
| InitProducerId allocate / re-init after fence | `empty` |
| `begin_txn` / first open | `ongoing` |
| First Enable2Pc EndTxn | `prepare_commit` or `prepare_abort` |
| Second EndTxn / non-2PC one-shot | `complete_commit` or `complete_abort` |
| Fence / timeout / KeepPreparedTxn=false | **`complete_abort`** (single record; not prepare_abort then complete_abort) |

KeepPreparedTxn does **not** rewrite the log (last record stays `prepare_*`).

## Recovery

On start with the flag **on**:

1. Load `__txn_prepared` as today (Phase 90 ranges / LSO).
2. If `__transaction_state-0` exists, **replay** last-write-wins per
   `transactional_id`.
3. **If both exist, the topic is SoT for state** (a `complete_*` on the
   topic drops a leftover prepared file entry). Prepared **bodies**
   (written ranges, pending, deferred offsets) still come from
   `__txn_prepared` when the topic still says `prepare_*`.
4. If the prepared file is empty/missing and the topic says `prepare_*`,
   rebuild a **stub** prepared txn so KeepPreparedTxn / second EndTxn
   still work (ranges may be empty — LSO hold needs the file).

Prepared still survives restart (Phase 90 invariant) because the file is
loaded first and kept when the topic agrees it is prepared.

## Wire

Unchanged. Enable2Pc / KeepPreparedTxn / EndTxn / DescribeTransactions /
ListTransactions stay as Phase 90. Describe / List read memory, which
includes replayed state after restart.

## Tests

`crates/volant-broker/tests/v13_transaction_state.rs`:

1. Flag off: topic not auto-created; Phase 90 prepare path unchanged
2. Flag on + Enable2Pc: after first EndTxn, topic shows `prepare_commit`
3. Complete EndTxn → last record is `complete_commit`
4. Restart same `data_dir`, flag on → prepared visible (KeepPrepared + complete)
5. Fence abort writes `complete_abort`
6. Replay rebuilds prepared when `__txn_prepared` is deleted

## Honest leftovers

- Not Kafka `__transaction_state` / KIP-890 schemas
- Not a replacement for `__txn_prepared` ranges or `__txn_coordinator` routing
- Crash≡abort of **open** txns does not write the topic (markers still apply)
- Cluster non-controller cannot create the topic (controller-only create)
- Stub replay without `__txn_prepared` has no write ranges (LSO not held)
