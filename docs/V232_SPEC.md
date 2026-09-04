# v0.232 — Write open/prepared partitions on txn-state log

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** When `VOLANT_TRANSACTION_STATE_TOPIC=1`, Kafka
`TransactionLogValue.partitions` is populated from the open / prepared
txn instead of always **null** (v0.229 leftover).

This is residual **v0.232**. It is **not** full KIP-890 / KIP-939,
**not** Kafka TV2, and **not** a replacement for `__txn_prepared`.
The flag stays **default off**. `__txn_prepared` still holds ranges.

## Goals

1. `append_transaction_state` fills `partitions` for **`ongoing` /
   `prepare_commit` / `prepare_abort`** from in-memory `OpenTxn`
   (`added` + `written`, plus pending keys) and/or `PreparedTxn.open`
   for that `producer_id`.
2. Topics and partition ids are sorted for determinism.
3. Empty set → **null** (not an empty array), same as unknown.
4. **`empty` / `complete_commit` / `complete_abort`** stay **null**
   (honest: the txn no longer has a live set).
5. Dual-read / JSON v1 replay unchanged. Encode still writes Kafka
   value **v0** only (no TV2).
6. Flag **off**: topic is still not auto-created.

## Non-goals

| Deferred | Why |
|----------|-----|
| Default-on flag | Phase 90 / 114 tests stay env-free |
| Replacing `__txn_prepared` | File snapshot still holds ranges + LSO |
| Kafka TV2 / `ClientTransactionVersion` writes | Write v0 only; v1 accepted on read |
| New Kafka API keys | `SUPPORTED_APIS` unchanged |
| Groups / Join / Fetch / SCRAM | Orthogonal |
| Full KIP-890 / KIP-939 | Coordinator rewrite; this is log bytes only |

## Default

| Knob | Default | Notes |
|------|---------|-------|
| `VOLANT_TRANSACTION_STATE_TOPIC` | **off** | `1` / `true` / `yes` enables. Snapshotted at `Broker::new` / `with_cluster`. |

## Semantics

```
append_transaction_state(state, producer_id)
  │
  ├─ ongoing / prepare_commit / prepare_abort
  │     ├─ OpenTxn for producer_id → topics_from_open (added + written + pending)
  │     ├─ else PreparedTxn.open with that producer_id
  │     ├─ empty → partitions = null
  │     └─ else → Some(sorted topic → sorted partition ids)
  │
  └─ empty / complete_commit / complete_abort
        └─ partitions = null
```

`Broker::txn_log_partitions(producer_id)` is the lookup. It does not
change `__txn_prepared` SoT for ranges.

## Tests

```bash
cargo test -p volant-broker --lib txn_state -- --test-threads=1
cargo test -p volant-broker --test v13_transaction_state -- --test-threads=1
cargo test -p volant-broker --test v229_txn_log_schema -- --test-threads=1
cargo test -p volant-broker --test v232_txn_log_partitions -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Flag on + begin + AddPartitions/produce + EndTxn prepare | last Kafka value has those topic/partitions |
| `complete_abort` / `complete_commit` last-write | partitions **null** |
| Flag off | no topic |
| encode/decode | still roundtrips `Some(partitions)` |

## Honesty leftovers

- `complete_*` / `empty` stay **null** (no live set after finalize).
- `__txn_prepared` is still range SoT (LSO / KeepPrepared / restart).
- Flag still default **off**.
- Not Kafka TV2 / not a new Kafka API key.
- Not groups / Join / Fetch / SCRAM.
- Not full KIP-890/939.

## Related

- [V13_SPEC.md](./V13_SPEC.md) — opt-in `__transaction_state` MVP
- [V226_SPEC.md](./V226_SPEC.md) — leftover `ongoing` → `complete_abort`
- [V229_SPEC.md](./V229_SPEC.md) — Kafka TransactionLog schemas (partitions always null)
