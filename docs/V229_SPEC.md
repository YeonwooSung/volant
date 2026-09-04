# v0.229 — Kafka TransactionLog schemas on `__transaction_state`

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** When `VOLANT_TRANSACTION_STATE_TOPIC=1`, write
`__transaction_state` records as Kafka **TransactionLogKey /
TransactionLogValue v0** (KIP-890 coordinator-log schemas), not Volant
JSON. **Dual-read** legacy JSON v1 so existing logs still replay.

This is residual **v0.229**. It is **not** full KIP-890 / KIP-939,
**not** KRaft control records, and **not** a new Kafka API key. The
flag stays **default off**. `__txn_prepared` still holds ranges.
`__txn_coordinator` remains the FindCoordinator routing map.

## Goals

1. Opt-in write path encodes classic (not flexible) Kafka
   `TransactionLogKey` v0 + `TransactionLogValue` v0.
2. Dual-read: header `volant-txn-state=1` **or** a `{` JSON body still
   parses as [`TransactionStateRecord`]. Kafka keys/values decode
   otherwise (raw utf8 key fallback).
3. In-memory view stays `TransactionStateRecord` (Describe / List /
   tests). Only on-disk topic bytes change.
4. v0.226 leftover last-write `ongoing` still becomes `complete_abort`
   (now as Kafka bytes).
5. Flag **off**: topic is still not auto-created.
6. Encode/decode are `pub` so tests can hex/roundtrip without produce.

## Non-goals

| Deferred | Why |
|----------|-----|
| Full KIP-890 / KIP-939 | Coordinator rewrite; this is log bytes only |
| KRaft / `__cluster_metadata` control records | Different topic + schemas |
| Kafka TV2 / `ClientTransactionVersion` writes | Write v0 only; v1 accepted on read |
| Replacing `__txn_prepared` | File snapshot still holds ranges + LSO |
| Replacing `__txn_coordinator` | Routing map (Phase 124), not a txn log |
| Default-on flag | Phase 90 / 114 tests stay env-free |
| Changelog required for EOS | Orthogonal stream leftover |
| New Kafka API keys | `SUPPORTED_APIS` unchanged |
| Groups / Join / key 45/46 | Orthogonal |

## Default

| Knob | Default | Notes |
|------|---------|-------|
| `VOLANT_TRANSACTION_STATE_TOPIC` | **off** | `1` / `true` / `yes` enables. Snapshotted at `Broker::new` / `with_cluster`. |

## On-disk format (flag on)

### Key — `TransactionLogKey` v0 (classic)

```
int16 version = 0
string transactionalId   // int16 length + utf8
```

### Value — `TransactionLogValue` v0 (classic; read v0 and v1)

```
int16 version
int64 producerId
int16 producerEpoch
int32 transactionTimeoutMs      // 0 if unknown
int8  transactionStatus
nullable array of { string topic; array of int32 partitions }
int64 transactionLastUpdateTimestampMs
int64 transactionStartTimestampMs
```

v1 adds `ClientTransactionVersion` (int16) at the end — **ignored on
read**, never written.

| Header | Meaning |
|--------|---------|
| `volant-txn-state` = `1` | Legacy Volant JSON v1 (read only) |
| `volant-txn-state` = `2` | Kafka TransactionLog v0 (current write) |

### Status bytes

| byte | `TransactionStateRecord.state` |
|-----:|--------------------------------|
| 0 | `empty` |
| 1 | `ongoing` |
| 2 | `prepare_commit` |
| 3 | `prepare_abort` |
| 4 | `complete_commit` |
| 5 | `complete_abort` |
| 6 | `PrepareEpochFence` — decode as `complete_abort`; **do not write 6** |

`transactionStartTimestampMs` is the existing `txn_start_ms` (`0` if
unknown). `transactionLastUpdateTimestampMs` is unix ms now on write.
`TransactionPartitions` is **null** on write (honest: bodies still live
in `__txn_prepared`).

## Dual-read

`read_transaction_state_latest` / `read_transaction_state_log`:

1. Header `1` **or** value starts with `{` → parse JSON
   `TransactionStateRecord`. Key may be raw utf8.
2. Else decode `TransactionLogKey` (fall back to raw utf8 on failure)
   and `TransactionLogValue`. Skip undecodable records.
3. Last-write-wins unchanged.

Writes never fail Init / EndTxn if encode or produce fails (best-effort).

## Tests

```bash
cargo test -p volant-broker --lib txn_state -- --test-threads=1
cargo test -p volant-broker --test v13_transaction_state -- --test-threads=1
cargo test -p volant-broker --test v229_txn_log_schema -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Flag on + Init + begin + produce + EndTxn abort | last record is Kafka value; status **5** `complete_abort`; key decodes `transactionalId` |
| Flag on + write Kafka + drop + reopen | replay still works (Describe Empty / last `complete_*`) |
| Seed JSON v1 (`header=1`, raw key, JSON body) then reopen with flag on | dual-read succeeds; leftover `ongoing` JSON becomes `complete_abort` (Kafka bytes) |
| Flag off | topic not created |
| encode/decode roundtrip | key + value v0 hex |

## Honesty leftovers

- Not KRaft control records / not `__cluster_metadata`.
- Not Kafka TV2 / `ClientTransactionVersion` writes (v1 value accepted on read only).
- Partitions in the value may be null; `__txn_prepared` still holds ranges.
- Flag still default **off**.
- Not a new Kafka API key.
- Not full KIP-890/939 (no share groups, no TV2, no coordinator rewrite).

## Related

- [V13_SPEC.md](./V13_SPEC.md) — opt-in `__transaction_state` MVP (JSON)
- [V226_SPEC.md](./V226_SPEC.md) — leftover `ongoing` → `complete_abort`
