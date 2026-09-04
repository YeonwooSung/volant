# v0.226 — Record open-txn abort on opt-in `__transaction_state`

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V13_SPEC.md](./V13_SPEC.md): when
`VOLANT_TRANSACTION_STATE_TOPIC=1`, **open** crash≡abort and timeout
paths that did not write the coordinator log now append a single
`complete_abort`. Replay / Describe / List then match **open ≡ abort**.

This is residual **v0.226**. It is **not** Kafka / KIP-890 / KIP-939
schemas, **not** a replacement for `__txn_prepared` ranges, and **not**
a new Kafka API key. The flag stays **default off**. Records stay
**Volant JSON**.

## Goals

1. Crash≡abort of an **open** (non-prepared) txn writes
   `complete_abort` on `__transaction_state` when the flag is on
   (Phase 98/105 control + soft markers unchanged).
2. Restart replay of a leftover last-write `ongoing` does **not**
   restore Ongoing (restart ≡ crash ≡ abort). It appends
   `complete_abort` so last-write-wins matches Describe / List
   (covers begin-only opens with no `__txn_markers` ranges).
3. Open-txn timeout / sweeper already wrote `complete_abort` (v0.13);
   keep that path and test it next to crash.
4. Flag **off**: topic is still not auto-created; Phase 90 / 98 paths
   unchanged.
5. Do **not** default the flag on. Do **not** require changelog for
   ExactlyOnce. Do **not** add Kafka keys. Do **not** replace
   `__txn_prepared`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka KIP-890 / KRaft record schemas | Still Volant JSON v1 |
| Kafka `TxnState` / new API keys | `SUPPORTED_APIS` frozen at 38 |
| Replacing `__txn_prepared` | File snapshot still holds ranges + LSO |
| Replacing `__txn_coordinator` | Routing map (Phase 124), not a txn log |
| Default-on flag | Phase 90 / 114 tests stay env-free |
| Changelog required for EOS | Orthogonal stream leftover |

## Default

| Knob | Default | Notes |
|------|---------|-------|
| `VOLANT_TRANSACTION_STATE_TOPIC` | **off** | `1` / `true` / `yes` enables. Snapshotted at `Broker::new` / `with_cluster`. |

## When we append `complete_abort` (open)

| Event | Topic write |
|-------|-------------|
| Open timeout / sweeper (`expire_timed_out_open_txns`) | `complete_abort` (already v0.13) |
| Crash promote of `__txn_markers` open / `open_added` | `complete_abort` (this slice) |
| Replay leftover last-write `ongoing` | `complete_abort`; **do not** restore open (this slice) |
| Fence / KeepPreparedTxn=false / prepared timeout | Unchanged (`complete_abort`) |
| KeepPreparedTxn=true | Still does **not** rewrite `prepare_*` |

## Recovery

On start with the flag **on**:

1. Load `__txn_markers` (promote open → aborted + ABORT control).
   Each crash-promoted pid with a transactional id appends
   `complete_abort`.
2. Load `__txn_prepared` as today.
3. Replay last-write-wins. A leftover `ongoing` is treated as
   crash≡abort (write `complete_abort`, leave memory Empty).
   `prepare_*` still restores prepared. `complete_*` / `empty` drop
   prepared + open.
4. Lazy expire of any remaining timed-out prepared (open already
   aborted).

Describe / List read memory after that sequence, so they report
**Empty** (not Ongoing) for a crashed or timed-out open txn.

## Wire

Unchanged. Enable2Pc / KeepPreparedTxn / EndTxn / DescribeTransactions /
ListTransactions stay as Phase 90. No new keys.

## Tests

`crates/volant-broker/tests/v13_transaction_state.rs` (plus existing
Phase 90 / 93 / 98 paths):

| Case | Expect |
|------|--------|
| Flag on + open produce + drop + reopen | last record `complete_abort`; Describe Empty; List not Ongoing |
| Flag on + begin-only + drop + reopen | same (replay leftover `ongoing`) |
| Flag on + backdate + `expire_timed_out_open_txns` | `complete_abort`; Describe Empty |
| Flag off + crash reopen | topic not created; Describe Empty via markers |

```bash
cargo test -p volant-broker --lib -- --test-threads=1
cargo test -p volant-broker --test v13_transaction_state -- --test-threads=1
```

## Honesty leftovers

- Still Volant JSON, not Kafka / KIP-890 schemas.
- Flag still default **off**.
- `__txn_prepared` still holds prepared ranges / LSO (not replaced).
- `__txn_coordinator` remains the FindCoordinator routing map.
- Cluster non-controller still cannot create the topic.
- Stub replay without `__txn_prepared` still has no write ranges.

## Related

- [V13_SPEC.md](./V13_SPEC.md) — opt-in `__transaction_state` MVP
- [PHASE98_SPEC.md](./PHASE98_SPEC.md) — crash≡abort ABORT control
- [PHASE93_SPEC.md](./PHASE93_SPEC.md) — open-txn timeout
