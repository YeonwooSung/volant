# Phase 92 — Prepared transaction timeout / auto-abort (MVP)

## Goals

1. Track **`prepared_at_ms`** (unix epoch milliseconds) when a txn enters
   PrepareCommit / PrepareAbort.
2. Configurable **prepared timeout** with a sensible default (**60s**).
3. On timeout: **auto-abort** the prepared txn (same effect as force-abort /
   KeepPreparedTxn=false: soft markers + ABORT control batches).
4. **Lazy** expiry on API paths that touch prepared state (and LSO / isolation
   reads so Fetch isolation stays correct without a background thread).
5. Tests (`phase92_*.rs`) + living docs honesty.

## Non-goals

- Multi-broker coordinator clocks / Raft / txn log replication
- Multi-lang clients, fuzz CI
- Session TTL (separate phase)
- Full `TRANSACTION_ABORTABLE` emission (stretch only if tiny — **deferred**)
- Timing out **open** (non-prepared) write-through transactions
  (`transaction_timeout_ms` on InitProducerId remains ignored for open txns)

## Design (honest MVP)

### Timeout source of truth

| Source | Behavior |
|--------|----------|
| Default | **60_000 ms** (matches common Kafka client `transaction.timeout.ms`, not broker `transaction.max.timeout.ms` 15m) |
| Env `VOLANT_PREPARED_TXN_TIMEOUT_MS` | Read at `Broker::new` / `with_cluster` |
| `Broker::set_prepared_txn_timeout_ms(ms)` | Runtime override (tests / ops hooks) |
| `0` | **Disabled** — prepared never auto-aborts (operator opt-out) |

**Choice rationale:** InitProducerId still parses but **ignores** client
`transaction_timeout_ms` for open transactions (Phase 29/62 honesty). Phase 92
introduces a **broker-level prepared timeout only**, so hanging 2PC phase-1
state cannot pin LSO forever. Per-producer client timeouts for open txns remain
deferred.

### Timestamp

| Field | Where | Meaning |
|-------|-------|---------|
| `prepared_at_ms: i64` | In-memory `PreparedTxn` + durable `__txn_prepared/state.json` | Unix ms when prepare succeeded |

On load, missing / zero `prepared_at_ms` (pre-Phase-92 snapshots) is treated as
**now** so upgrades do not mass-abort existing prepared txns.

### Auto-abort effect

Identical to `force_abort_prepared` (Phase 90 KeepPreparedTxn=false path):

1. Soft abort markers for write-through ranges
2. ABORT control batches on each written partition (Phase 89 dual-write)
3. Remove from prepared map + persist `__txn_prepared`
4. Persist `__txn_markers`

**Always abort** on timeout, even if the prepared decision was PrepareCommit.
The prepared decision is discarded; data stays on the log for
`READ_UNCOMMITTED` and is hidden under `READ_COMMITTED` / native committed-only.

### Lazy sweep approach

No background thread. `expire_timed_out_prepared_txns()` runs at the start of:

| Path | Why |
|------|-----|
| `init_producer_id_with_opts` | KeepPrepared / fence must see post-timeout state |
| `end_txn` | Second EndTxn / prepare must not finalize a timed-out prepare as commit |
| `begin_txn` / `ensure_txn_open` | Produce-side open checks |
| Idempotent produce / add-partitions guards that reject when prepared | Same |
| `list_open_transactions` | ListTransactions honesty |
| `describe_transaction` | DescribeTransactions honesty |
| `last_stable_offset` | Fetch LSO advances after timeout without a txn API |
| `is_unstable_offset` | Isolation reads stay correct |

Cheap when the prepared map is empty (typical).

### Durable snapshot extension

```json
{
  "prepared": [
    {
      "transactional_id": "app-1",
      "producer_id": 1,
      "producer_epoch": 0,
      "commit": true,
      "prepared_at_ms": 1710000000000,
      "written": [ ... ],
      "pending": [ ... ],
      "deferred_offsets": []
    }
  ]
}
```

### Semantics table

| Case | Behavior |
|------|----------|
| Prepare, then complete before timeout | Unchanged Phase 90 finalize |
| Prepare, sleep past timeout, Describe/List | Empty (auto-aborted); no longer Prepare* |
| PrepareCommit timed out, Fetch READ_COMMITTED | LSO catches HWM (absent other open/prepared); payload hidden |
| PrepareCommit timed out, second EndTxn(commit) | `InvalidTxnState` (no longer prepared) |
| KeepPreparedTxn=true after timeout | OngoingTxn* = -1 (nothing prepared) |
| Timeout disabled (`0`) | Prepared held until EndTxn / re-init abort |
| Crash with prepared past timeout | Reload + first lazy sweep aborts |
| Pre-92 snapshot without `prepared_at_ms` | Clock starts at load time |

### DescribeTransactions fields

When prepared (and not yet expired):

- `transaction_timeout_ms` → configured prepared timeout (or `0` if disabled)
- `transaction_start_time_ms` → `prepared_at_ms`

(Open / Empty still report `0` / `0` — open txn start times remain untracked.)

## Exit criteria

1. Prepared entries carry durable `prepared_at_ms`
2. Default 60s timeout; env + setter override; `0` disables
3. Timeout → force-abort (soft + control); LSO/isolation correct
4. Non-timeout prepare→complete path still green
5. `phase92_*` + prior txn phases green
6. Docs: honest that open-txn `transaction_timeout_ms` is still ignored

## Honest limitations

- Open (non-prepared) transactions still have **no** timeout
- Client InitProducerId `transaction_timeout_ms` still **ignored** for open txns
- Lazy expiry only (no background sweeper); idle prepared may linger until an
  API/LSO path runs — Fetch LSO path covers the common consumer case
- Single-node clock; no multi-broker coordinated expiry
- No `TRANSACTION_ABORTABLE` error code surface
- Not full Kafka `transaction.max.timeout.ms` / coordinator config surface

## Phase 93 ideas

- Open-txn timeout using InitProducerId `transaction_timeout_ms`
- `TRANSACTION_ABORTABLE` where Kafka emits it
- Background prepared sweeper / metrics (expired count)
- Multi-broker prepared replication
- Session TTL / max sessions
- Multi-lang clients / cargo-fuzz corpus CI
