# Phase 93 — Open transaction timeout (MVP)

## Goals

1. Honor InitProducerId **`transaction_timeout_ms`** (or a broker default/env when
   the client field is absent / non-positive) for **open** (non-prepared)
   write-through transactions.
2. Track **`opened_at_ms`** when a txn becomes open.
3. On timeout: **abort** the open txn (soft markers + ABORT control batches as
   Phase 86/89 abort path; drop deferred offsets).
4. **Lazy** expiry on produce / add-partitions / EndTxn / List / Describe / LSO
   paths (same pattern as Phase 92) so Fetch isolation advances without a
   background thread.
5. Interact cleanly with Phase 90/92 prepared path (open timeout must not break
   prepare/complete; prepared still uses prepared timeout).
6. Tests (`phase93_*.rs`) + living docs honesty.

## Non-goals

- Multi-broker coordinator clocks / Raft / txn log replication
- Full `TRANSACTION_ABORTABLE` emission (stretch only if tiny — **deferred**)
- Session TTL
- Multi-lang clients / fuzz CI
- Background sweeper thread (lazy is fine)
- Changing prepared-txn timeout semantics (Phase 92 remains SoT for Prepare*)

## Design (honest MVP)

### When does the open clock start?

| Event | Behavior |
|-------|----------|
| `begin_txn` / `ensure_txn_open` that creates the open entry | Sets `opened_at_ms = now` |
| First produce inside an already-open txn | Does **not** reset the clock |
| EndTxn prepare (2PC) | Open entry leaves `open_txns`; clock no longer applies; prepared uses `prepared_at_ms` + prepared timeout |
| Fence / re-InitProducerId | Open ranges abort immediately (existing path); new open starts a new clock |

Kafka has no separate BeginTxn API; Volant opens on AddPartitionsToTxn /
`ensure_txn_open` (or explicit `begin_txn`). That is the documented start.

### Timeout source of truth

| Source | Behavior |
|--------|----------|
| InitProducerId `transaction_timeout_ms` **> 0** | Stored on producer; used for subsequent open txns |
| InitProducerId `transaction_timeout_ms` **≤ 0** or non-Kafka paths | Use broker default |
| Broker default | **60_000 ms** |
| Env `VOLANT_OPEN_TXN_TIMEOUT_MS` | Read at `Broker::new` / `with_cluster` as broker default |
| `Broker::set_open_txn_timeout_ms(ms)` | Runtime override of broker default (tests / ops) |
| Effective timeout **0** | **Disabled** for that open txn (no auto-abort) |

**Choice rationale:** Phase 92 introduced a broker-level prepared timeout only.
Phase 93 closes the InitProducerId honesty gap for **open** write-through txns
by storing the client-supplied timeout on the producer and applying it from
`opened_at_ms`. Prepared txns continue to use `prepared_txn_timeout_ms` /
`VOLANT_PREPARED_TXN_TIMEOUT_MS` exclusively.

Per-producer timeout is durable under `__producer_state` so re-Init after
restart keeps the last configured client timeout. Open txn state itself remains
memory-only (crash ≡ abort via `__txn_markers`); no durable `opened_at_ms`.

### Auto-abort effect

Identical to EndTxn(abort) finalize for an open (non-prepared) txn:

1. Soft abort markers for write-through ranges
2. ABORT control batches on each written partition (Phase 89 dual-write)
3. Drop deferred offsets (never applied)
4. Remove from `open_txns` + persist `__txn_markers`

Data stays on the log for `READ_UNCOMMITTED` and is hidden under
`READ_COMMITTED` / native committed-only.

### Lazy sweep approach

No background thread. `expire_timed_out_open_txns()` runs together with
prepared expiry (`expire_timed_out_txns` helper) at the start of:

| Path | Why |
|------|-----|
| `init_producer_id_with_opts` | Fence / KeepPrepared must see post-timeout state |
| `end_txn` | Must not commit/prepare a timed-out open txn |
| `begin_txn` / `ensure_txn_open` | Produce-side open checks |
| Idempotent produce / add-partitions / buffer offsets | Same |
| `list_open_transactions` | ListTransactions honesty |
| `describe_transaction` | DescribeTransactions honesty |
| `last_stable_offset` | Fetch LSO advances after timeout without a txn API |
| `is_unstable_offset` | Isolation reads stay correct |

Cheap when `open_txns` is empty (typical).

### Interaction with prepared (Phase 90/92)

| Case | Behavior |
|------|----------|
| Open within timeout, EndTxn prepare | Moves to prepared; open timeout no longer applies |
| Prepared within prepared timeout, complete | Unchanged Phase 90/92 |
| Open times out before EndTxn | Auto-abort; subsequent EndTxn → `InvalidTxnState` |
| Prepared times out | Phase 92 force-abort; open map already empty |
| Both maps non-empty (different producers) | Each expiry path only touches its own map |

### DescribeTransactions fields

When **Ongoing** (open, not prepared):

- `transaction_timeout_ms` → effective open timeout (client or broker default; `0` if disabled)
- `transaction_start_time_ms` → `opened_at_ms`

Prepared still reports prepared timeout + `prepared_at_ms` (Phase 92).
Empty still reports `0` / `0`.

### Semantics table

| Case | Behavior |
|------|----------|
| Open, EndTxn commit before timeout | Unchanged commit finalize |
| Open, sleep past timeout, Describe/List | Empty / not listed; auto-aborted |
| Open timed out, Fetch READ_COMMITTED | LSO catches HWM (absent other open/prepared); payload hidden |
| Open timed out, EndTxn(commit) | `InvalidTxnState` |
| Timeout disabled (effective `0`) | Open held until EndTxn / fence |
| Client timeout on InitProducerId | Stored; used for next open |
| 2PC prepare before open timeout | Prepare succeeds; prepared timeout owns the rest |
| Crash with open write-through | Existing crash≡abort via markers (no open-timeout clock needed) |

## Exit criteria

1. Open entries carry `opened_at_ms` from begin / ensure-open
2. InitProducerId `transaction_timeout_ms` stored per producer; broker default via env + setter; effective `0` disables
3. Timeout → abort (soft + control); LSO/isolation correct; deferred offsets dropped
4. Active EndTxn before timeout still works
5. Prepared path regression smoke green
6. `phase93_*` + prior txn phases green
7. Docs: open-txn timeout honesty closed; prepared path still separate

## Honest limitations

- Lazy expiry only (no background sweeper); idle open may linger until an
  API/LSO path runs — Fetch LSO path covers the common consumer case
- Single-node clock; no multi-broker coordinated expiry
- No `TRANSACTION_ABORTABLE` error code surface
- No Kafka `transaction.max.timeout.ms` clamp / coordinator config surface
- Open `opened_at_ms` is memory-only (crash already aborts open ranges)
- Does not re-time open clock on produce; only begin/ensure-open

## Phase 94 ideas

- `TRANSACTION_ABORTABLE` where Kafka emits it → **shipped Phase 94 (honest subset)**
- Background txn sweeper / metrics (expired open + prepared counts)
- `transaction.max.timeout.ms` broker clamp
- Multi-broker prepared / open replication
- Session TTL / max sessions
- Multi-lang clients / cargo-fuzz corpus CI
