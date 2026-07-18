# Phase 94 — TRANSACTION_ABORTABLE emission (honest subset MVP)

## Goals

1. Emit Kafka error **TRANSACTION_ABORTABLE (123)** in the subset of cases
   Volant can honestly support after open/prepared timeout auto-abort
   (Phases 92–93).
2. Keep **InvalidTxnState (48)** for classic invalid cases (never opened,
   prepared decision mismatch, non-transactional produce, etc.) so existing
   clients are not surprised.
3. Wire: `KafkaErrorCode::TransactionAbortable = 123` already defined; map
   protocol `ErrorCode::TransactionAbortable = 24` via `map_idempotent_error`.
   ApiVersions unchanged (no invented max versions).
4. Tests (`phase94_*.rs`) covering ≥2 honest emit paths + success still 0 +
   never-opened still 48.
5. Living docs honesty: KAFKA_COMPAT / ROADMAP / PHASE_HISTORY / WHITEPAPER /
   ops / features.

## Non-goals

- Full KIP-890 multi-broker abortable surface
- Emitting 123 on FindCoordinator (no txn state on that path)
- Multi-lang clients / fuzz CI
- Session TTL
- Multi-broker 2PC
- Background txn sweeper

## Design (honest MVP)

### Abortable producer set

When open or prepared timeout auto-abort runs (Phase 92/93 lazy expiry), the
producer id is inserted into an in-memory **`abortable_producers`** set.

| Event | Effect on set |
|-------|----------------|
| Open timeout auto-abort | `mark(pid)` |
| Prepared timeout auto-abort | `mark(pid)` |
| EndTxn with no open/prepared and pid in set | return **123**, `take` (clear) |
| Successful open finalize / prepared complete | `clear(pid)` (defensive) |
| InitProducerId fence / KeepPrepared path | `clear(pid)` |
| Intentional KeepPreparedTxn=false force-abort | `clear(pid)` (not a timeout signal) |

The set is **process-local / memory-only** (same lifetime as open-txn clocks).

### Emit matrix

| API / path | Condition | Kafka code | Notes |
|------------|-----------|------------|-------|
| Produce (txn write-through) | No open + abortable | **123** | After open/prepared timeout |
| Produce | No open + not abortable | **48** InvalidTxnState | Never began / already cleared |
| Produce | Prepared still live | **48** | Must complete with EndTxn |
| Produce | Non-transactional PID | **48** | Unchanged |
| EndTxn | No open/prepared + abortable | **123** then clear | Commit or abort decision |
| EndTxn | No open/prepared + not abortable | **48** | Classic empty |
| EndTxn | Prepared decision mismatch | **48** | Unchanged |
| EndTxn | Active open/prepare finalize | **0** | Unchanged |
| AddPartitionsToTxn | `ensure_txn_open` + abortable | **123** | Partition-level error |
| AddOffsetsToTxn (incl. v4) | `ensure_txn_open` + abortable | **123** | KIP-890 surface |
| TxnOffsetCommit (`buffer_txn_offsets`) | No open + abortable | **123** | Same mark |
| begin / ensure open | Abortable | **123** | Does **not** open a new txn until EndTxn clears |
| FindCoordinator | Any | **never 123** | No abortable state on this API |
| Success paths (open→produce→EndTxn) | Healthy txn | **0** | Regression |

### Why not map all InvalidTxnState → 123?

Kafka's TRANSACTION_ABORTABLE means "client should abort this transaction."
Volant only auto-aborts on **timeout**. Mapping every empty-txn produce to 123
would lie for "forgot to begin" and break clients that branch on 48.

### Client recovery sequence (honest)

1. Broker times out open/prepared → soft abort + mark abortable
2. Client Produce / AddOffsets / AddPartitions → **123**
3. Client EndTxn (any decision) → **123** and **clears** mark
4. Client AddPartitions / AddOffsets → opens new txn → **0**

Alternatively InitProducerId fence clears the mark with a new epoch.

### Wire / mapping

| Layer | Value |
|-------|------:|
| `volant_protocol::ErrorCode::TransactionAbortable` | 24 |
| `KafkaErrorCode::TransactionAbortable` | 123 |
| `map_idempotent_error(24)` | 123 |

No ApiVersions / max-version changes.

## Exit criteria

1. Open timeout → Produce returns 123
2. Open timeout → EndTxn returns 123 and clears abortable
3. Open timeout → AddOffsetsToTxn v4 returns 123
4. Prepared timeout → EndTxn returns 123
5. Healthy produce/EndTxn/AddOffsets still 0
6. Never-opened produce/EndTxn still 48 (not 123)
7. FindCoordinator still never emits 123 (phase81)
8. Unknown producer AddOffsets still never 123 (phase82)
9. `phase94_*` + prior txn phases green
10. Docs updated

## Honest limitations

- Only timeout auto-abort marks abortable (not mid-txn partition failures,
  not multi-broker coordinator decisions)
- FindCoordinator never returns 123 (no Volant state for it)
- Abortable set is memory-only; restart ≡ crash-abort via markers without 123
  on the next process
- Not full KIP-890 abortable defense (no per-partition abortable during open)
- Lazy expiry only — 123 appears after an API/LSO path runs expiry
- Clients that ignore 123 and only handle 48 still see non-zero errors

## Phase 95 ideas

- Background txn sweeper + metrics (expired open/prepared/abortable counts)
- `transaction.max.timeout.ms` broker clamp → **closed by Phase 96**
- Mid-txn abortable signals (e.g. produce failure forces abortable while open)
- Multi-broker prepared / open replication
- Session TTL / max sessions → **closed by Phase 95 (MVP)**
- Multi-lang clients / cargo-fuzz corpus CI
