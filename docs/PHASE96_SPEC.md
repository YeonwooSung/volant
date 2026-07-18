# Phase 96 — Broker `transaction.max.timeout.ms` clamp (MVP)

## Goals

1. Introduce a broker **maximum** transaction timeout (Kafka-ish
   `transaction.max.timeout.ms`), default **900_000 ms (15 minutes)**.
2. Config: env `VOLANT_TRANSACTION_MAX_TIMEOUT_MS` +
   `Broker::set_transaction_max_timeout_ms` / getter.
3. **Clamp** effective open-txn timeout (client or open default) and prepared
   timeout to ≤ max when max > 0.
4. InitProducerId: if client `transaction_timeout_ms` **exceeds** max (when
   max > 0), **reject** with Kafka **INVALID_TRANSACTION_TIMEOUT (50)** —
   Kafka-honest behavior (TransactionCoordinator rejects rather than clamps).
5. Lazy expiry / Describe paths already use effective timeouts — clamp applies
   in `effective_open_txn_timeout_ms` and prepared timeout load.
6. Tests (`phase96_*.rs`) + living docs honesty.

## Non-goals

- Background sweeper thread
- Multi-broker clocks
- Multi-lang / fuzz CI
- Changing default 60s open/prepared defaults (only introduce max)
- Full Kafka coordinator config surface / DynamicConfig

## Design (honest MVP)

### Default choice

| Knob | Default | Rationale |
|------|---------|-----------|
| `transaction_max_timeout_ms` | **900_000** (15 min) | Matches Apache Kafka broker default `transaction.max.timeout.ms` |
| Open default (Phase 93) | 60_000 | Unchanged |
| Prepared default (Phase 92) | 60_000 | Unchanged |

### Config surface

| Source | Behavior |
|--------|----------|
| Default | **900_000 ms** |
| Env `VOLANT_TRANSACTION_MAX_TIMEOUT_MS` | Read at `Broker::new` / `with_cluster` |
| `Broker::set_transaction_max_timeout_ms(ms)` | Runtime override (tests / ops) |
| `Broker::transaction_max_timeout_ms()` | Current max |
| **`0`** | **No max** — disable clamp + InitProducerId reject (operator opt-out / tests) |

**`0` semantics:** "no max" (not "use default"). Default remains 900_000 when
env is unset. Setting `0` intentionally disables the cap so tests and lab
clusters can request arbitrarily large client timeouts.

### Clamp rules

Let `max = transaction_max_timeout_ms`.

| Input timeout `t` | `max == 0` | `max > 0` |
|-------------------|------------|-----------|
| `t == 0` (disabled / use-default sentinel after resolve) | unchanged `0` | unchanged `0` (disabled stays disabled) |
| `0 < t ≤ max` | `t` | `t` |
| `t > max` | `t` | **`max`** |

Applied to:

1. **Effective open timeout** — after resolving client vs broker-default open
   timeout (`effective_open_txn_timeout_ms`).
2. **Effective prepared timeout** — after loading configured prepared timeout
   (`effective_prepared_txn_timeout_ms`).
3. Open expiry loop and DescribeTransactions timeout fields use the effective
   (clamped) values so lowering max mid-flight shortens the remaining clock.

### InitProducerId (Kafka-honest reject)

When client field `transaction_timeout_ms > 0` **and** `max > 0` **and**
`transaction_timeout_ms as u64 > max`:

- Return wire error **50** (`INVALID_TRANSACTION_TIMEOUT`)
- Do **not** allocate / fence / store producer state
- Response pid/epoch / OngoingTxn* = `-1`

When client field ≤ 0: Volant still treats as "use broker default" (Phase 93);
**not** rejected (Kafka would reject `≤ 0`, but Volant keeps the Phase 93
default-fallback honesty).

When `max == 0`: no reject (cap disabled).

**Choice rationale:** Kafka's `TransactionCoordinator` validates
`0 < txnTimeoutMs ≤ transaction.max.timeout.ms` and fails InitProducerId with
`INVALID_TRANSACTION_TIMEOUT`. Volant mirrors the over-max reject (code **50**)
while preserving Phase 93's `≤ 0 → broker default` for open-txn convenience.
Silent clamp of the client field was rejected as less Kafka-honest for the
Init path; clamp still applies to **effective** open/prepared clocks so
operator-lowered max and oversize stored state remain safe.

### Semantics table

| Case | Behavior |
|------|----------|
| Client timeout 60s, max 15m | Accept; effective open = 60s |
| Client timeout 20m, max 15m | InitProducerId → **50** |
| Client timeout 0, open default 60s, max 15m | Accept; effective open = 60s |
| Client timeout 5s, max 1s (after prior accept with max=0) | Effective open clamped to 1s; expire uses 1s |
| Prepared timeout 60s, max 15m | Effective prepared = 60s |
| Prepared timeout 5s, max 1s | Effective prepared = 1s; expire uses 1s |
| Prepared / open timeout **0** (disabled), max 15m | Still disabled (0) |
| max **0** | No clamp; no Init reject on oversize client timeout |
| Describe Ongoing | Reports **effective** (clamped) open timeout |
| Describe Prepare* | Reports **effective** (clamped) prepared timeout |

### Interaction with Phase 92/93/94

| Path | Change |
|------|--------|
| Open clock / lazy expire | Uses clamped effective open timeout |
| Prepared clock / lazy expire | Uses clamped effective prepared timeout |
| TRANSACTION_ABORTABLE (94) | Unchanged; still marks on timeout abort |
| KeepPrepared / fence | Unchanged after successful Init |

## Exit criteria

1. Default max 900_000 ms; env + setter; `0` = no max
2. InitProducerId rejects client timeout > max with **50**
3. Effective open + prepared timeouts clamped when max > 0
4. Expire / Describe use clamped values; below-max paths unchanged
5. `phase96_*` + prior txn phases green
6. Docs: PHASE96_SPEC + living docs / ROADMAP

## Honest limitations

- Lazy expiry only (no background sweeper)
- Single-node clock; no multi-broker coordinated max
- Volant still accepts client timeout ≤ 0 as "broker default" (not full Kafka
  `txnTimeoutMs > 0` validation)
- No DynamicConfig / Admin DescribeConfigs for the knobs
- Stored client timeout is not rewritten on max change (only effective clamp)

## Phase 97 ideas

- Background txn + session sweeper (periodic, not only lazy)
- Metrics: open/prepared expired counts; Init reject counters
- Admin/DescribeConfigs surface for timeout knobs
- Mid-txn abortable signals beyond timeout-only (Phase 94 stretch)
- Multi-broker 2PC / session affinity
- Multi-lang clients / cargo-fuzz corpus CI
