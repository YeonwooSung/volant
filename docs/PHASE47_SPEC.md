# Phase 47 — Kafka transaction APIs classic version bumps

## Goals

1. Raise classic transaction APIs to Kafka's last non-flexible versions:
   - **AddPartitionsToTxn** 0 → **0–2** (flexible 3+)
   - **AddOffsetsToTxn** 0 → **0–2** (flexible 3+)
   - **EndTxn** 0 → **0–2** (flexible 3+)
   - **TxnOffsetCommit** 0 → **0–2** (flexible 3+)
2. Parse TxnOffsetCommit v2+ `committed_leader_epoch` (ignored; not stored)
3. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible (compact) txn APIs (v3+)
- **InitProducerId** bump — already classic max 0–1 (flexible 2+)
- Control markers / `WriteTxnMarkers` / true `READ_COMMITTED` LSO filtering
- Persistent in-flight txn recovery (crash ≡ abort)
- PRODUCER_FENCED / quota-timing semantics unique to higher versions on real Kafka
- Multi-key FindCoordinator batch / DescribeCluster / ListTransactions

## Wire summary

### AddPartitionsToTxn / AddOffsetsToTxn / EndTxn (v0–2)

Request and response bodies are **wire-identical** to v0 for classic framing.
All responses already lead with `throttle_time_ms` (Kafka has throttle on these
APIs from v0).

| Ver | Notes |
|-----|--------|
| v0 | Phase 31 baseline |
| v1–2 | Same classic layout; real Kafka adds fenced-producer / quota-timing nuances only |

### TxnOffsetCommit (v0–2)

| Ver | Additive |
|-----|----------|
| v0–1 | partition, offset, metadata (nullable) |
| v2 | `committed_leader_epoch` INT32 after offset (parsed, ignored) |

Response (all versions): `throttle_time_ms`, `[topic [partition, error_code]]`.

## Exit criteria

1. ApiVersions: keys 24/25/26/28 max **2**; InitProducerId (22) stays max **1**
2. AddPartitionsToTxn / AddOffsetsToTxn / EndTxn v2 succeed with v0 body layout
3. TxnOffsetCommit v2 with leader_epoch applies offsets only on EndTxn commit
4. Version 3 returns UnsupportedVersion
5. phase31 ApiVersions asserts updated; phase47 tests green

## Honest limitations

- Buffer-until-commit only (no control markers / READ_COMMITTED filtering)
- Leader epoch on TxnOffsetCommit is not stored (same as OffsetCommit)
- No flexible encoding; clients needing v3+ must stay on classic max
