# Phase 77 — InitProducerId v6 (OngoingTxn / 2PC wire)

## Goals

1. Raise **InitProducerId** max from **0–5** to **0–6**
2. Parse v6 request fields **Enable2Pc** and **KeepPreparedTxn** (ignored)
3. Emit v6 response **OngoingTxnProducerId** / **OngoingTxnProducerEpoch**
   (always **-1** / **-1** — no prepared/2PC transactions)
4. v0–5 paths unchanged (no OngoingTxn fields on response)
5. v7 → UnsupportedVersion with response header v1
6. Tests + docs honesty

## Non-goals

- Real two-phase commit / prepared transactions
- Returning non-default OngoingTxn* for open buffer-until-commit txns
  (honest: only prepared/2PC state would populate these; Volant has none)
- Emitting TRANSACTION_ABORTABLE on InitProducerId
- Raising AddPartitionsToTxn / EndTxn / AddOffsetsToTxn beyond Phase 75
- Control markers / READ_COMMITTED log filtering
- Real resume of ProducerId/Epoch on v3–6 (still always re-allocate)

## Wire summary

### Request (flexible v2+)

| Version | Fields after TransactionalId + Timeout |
|--------:|----------------------------------------|
| v2 | (none) + tags |
| v3–5 | ProducerId, ProducerEpoch + tags |
| **v6** | ProducerId, ProducerEpoch, **Enable2Pc (bool)**, **KeepPreparedTxn (bool)** + tags |

```
TransactionalId (compact nullable),
TransactionTimeoutMs (int32),
ProducerId (int64),           # v3+
ProducerEpoch (int16),        # v3+
Enable2Pc (bool),             # v6+  — parsed, ignored
KeepPreparedTxn (bool),       # v6+  — parsed, ignored
TAG_BUFFER
```

### Response (flexible v2+)

| Version | Fields after error + pid + epoch |
|--------:|----------------------------------|
| v2–5 | tags |
| **v6** | **OngoingTxnProducerId (int64)**, **OngoingTxnProducerEpoch (int16)**, tags |

Volant always writes:

```
OngoingTxnProducerId   = -1
OngoingTxnProducerEpoch = -1
```

Allocation path unchanged: `broker.init_producer_id_with_txn` (same as v0–5).

### Flexible header

v2+ uses request header TAG_BUFFER and **response header v1** (already true for
InitProducerId ≥ 2).

## Semantics (honest)

| Client flag | Behavior |
|-------------|----------|
| Enable2Pc=false | Normal init (same as v5) |
| Enable2Pc=true | Accepted; still no prepared/2PC state (not rejected) |
| KeepPreparedTxn=* | Ignored; OngoingTxn* always -1 |

Open in-memory buffer-until-commit transactions are **not** reported as
OngoingTxn* (Kafka uses those fields for prepared/2PC state, not ordinary
open txns). Clients that inspect OngoingTxn* see "no prepared txn".

## Exit criteria

1. ApiVersions: InitProducerId **0–6**
2. InitProducerId **v6** with Enable2Pc=0/1 succeeds; response has OngoingTxn -1/-1
3. InitProducerId **v5** response still has no OngoingTxn fields (wire length)
4. InitProducerId **v7** → header v1 + UnsupportedVersion (35)
5. phase77 + phase75/62/47 green after max-version updates
6. ROADMAP / README / ops honesty

## Honest limitations (at ship; partially closed later)

- At Phase 77 ship: no real 2PC / prepared transactions; OngoingTxn* always -1;
  Enable2Pc / KeepPreparedTxn ignored
- **Superseded by Phase 90:** prepared state + non-default OngoingTxn* when
  prepared; Enable2Pc/KeepPreparedTxn honored (see [PHASE90_SPEC.md](./PHASE90_SPEC.md))
- OngoingTxn* still does **not** surface ordinary open (non-prepared) txns
- Resume pid/epoch still ignored (always re-allocate; KeepPrepared reuses identity)
- AddPartitions / EndTxn / AddOffsets maxes unchanged by this phase
- No control-marker READ_COMMITTED (later Phase 86/89)
