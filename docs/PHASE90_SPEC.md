# Phase 90 — Real 2PC / prepared transactions (MVP)

## Goals

1. Honor InitProducerId v6 **Enable2Pc** and **KeepPreparedTxn** with real
   prepared-transaction state (not parse-and-ignore).
2. Return non-default **OngoingTxnProducerId / OngoingTxnProducerEpoch** when a
   prepared txn exists for the transactional id.
3. EndTxn / abort / fence interactions with prepared state are correct and tested.
4. Durable prepared state under `{data_dir}/__txn_prepared/state.json` for crash
   recovery (prepared **survives** restart; open non-prepared still crash≡abort).
5. Tests (`phase90_*.rs`) + living docs honesty (no false KIP-890/939 parity).

## Non-goals

- Full multi-broker 2PC coordinator / transaction log replication
- Kafka TRANSACTION_ABORTABLE emission
- Multi-lang clients, cargo-fuzz CI, omit-unchanged fetch cache
- Automatic prepare for non-2PC producers (Enable2Pc=false keeps one-shot EndTxn)
- Completing prepared txns via any API other than EndTxn / InitProducerId abort

## Design (honest MVP)

### Two-phase EndTxn for Enable2Pc producers

| Step | API | Effect |
|------|-----|--------|
| 1 | InitProducerId v6 `Enable2Pc=true` | Mark producer `enable_2pc`; allocate/fence as today |
| 2 | AddPartitions / Produce / TxnOffsetCommit | Open write-through txn (unchanged) |
| 3 | **First** EndTxn(commit\|abort) | Move open → **Prepared** (PrepareCommit / PrepareAbort); **no** soft markers / control batches / offset apply yet |
| 4a | **Second** EndTxn(same decision) | Finalize: commit or abort (soft + control markers, offsets) |
| 4b | InitProducerId `KeepPreparedTxn=false` | Force-abort prepared, then normal fence |
| 4c | InitProducerId `KeepPreparedTxn=true` | Keep prepared; return **OngoingTxn\*** = prepared pid/epoch; **no** epoch bump |

Enable2Pc=**false** (default / classic InitProducerId): EndTxn remains **one-shot**
finalize (Phases 18/86/89). Only 2PC-enabled producers use prepare-then-complete.

### Wire (unchanged shapes)

InitProducerId **v6** request already carries Enable2Pc + KeepPreparedTxn.
Response already has OngoingTxnProducerId / OngoingTxnProducerEpoch.

| Condition | OngoingTxnProducerId | OngoingTxnProducerEpoch |
|-----------|---------------------:|------------------------:|
| No prepared for transactional id | -1 | -1 |
| Prepared exists + KeepPreparedTxn | prepared pid | prepared epoch |
| Prepared aborted on re-init | -1 | -1 |

v0–5 InitProducerId responses still omit OngoingTxn fields.

### On-disk prepared snapshot

`{data_dir}/__txn_prepared/state.json`:

```json
{
  "prepared": [
    {
      "transactional_id": "app-1",
      "producer_id": 1,
      "producer_epoch": 0,
      "commit": true,
      "written": [
        {
          "topic": "events",
          "partition": 0,
          "first_offset": 0,
          "end_offset": 1,
          "base_sequence": 0,
          "count": 1
        }
      ],
      "pending": [
        {
          "topic": "events",
          "partition": 0,
          "base_sequence": 0,
          "count": 1,
          "base_offset": 0
        }
      ],
      "deferred_offsets": []
    }
  ]
}
```

`enable_2pc` is also persisted on the producer under `__producer_state` so a
restarted 2PC producer still prepares on EndTxn.

### Isolation while prepared

Prepared write ranges still hold **LSO** and count as unstable (same as open).
READ_COMMITTED hides them until CompleteCommit; CompleteAbort soft-marks them.
Control batches are written only on **finalize** (second EndTxn), not on prepare.

### Describe / ListTransactions

| State string | Meaning |
|--------------|---------|
| `Empty` | Known transactional id, no open/prepared |
| `Ongoing` | Open write-through txn |
| `PrepareCommit` | Prepared with commit decision |
| `PrepareAbort` | Prepared with abort decision |

### Semantics table

| Case | Behavior |
|------|----------|
| EndTxn, `enable_2pc=false` | One-shot commit/abort (unchanged) |
| EndTxn #1, `enable_2pc=true`, open | → Prepared; success; data still unstable |
| EndTxn #2, same decision | Finalize; control markers; offsets (commit) |
| EndTxn #2, **mismatched** decision | `InvalidTxnState` |
| EndTxn while prepared, wrong epoch | `InvalidProducerEpoch` |
| Ensure open / produce while prepared | `InvalidTxnState` |
| Init v6 KeepPreparedTxn=true + prepared | OngoingTxn* set; same pid/epoch; no fence |
| Init v6 KeepPreparedTxn=false + prepared | Abort prepared + fence (epoch bump) |
| Init re-fence with open (not prepared) | Abort open (unchanged); prepared untouched only if KeepPrepared |
| Crash with open ranges | Still crash≡abort (soft markers) |
| Crash with prepared | **Reload prepared**; LSO held; must EndTxn complete or re-init abort |
| Fence open while prepared exists | Open abort as today; prepared only if KeepPrepared=false |

## Exit criteria

1. Enable2Pc EndTxn prepares; second EndTxn finalizes; data isolation correct
2. InitProducerId v6 returns non-default OngoingTxn* when prepared + KeepPreparedTxn
3. KeepPreparedTxn=false aborts prepared and OngoingTxn* = -1
4. Prepared survives broker restart from `__txn_prepared`
5. Non-2PC EndTxn path unchanged (one-shot)
6. `phase90_*` + prior txn phases green
7. Docs: honest gaps (no multi-broker 2PC, no TRANSACTION_ABORTABLE, …)

## Honest limitations

- **Not** full KIP-890/939 parity (no multi-broker txn log, no TRANSACTION_ABORTABLE)
- Prepare is Volant MVP: first EndTxn prepares only when `enable_2pc` was set on
  InitProducerId for that producer (persisted flag)
- No separate PrepareTxn API; completion only via matching second EndTxn
- KeepPreparedTxn does not preserve ordinary open (non-prepared) txns
- Coordinator epoch always 0; single-node prepared store only
- No timeout-based prepared expiry
- Resume pid/epoch fields on InitProducerId v3–6 still ignored for allocation
  (KeepPreparedTxn path reuses existing identity without consulting resume fields)

## Phase 91 ideas

- Prepared timeout / auto-abort
- TRANSACTION_ABORTABLE where Kafka emits it
- Multi-broker prepared replication
- Omit-unchanged fetch session cache
- cargo-fuzz corpus CI / multi-lang clients
