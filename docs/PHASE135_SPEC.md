# Phase 135 — DeleteRecords majority wait (optional)

**Status:** ✅ Shipped  
**Theme:** Optional client-visible wait on **truncate-journal majority** after a
successful local DeleteRecords, so operators can choose best-effort latency
(default) or majority honesty (opt-in).

## Goals

1. **Knob (default off):** `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` — `0`/`false`/
   unset = current best-effort (client success does not depend on journal
   majority). `1`/`true` = client path waits for Phase 130 journal majority
   and surfaces failure when majority is not reached.
2. **Native protocol:** when wait is on, after local truncate +
   `fanout_delete_records`, if journal majority failed →
   `DeleteRecords.error_code = NotEnoughReplicas` (**15**). Still return
   achieved `low_watermark` (local truncate already applied).
3. **Kafka DeleteRecords:** when wait is on, **await** fan-out (do not
   fire-and-forget) before finishing the response; map majority fail to
   Kafka `NOT_ENOUGH_REPLICAS` (19) or existing mapped equivalent used for
   not-enough-replicas — prefer **19** if already in `KafkaErrorCode`, else
   document mapping to **15**/closest honest code. Prefer reusing
   `NotEnoughReplicas` / Kafka **19** if present.
4. **Return value plumbing:** `fanout_truncate_journal_note` returns whether
   majority was reached; `fanout_delete_records` returns a small result
   (`majority_ok: bool`). Single-node / no cluster → majority_ok = true.
5. Metrics (cheap):
   - `volant_delete_records_majority_wait_success_total`
   - `volant_delete_records_majority_wait_fail_total`
   (only incremented when wait mode is on)
6. Tests + living docs 0–135.

## Non-goals

| Deferred | Why |
|----------|-----|
| Wait for every replica **log** truncate (ReplicaDeleteRecords) | Journal majority is the SoT; outbox still best-effort |
| Request-level wait flag on wire (native trailer) | Env/broker knob MVP; wire flag later |
| Rollback local truncate on majority fail | Log already advanced; honest residual |
| Full Raft linearizable delete | Larger |

## Design

```text
  client DeleteRecords
       │
       ▼
  local truncate → achieved low
       │
       ▼
  fanout_delete_records (journal majority + push + ReplicaDeleteRecords)
       │
       ├─ wait_majority OFF (default): always error_code=0 if local ok
       │
       └─ wait_majority ON:
              majority ok  → error_code=0
              majority fail → error_code=NotEnoughReplicas (15)
              (low_watermark still achieved local low)
```

Config:
- Env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` at broker construct (and optional
  runtime setter for tests).
- Default **false**.

## Honest limitations

- Local log may already be truncated when majority fails (no undo).
- Majority uses **configured N** (Phase 130), not live-only.
- Replica log truncate + outbox remain best-effort even in wait mode.
- Kafka multi-partition DeleteRecords: per-partition wait; fail any partition
  that misses majority when wait is on (or document batch policy).

## Exit criteria

1. Default off: existing phase113/116/129/130/134 delete tests green; client still succeeds without majority  
2. Wait on + 3/3 live → majority success, client error 0  
3. Wait on + only 1 of 3 can ack note → client NotEnoughReplicas, metrics fail++  
4. Docs + ops knobs honest  

## Tests

- `crates/volant-broker/tests/phase135_delete_records_majority_wait.rs`

## Implementation notes (shipped)

- Knob: `Broker::delete_records_wait_majority` (`AtomicBool`), default from
  `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` (`1`/`true`/`yes` → on; else off).
  Runtime: `set_delete_records_wait_majority` for tests.
- `fanout_truncate_journal_note` → `bool` (majority / no-cluster).
- `fanout_delete_records` → `DeleteRecordsFanoutResult { majority_ok }`; journal
  note runs first under budget so replica timeouts do not flip majority_ok;
  note skipped (not leader) → `majority_ok = true`.
- Native `DeleteRecords`: when wait on + local ok + `!majority_ok` →
  `ErrorCode::NotEnoughReplicas` (**15**); `low_watermark` still local achieved.
- Kafka DeleteRecords: wait on → await fan-out inside encode; fail → Kafka
  `NOT_ENOUGH_REPLICAS` (**19**). Wait off → fire-and-forget spawn (unchanged).
- Metrics (wait mode only):
  `volant_delete_records_majority_wait_success_total` /
  `volant_delete_records_majority_wait_fail_total`.
