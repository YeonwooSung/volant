# Phase 5 — Broker Batch Produce Coalescing Plan (Iteration 1)

## Goals

Make `Broker::produce(MessageBatch)` an efficient coalesced path:

1. Hold the broker topics write lock **once** for the entire batch
2. Append all messages without intermediate `fsync`
3. Apply `flush_every_n` **once** after the batch (not per message)
4. Document coalescing behavior in module / method docs
5. Optional metric: `messages_coalesced` (`AtomicU64` on `Broker`)
6. Test: produce N messages → contiguous offsets, HWM advanced by N

## Current state

`Broker::produce` already:

- Acquires `topics` write lock once
- Loops `part.log.append(msg)` per message

`PartitionLog::append` checks `flush_every_n` **after every message**, so a batch of
size N can trigger multiple mid-batch fsyncs. That defeats batch efficiency.

## Design

### Storage (minimal touch)

Add `PartitionLog::append_batch(messages) -> Result<Vec<Record>>`:

```
for each message:
  append_one (encode + write; no flush check)
appends_since_flush += N
if flush_every_n > 0 && appends_since_flush >= flush_every_n:
  flush() once
```

Refactor single `append` to share `append_one` + per-message flush policy so
behavior for single-message produces is unchanged.

### Broker

Improve `produce` (no separate `produce_coalesced` API needed):

1. Write-lock topics once
2. Resolve topic + partition
3. Call `log.append_batch(batch.messages)`
4. If `N > 1`, `messages_coalesced.fetch_add(N)`
5. Return records with contiguous offsets

Expose:

- `pub fn messages_coalesced(&self) -> u64` — total messages that went through
  multi-message coalesce path

### Docs

- Crate / `broker` module note on batch coalescing
- `produce` rustdoc: single lock, no mid-batch flush, flush policy once

### Tests (`crates/volant-broker/tests/`)

| Test | Assert |
|------|--------|
| `batch_produce_contiguous_offsets_and_hwm` | N msgs → offsets `base..base+N`, HWM `+= N`, fetch round-trip |
| `batch_produce_coalesce_metric` | N≥2 increments `messages_coalesced` by N; single-msg does not |

Optional unit coverage of `append_batch` flush-once via storage path if needed
(covered indirectly when `flush_every_n > 0` and durable reopen still works).

## Non-goals

- Write-behind queue (stretch; document as not implemented)
- Changing network encode path (storage-io / protocol agents)
- Partition-level fine-grained locks (topics `RwLock` is the exclusive gate)

## Files

| Path | Change |
|------|--------|
| `crates/volant-storage/src/log.rs` | `append_one` + `append_batch` |
| `crates/volant-broker/src/broker.rs` | coalesce docs, metric, use `append_batch` |
| `crates/volant-broker/src/lib.rs` | module doc note if useful |
| `crates/volant-broker/tests/inprocess_produce.rs` | batch + metric tests |
| `docs/phase5/broker-batch-plan.md` | this plan |
| `docs/phase5/broker-batch-review.md` | review after code |

## Iteration policy

Max 3 PLAN→CODE→REVIEW→TEST→FIX loops. Document each in the review file.
