# Phase 5 — Broker Batch Produce Coalescing Review

## Iteration 1

### Plan adherence

| Deliverable | Status |
|-------------|--------|
| `produce` holds topics/partition lock once for entire batch | ✅ already true; documented; still single `topics.write()` |
| Append all via batch path; flush policy once | ✅ `PartitionLog::append_batch` |
| Module docs for coalescing | ✅ `broker.rs` module + crate `lib.rs` + `produce` rustdoc |
| Test: MessageBatch of N → contiguous offsets, HWM += N | ✅ `batch_produce_contiguous_offsets_and_hwm` |
| Metric `messages_coalesced` | ✅ `AtomicU64` + `Broker::messages_coalesced()` |
| Minimal storage touch for flush-after-batch | ✅ `append_one` / `append_batch` only |

### Code review notes

1. **Lock scope** — Broker still uses the global topics `RwLock` write guard as the
   exclusive gate for partition logs (no per-partition mutex). Holding it for the
   full batch matches the spec intent of one critical section per produce.

2. **Flush semantics** — Single `append` still flushes per message when
   `flush_every_n` is hit. Batch path counts each successful append but evaluates
   flush **once** after the loop. Partial failure mid-batch still advances the
   counter for written messages without mid-batch fsync.

3. **Metric** — Increments by `N` only when `N > 1` and only after successful
   `append_batch`. Single-message / empty batches do not inflate the counter.

4. **API surface** — Improved `produce` in place (no separate `produce_coalesced`);
   matches PHASE5_SPEC “or improve produce”.

5. **Non-goals** — Write-behind queue not implemented (stretch). Fine-grained
   partition locks not introduced.

### Tests

```
cargo test -p volant-broker  → 21 passed (13 unit + 8 integration)
cargo test -p volant-storage → regression after append_batch
```

New integration tests in `crates/volant-broker/tests/inprocess_produce.rs`:

- `batch_produce_contiguous_offsets_and_hwm`
- `batch_produce_coalesce_metric`

### Fixes in iteration 1

- Initial `append_batch` deferred `appends_since_flush` until end of loop; revised
  to increment per successful message so a mid-batch error still accounts for
  durable writes, while flush still runs at most once after the batch.

### Verdict

**PASS** — no further iteration required.

## Iteration count

**1** (PLAN → CODE → REVIEW → TEST all green; one in-loop flush-counter fix before review close).

## Files touched

| File | Role |
|------|------|
| `docs/phase5/broker-batch-plan.md` | Plan |
| `docs/phase5/broker-batch-review.md` | This review |
| `crates/volant-storage/src/log.rs` | `append_one`, `append_batch` |
| `crates/volant-broker/src/broker.rs` | Coalesced produce, metric, docs |
| `crates/volant-broker/src/lib.rs` | Crate-level produce note |
| `crates/volant-broker/tests/inprocess_produce.rs` | Batch + metric tests |
