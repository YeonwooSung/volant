# Phase 87 — Durable OffsetForLeaderEpoch history (MVP)

## Goals

1. Persist **per-partition leader-epoch history** (`epoch → start_offset`)
   sufficient for OffsetForLeaderEpoch end-offset responses
2. **OffsetForLeaderEpoch** (API key 23, v0–4 already advertised): return
   correct **end offsets for known prior epochs** when history exists; keep
   honest fencing / unknown-epoch behavior
3. Advance / record history when the local broker is leader:
   - Explicit epoch bumps (`set_partition_leader_epoch` / tests)
   - Multi-node failover (`on_broker_death` leader election) best-effort
4. Advertise **real** `leader_epoch` on Metadata v7+ / flexible (was always `-1`)
5. Tests + docs honesty

## Non-goals

- Full KRaft / remote log epoch state machine
- Real incremental fetch sessions
- Fetch **DivergingEpoch** tagged field on truncation (deferred)
- Kafka control batches on the data log / real 2PC
- Multi-lang clients / cargo-fuzz corpus CI
- Inventing unsupported API max versions beyond `SUPPORTED_APIS`

## Wire (unchanged)

OffsetForLeaderEpoch request/response layouts remain Phase 39/63:

```
Request (v0–4):
  replica_id: INT32                         # v3+
  topics: [{ name, partitions: [{
    partition, current_leader_epoch (v2+), leader_epoch
  }]}]

Response (v0–4):
  throttle_time_ms (v2+)
  topics: [{ name, partitions: [{
    error_code, partition, leader_epoch (v1+), end_offset
  }]}]
```

No new API keys or version maxes.

## Semantics

History is a sorted list of `(epoch, start_offset)` per partition (Kafka-style
epoch cache). End offset of epoch **E** = start offset of the next higher
epoch entry, or **HWM** if **E** is the current/latest epoch.

| Case | Result |
|------|--------|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION`, end `-1` |
| ACL deny (Topic Describe) | `TOPIC_AUTHORIZATION_FAILED` |
| `current_leader_epoch` ≠ -1 and **>** partition epoch | `UNKNOWN_LEADER_EPOCH` |
| `current_leader_epoch` ≠ -1 and **<** partition epoch | `FENCED_LEADER_EPOCH` |
| Requested `leader_epoch` **>** current (and ≠ -1) | `UNKNOWN_LEADER_EPOCH` |
| Requested `-1` (latest) or **current** epoch | error 0; epoch = current; end = **HWM** |
| Requested **prior** epoch present in history | error 0; epoch = found; end = **start of next epoch** (not HWM) |
| Requested prior epoch **gap** (no exact match) | largest stored epoch **≤** requested; end of that epoch |
| Empty history (never seeded) | seed epoch 0 @ 0 on first ensure; then same as above |
| `replica_id` | ignored |

### Epoch advance (recording)

When leader epoch advances from `old` → `new` (`new > old`):

1. Ensure history contains an entry for `old` (default start 0 if missing)
2. Append `(new, start_offset)` where `start_offset` is local **LEO** at the
   transition (best-effort; 0 if partition not local)
3. Persist under `{data_dir}/__leader_epochs/state.json`
4. Update in-memory `Partition.leader_epoch` (and assignment on failover)

Topic create seeds `(0, 0)` for each partition. Reload restores the JSON file.

## Metadata

Metadata partition `LeaderEpoch` (v7+ classic / flexible) now emits the live
partition epoch (`u32` as `i32`), not `-1`. New topics start at epoch **0**.

## Persistence layout

```
{data_dir}/__leader_epochs/state.json
{
  "partitions": {
    "orders:0": [
      { "epoch": 0, "start_offset": 0 },
      { "epoch": 1, "start_offset": 2 }
    ]
  }
}
```

Atomic write (temp + rename), same pattern as producer state / txn markers.

## Exit criteria

1. After epoch bump mid-log, OFLE for the **prior** epoch returns end offset
   **≠** current HWM (the transition LEO)
2. OFLE for current / `-1` still returns HWM
3. History **survives broker restart** (reload from `__leader_epochs`)
4. Metadata v7+ reports real leader epoch (≥ 0)
5. Prior phase39/63 fencing tests remain green
6. `cargo test` green for broker phase tests + workspace

## Honest limitations

- Soft MVP: history is a JSON file, not a KRaft / remote-log epoch state machine
- Multi-node failover records start offset from **local** LEO only (best-effort
  if controller is not a replica)
- No DivergingEpoch on Fetch truncation
- No inter-broker epoch cache replication beyond assignment `leader_epoch`
- Epoch gaps: largest ≤ requested (Kafka-compatible), not UNKNOWN for holes
- Single-node epochs only advance via explicit bump / test helper unless
  multi-node failover runs

## Test plan

`crates/volant-broker/tests/phase87_leader_epoch_history.rs`:

1. Produce N messages → bump epoch → produce more → OFLE prior epoch end = N
2. Restart broker on same data dir → OFLE prior epoch still N; current = HWM
3. Metadata reports non-`-1` epoch after bump
4. Unknown / fenced epoch paths unchanged

## Deferred (Phase 88+)

- Fetch DivergingEpoch tagged field → **closed by Phase 88**
- Real fetch sessions → **closed by Phase 88 (MVP)**
- Kafka control batches on data log / real 2PC
- Multi-lang clients / cargo-fuzz corpus CI
