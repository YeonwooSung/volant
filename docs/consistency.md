# Volant consistency model (Phase 6)

This document defines what “committed” means for multi-replica partitions.

## Roles

| Role | Responsibility |
|------|----------------|
| **Leader** | Sole acceptor of client produces for a partition; advances high watermark |
| **Follower** | Replicates the leader log; never serves client produce |
| **Controller** | Lowest live broker id; assigns replicas, elects leaders, tracks liveness |
| **ISR** | In-sync replicas: leader + followers that are live and within lag threshold |

## Offsets

| Term | Definition |
|------|------------|
| **LEO** (log end offset) | Next offset the local replica will write |
| **HWM** (high watermark) | `min(LEO of every broker in the ISR)` |
| **Committed offset `o`** | `o < HWM` |

Client **Fetch** returns only records with `offset < HWM`. Uncommitted data is never visible to consumers.

## Produce acknowledgements

| `acks` | Wire value | When the produce response is sent |
|--------|------------|-----------------------------------|
| 0 | `0` | After local leader append (best-effort; response still sent) |
| 1 | `1` (default) | After local leader append; **not** waiting for followers |
| all | `255` | After HWM covers the produced batch (`HWM >= base_offset + count`) |

### `min_insync_replicas`

When `acks=all`, if `|ISR| < min_insync_replicas`, the leader rejects the produce with
`NotEnoughReplicas` and does **not** append.

## Failure guarantees

### What survives a leader crash?

| Produce mode | Survives former-leader crash? |
|--------------|-------------------------------|
| `acks=all` (response received) | **Yes** — every ISR member has the data; new leader is elected from ISR |
| `acks=1` (response received) | **Maybe not** — data may exist only on the dead leader |
| In-flight produce (no response) | Client should retry; with Phase 10 `enable_idempotence` duplicates are de-duped per PID/seq. Phase 11 persists that state under `{data_dir}/__producer_state/` so de-dupe survives broker restart |

### What Volant does **not** guarantee (Phase 6+)

- Exactly-once produce/consume end-to-end (Kafka control-batch wire on EndTxn + crash promote shipped as Phase **89**/**98** MVP; soft markers remain isolation SoT; not full EOS)
- Durable in-flight txn recovery that resumes open transactions (open txn crash ≡ **abort** via `__txn_markers`; write-through ranges are not rolled forward)
- Linearizability of metadata during controller failover (brief windows of stale Metadata)
- Durability if `min_insync_replicas=1` and that sole replica dies after ack
- Full KRaft leader-epoch state machine (Phase **87** durable OFLE history is a soft JSON MVP)

### Transactions (Phase 86 write-through + Phase 90 prepared MVP)

| Mode | Behavior |
|------|----------|
| Open txn produce | Appends to the log immediately; HWM advances; LSO held at first unstable offset |
| EndTxn commit (non-2PC) | Ranges become stable; LSO catches HWM when no other open/prepared txn |
| EndTxn abort (non-2PC) | Soft abort markers hide ranges under `READ_COMMITTED` / native committed-only fetch |
| EndTxn #1 with Enable2Pc | Moves open → Prepared (PrepareCommit/Abort); LSO still held; no markers yet |
| EndTxn #2 matching decision | Finalize commit/abort (soft + control markers) |
| Prepared timeout (Phase 92) | Lazy auto-abort after configured timeout (default 60s); soft + ABORT control markers |
| Open timeout (Phase 93) | Lazy auto-abort of open write-through txns after client/broker timeout; soft + ABORT control markers |
| Max timeout clamp (Phase 96) | Broker max (default 15m); Init over-max → **50**; effective open/prepared timeouts clamped |
| Background sweeper (Phase 97/101) | Periodic open/prepared + idle session expiry (default 1s); `0` pauses; always-spawn so 0→>0 live; lazy paths remain; expiry counters |
| Soft-marker GC (Phase 104) | DeleteRecords / retention / load drop markers with `end_offset <= log_start`; partial overlaps retained |
| Crash with open writes | Open ranges promoted to aborted on reload + ABORT control batches (Phase 98) |
| Crash with prepared | Prepared reloaded from `__txn_prepared` (survives; complete, re-init abort, or timeout) |

### Leader epochs (Phase 87)

Per-partition `(epoch, start_offset)` history under `{data_dir}/__leader_epochs`.
OffsetForLeaderEpoch returns transition end offsets for prior epochs; Metadata
advertises the live leader epoch.

## Single-node mode

Without `--cluster-config`, the broker runs as a single node:

- RF = 1, ISR = `[self]`, HWM = LEO always
- `acks=all` behaves like `acks=1` (local append is sufficient)
- No inter-broker traffic

## Operational notes

- Prefer `acks=all` and `min_insync_replicas >= 2` for multi-node deployments
- Rolling restart: restart followers first; restart the controller carefully (next-lowest id takes over)
- After failover, clients should refresh Metadata on `NotLeaderForPartition`

See [PHASE6_SPEC.md](./PHASE6_SPEC.md) for wire protocol and configuration details.
