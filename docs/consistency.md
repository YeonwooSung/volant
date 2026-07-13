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
| In-flight produce (no response) | Client should retry; with Phase 10 `enable_idempotence` duplicates are de-duped per PID/seq (in-memory; lost on broker restart) |

### What Volant does **not** guarantee (Phase 6)

- Exactly-once produce/consume
- Cross-partition atomicity
- Linearizability of metadata during controller failover (brief windows of stale Metadata)
- Durability if `min_insync_replicas=1` and that sole replica dies after ack

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
