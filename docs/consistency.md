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
| Soft-marker GC (Phase 104/111) | DeleteRecords / retention / load drop markers with `end_offset <= log_start`; straddle clips `first_offset = log_start` |
| Crash with open writes | Open ranges promoted to aborted on reload + ABORT control batches (Phase 98) |
| Crash with prepared | Prepared reloaded from `__txn_prepared` (survives; complete, re-init abort, or timeout) |
| Multi-broker Enable2Pc (Phase 114) | Coordinator fans out open/prepare/complete to live peers; each leader holds local prepared ranges + LSO; controller stores durable cluster prepared index (`__txn_prepared/cluster.json`); prepare is **strict** for live peers (rollback local prepare on fan-out failure); fence complete with `commit=false` force-aborts peer PrepareCommit |

### Leader epochs (Phase 87)

Per-partition `(epoch, start_offset)` history under `{data_dir}/__leader_epochs`.
OffsetForLeaderEpoch returns transition end offsets for prior epochs; Metadata
advertises the live leader epoch.

### Fetch sessions (Phase 88–95 + 115)

Incremental Fetch sessions (session_id / epoch / omit-unchanged HWM+LSO cache)
are **per-broker** and **durable under** `{data_dir}/__fetch_sessions` (Phase 115).
Same-node restart restores live sessions within idle TTL. Sessions are **not**
replicated or handed off across brokers — a client that lands on a different
broker gets **FETCH_SESSION_ID_NOT_FOUND (70)** and must full-fetch recreate.
Pin Fetch TCP (or LB stickiness) to the session-owner broker.

## Single-node mode

Without `--cluster-config`, the broker runs as a single node:

- RF = 1, ISR = `[self]`, HWM = LEO always
- `acks=all` behaves like `acks=1` (local append is sufficient)
- No inter-broker traffic

## Operational notes

- Prefer `acks=all` and `min_insync_replicas >= 2` for multi-node deployments
- Rolling restart: restart followers first; restart the controller carefully (next-lowest id takes over)
- After failover, clients should refresh Metadata on `NotLeaderForPartition`
- **Follower death (Phase 108/110):** every node that observes a broker death
  removes that id from local partition ISR and recomputes HWM when it leads.
  The controller also shrinks the durable assignment and bumps generation
  (including pure ISR shrink with no leader change) so peers learn via
  ClusterState. Non-controllers additionally detect deaths from controller
  `HeartbeatBroker.alive_brokers` gaps and local membership expire (Phase 110),
  calling the same death path without waiting for ClusterState.
  `acks=all` then waits only on the **remaining live** ISR — it no longer
  REQUEST_TIMED_OUT solely because a dead follower still held a stale LEO in
  the ISR set. If `|ISR|` falls below `min_insync_replicas`, produce is still
  rejected with `NotEnoughReplicas`.
- **Cluster admin fan-out (Phase 113 + 116):**
  - **DeleteRecords:** only the partition **leader** accepts the client RPC;
    after local truncate it best-effort RPCs other replicas. Peer failure does
    not fail the client. Failed targets are recorded in a **leader-local durable
    outbox** (`__delete_records_outbox`, Phase 116) and retried at-least-once
    when the peer is live again — not a consensus truncate log; leadership
    change does not transfer the old leader’s pending set.
  - **BROKER config / ACL mutate:** controller is SoT; non-controllers return
    `NotController`. Successful controller mutates push generationed state to
    live peers (config knobs or full ACL snapshot). Describe / authorize use
    each node's local applied state after push.
- **Multi-broker 2PC (Phase 114):** Enable2Pc prepare/complete is coordinated
  over inter-broker RPC (opcodes 76–81). Pin Init/Begin/EndTxn to the broker
  that allocated the producer; produce still targets partition leaders after
  open fan-out. Not a Kafka `__transaction_state` topic / full KIP-890.

See [PHASE6_SPEC.md](./PHASE6_SPEC.md) for wire protocol and configuration details.
Admin fan-out detail: [PHASE113_SPEC.md](./PHASE113_SPEC.md).
DeleteRecords outbox: [PHASE116_SPEC.md](./PHASE116_SPEC.md).
Multi-broker 2PC detail: [PHASE114_SPEC.md](./PHASE114_SPEC.md).
