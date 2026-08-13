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

### Fetch sessions (Phase 88–95 + 115 + 119)

Incremental Fetch sessions (session_id / epoch / omit-unchanged HWM+LSO cache)
are **owned by one broker** and **durable under** `{data_dir}/__fetch_sessions`
(Phase 115). Same-node restart restores live sessions within idle TTL.
**Phase 119:** cluster session_ids encode the owner; a non-owner that receives
an incremental Fetch transparent-forwards to the owner (single SoT for epoch /
omit cache). Unreachable owner ⇒ **70**. Not a replicated session table.
**Phase 126:** Fetch PreferredReadReplica (KIP-392 subset) may redirect reads to
a same-rack ISR follower when LEO ≥ HWM; not a shared session store / not full
Kafka preferred selector.

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
- **ISR rejoin + lag shrink (Phase 118 + 125):** on leader `ReplicaFetch`, members
  with `leader_leo - leo > replica_lag_max_messages` leave the ISR even if
  still membership-alive. **Phase 125** also drops members whose last
  caught-up observation (lag ≤ message max) is older than `replica_lag_max_ms`
  (default 30s; `0` disables; env `VOLANT_REPLICA_LAG_MAX_MS` overrides).
  A previously removed replica re-enters when its fetch LEO is ≥ committed HWM
  **and** lag ≤ the message threshold — time lag does not block rejoin after
  catch-up. ClusterState apply on the leader preserves still-caught-up local
  rejoin members so a controller assignment that still lists a shrunk set does
  not undo rejoin (then re-applies offset + time shrink). Produce/HWM use
  **leader-local** ISR. **Phase 142:** Metadata on the partition **leader**
  overlays local ISR (and local epoch/HWM); non-controller leaders best-effort
  report ISR to the controller (`IsrUpdate` 94/95) so controller assignment is
  SoT and ClusterState pulls refresh peers — report lag/fail still leaves
  non-leader Metadata stale until retry. Metrics:
  `volant_isr_expand_total` / `volant_isr_shrink_total` /
  `volant_isr_time_shrink_total`.
- **Cluster admin fan-out (Phase 113 + 116 + 123 + 135/137/148):**
  - **DeleteRecords:** only the partition **leader** accepts the client RPC.
    **Default (wait off):** local truncate first, then best-effort RPCs other
    replicas at the **achieved** `low_watermark` (whole-segment clamp), not the
    client-requested `before_offset`. Peer/journal majority failure does not
    fail the client. **Phase 148 wait on** (env or native trailer): journal
    majority note **first**; majority fail → client `NotEnoughReplicas` and
    **no local truncate** (provisional journal rolled back); majority ok → local
    truncate then replica/outbox fan-out. Failed replica targets are recorded in
    a **leader-local durable outbox** (`__delete_records_outbox`, Phase 116) and
    retried at-least-once when the peer is live again. **Phase 123:** on
    leadership change the new leader **reconciles** pending targets from its
    local `log_start` (current epoch). **Phase 129–131:** multi-controller
    truncate journal (`__truncate_journal`) majority note + full-snapshot push +
    heartbeat rejoin catch-up; reconcile = `max(local log_start, journal
    watermark)`. Ingress `TruncateJournalNote` fences negative/stale epochs /
    unknown TP (residual: current-epoch forge under weak auth; push 88 max-merge
    unfenced by design). Peers still clamp independently; journal max-merge SoT;
    best-effort fan-out. Still not a full Raft truncate log.
  - **BROKER config / ACL mutate:** controller is SoT; non-controllers return
    `NotController`. Successful controller mutates push generationed state to
    live peers (config knobs or full ACL snapshot). Describe / authorize use
    each node's local applied state after push.
  - **Admin catch-up (Phase 117):** generations are durable under
    `{data_dir}/__cluster_admin`. Non-controllers piggyback applied gens on
    `HeartbeatBroker`; when the controller sees lag it re-pushes opcodes 72–75
    (full effective BROKER knobs + full ACL snapshot). Brief lag until the next
    successful heartbeat + catch-up RPC is still allowed; not Raft.
- **Multi-broker 2PC (Phase 114 + 120 + 121 + 122 + 124):** Enable2Pc prepare/complete is coordinated
  over inter-broker RPC (opcodes 76–81). Init owner is the txn coordinator;
  produce still targets partition leaders after open fan-out. **Phase 120:**
  Kafka EndTxn to a non-coordinator transparent-forwards to the Init owner
  (opcodes 84/85) so clients/LBs need not pin EndTxn; only the coordinator runs
  local prepare/complete (no dual prepare). **Phase 121:** FindCoordinator maps
  group/txn keys via sticky murmur2 over the static membership ring (next-live
  on preferred death); known transactional_id returns Init owner (registry
  override). **Phase 122:** AddOffsetsToTxn / TxnOffsetCommit also forward via
  84/85 so deferred offsets buffer only on the coordinator (no dual-commit).
  **Phase 124:** Init-owner registry is durable under
  `{data_dir}/__txn_coordinator` (load on open; persist on note); peer restart
  restores forward/FC override without re-Init. Not a Kafka
  `__transaction_state` topic / full KIP-890.

See [PHASE6_SPEC.md](./PHASE6_SPEC.md) for wire protocol and configuration details.
Admin fan-out detail: [PHASE113_SPEC.md](./PHASE113_SPEC.md).
DeleteRecords outbox: [PHASE116_SPEC.md](./PHASE116_SPEC.md).
DeleteRecords outbox leadership handoff: [PHASE123_SPEC.md](./PHASE123_SPEC.md).
Admin catch-up: [PHASE117_SPEC.md](./PHASE117_SPEC.md).
ISR rejoin + lag shrink: [PHASE118_SPEC.md](./PHASE118_SPEC.md).
Multi-broker 2PC detail: [PHASE114_SPEC.md](./PHASE114_SPEC.md).
EndTxn forward: [PHASE120_SPEC.md](./PHASE120_SPEC.md).
Durable txn coordinator registry: [PHASE124_SPEC.md](./PHASE124_SPEC.md).
Sticky FindCoordinator: [PHASE121_SPEC.md](./PHASE121_SPEC.md).
AddOffsets / TxnOffsetCommit forward: [PHASE122_SPEC.md](./PHASE122_SPEC.md).
