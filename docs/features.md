# Native features (post-core)

Summary of Volant-native capabilities shipped after the core platform
(Phases **7–22** and related). Core formats: [PHASE1–6](./INDEX.md). Kafka
shim: [KAFKA_COMPAT.md](./KAFKA_COMPAT.md).

## Ops & packaging (7–9)

| Feature | Behavior |
|---------|----------|
| Metrics | Prometheus `GET /metrics` (`--metrics-addr`); optional Bearer token |
| Logs | `text` or `json` (`--log-format`) |
| Shared-token auth | `VOLANT_AUTH_TOKEN` on native protocol |
| Server TLS | Feature `tls`; cert/key; TLS-only listen |
| Client TLS | Feature `tls` on `volant-client` |
| Inter-broker TLS | On by default when server TLS enabled (Phase 9) |
| Leader redirect | Client refreshes Metadata + reconnects on `NotLeaderForPartition` |
| Deploy | Docker, compose, systemd, Helm (single or multi-node) |
| Fuzz scaffold | `fuzz/` targets for frame/request decode |

## Reliability (10–11)

| Feature | Behavior |
|---------|----------|
| Idempotent produce | PID / epoch / sequence de-dupe |
| Produce retries | Client-side with redirect awareness |
| Durable producer state | `{data_dir}/__producer_state/state.json` |
| Consumer lag | Metrics + `volant group lag` |
| Sticky assignor | Default partition assignor |

## Groups & topics (12–17)

| Feature | Behavior |
|---------|----------|
| Group admin | list / describe / delete-offsets; static membership |
| Topic configs | `retention.ms` / `retention.bytes` / `segment.bytes` |
| Topic catalog | Survives single-node restart |
| DeleteRecords | Truncate sealed segments before offset; GC aborted soft markers below new log start (Phase 104) |
| CreatePartitions | Grow partition count (cannot shrink) |
| ListOffsets | Earliest / latest (+ Kafka specials on shim) |
| Compaction | `cleanup.policy=compact` on sealed segments |
| Cooperative rebalance | JoinGroup `revoked` list; sticky-retained positions |

## Transactions (18)

| Feature | Behavior |
|---------|----------|
| transactional_id fencing | Yes |
| Write-through (Phase 86) | Txn produces append immediately; LSO holds until EndTxn |
| Abort | Soft markers hide ranges (native + READ_COMMITTED); data stays on log for READ_UNCOMMITTED |
| Deferred offsets | Txn offset commits apply on commit only |
| Crash | Open write-through ranges ≡ abort (persisted `__txn_markers`) + ABORT control batches (Phase 98) |
| READ_COMMITTED | MVP: LSO filtering + aborted list (soft markers SoT) |
| Soft-marker GC (Phase 104) | DeleteRecords / retention / load drop markers with `end_offset <= log_start`; persist `__txn_markers` |
| Control batches (Phase 89/98/105) | EndTxn COMMIT/ABORT + crash-promote ABORT magic-2 control RecordBatches on log; empty AddPartitions membership included (control-only) |
| Prepared 2PC MVP (Phase 90) | Enable2Pc prepare→complete EndTxn; KeepPreparedTxn + OngoingTxn*; `__txn_prepared` |
| Prepared timeout (Phase 92) | Lazy auto-abort after timeout (default 60s; `VOLANT_PREPARED_TXN_TIMEOUT_MS`; `0` disables) |
| Open txn timeout (Phase 93) | Honor InitProducerId `transaction_timeout_ms` (or broker default `VOLANT_OPEN_TXN_TIMEOUT_MS`, 60s; effective `0` disables); lazy auto-abort |
| Transaction max timeout (Phase 96) | Broker max default **15m** (`VOLANT_TRANSACTION_MAX_TIMEOUT_MS`; `0` = no max); Init over-max → **50**; effective open/prepared clamped |
| Background sweeper (Phase 97/101/106) | Periodic open/prepared + idle session expiry (default 1s / `VOLANT_SWEEP_INTERVAL_MS`); `0` pauses bg; always-spawn so 0→>0 without restart; graceful shutdown/join on stop; lazy paths remain |
| BROKER config (Phase 99–103) | Describe/Alter txn/session/sweep knobs; **sparse** durable under `__broker_config/state.json` (only altered keys; DELETE unfreezes env); resource name must be empty or local `node_id` decimal (Phase 103) |

## Security (19–22)

| Feature | Behavior |
|---------|----------|
| mTLS identity | Client cert CN/SAN as principal |
| Principal ACLs | Topic / group / cluster (+ Kafka User resource store-only); allow/deny; durable file |
| Super-users | Bypass ACL checks |
| SCRAM-SHA-256 | Durable users; **native** + Kafka SASL |
| SCRAM-SHA-512 | Dual hashes per user; **Kafka SASL only** |

## Stream processing (Phase 4+)

In-process `volant-stream`: map, filter, flat_map, reduce, windows, foreach.
**In-memory state only**; at-least-once. No durable state store, no distributed
workers.

## Leader epochs (Phase 87)

| Feature | Behavior |
|---------|----------|
| Durable history | `{data_dir}/__leader_epochs/state.json` — `(epoch, start_offset)` per partition |
| OffsetForLeaderEpoch | Prior epochs → transition end offset; current / `-1` → HWM |
| Metadata | Live `leader_epoch` (not always `-1`) |
| Advance | Explicit bump / multi-node failover best-effort |

## Fetch DivergingEpoch + sessions (Phase 88 + 91 + 95)

| Feature | Behavior |
|---------|----------|
| DivergingEpoch | Fetch v12+ tag 0 when `last_fetched_epoch` + `fetch_offset` past epoch end |
| Partition error | `OFFSET_OUT_OF_RANGE` with empty records; HWM/LSO still filled |
| Sessions | Process-local create / merge / forgotten / FINAL close |
| Incremental | Empty topics re-fetches session set |
| Omit-unchanged (91) | Empty-topics incremental omits partition when HWM+LSO unchanged and records empty |
| Idle TTL (95) | Default 60s (`VOLANT_FETCH_SESSION_IDLE_MS`; `0` disables); lazy on create/incremental |
| Max sessions (95) | Default 1000 (`VOLANT_FETCH_SESSION_MAX`; `0` unlimited); LRU-evict at cap |
| Errors | 70 session id not found (incl. after TTL/LRU); 71 invalid session epoch |

## Open limitations (native)

- Multi-language clients deferred  
- No Raft metadata / dynamic membership  
- Crash≡abort control batches yes (Phase 98); empty AddPartitions control yes (Phase 105)  
- Prepared 2PC is single-node MVP (no multi-broker txn log); prepared timeout yes (Phase 92); open-txn timeout yes (Phase 93); TRANSACTION_ABORTABLE honest subset after timeout (Phase 94; FindCoordinator never); transaction max timeout clamp yes (Phase 96; default 15m; Init **50** over-max) 

- Fetch sessions not durable / multi-broker sticky; omit cache is HWM+LSO only (not byte-identical Kafka response cache); idle TTL + max/LRU yes (Phase 95)  
- ACL store is single-node file (no consensus)  
- DeleteRecords does not fan out to cluster followers  
- Compaction simpler than Kafka (no tombstone retention window)  
- Inter-broker not ACL-gated; uses shared-token when configured  

See [ROADMAP.md](../ROADMAP.md) for the full deferred list.
