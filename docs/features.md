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
| Fuzz scaffold | `fuzz/` targets for frame/request decode; corpus smoke + CI (Phase 112) |

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
| DeleteRecords | Truncate sealed segments before offset; GC/clip aborted soft markers vs new log start (Phase 104/111); native optional `wait_majority` trailer (Phase 137) |
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
| Soft-marker GC (Phase 104/111) | Drop markers with `end_offset <= log_start`; clip straddlers to `first_offset = log_start`; persist `__txn_markers` |
| Control batches (Phase 89/98/105) | EndTxn COMMIT/ABORT + crash-promote ABORT magic-2 control RecordBatches on log; empty AddPartitions membership included (control-only) |
| Prepared 2PC MVP (Phase 90) | Enable2Pc prepare→complete EndTxn; KeepPreparedTxn + OngoingTxn*; `__txn_prepared` |
| Prepared timeout (Phase 92) | Lazy auto-abort after timeout (default 60s; `VOLANT_PREPARED_TXN_TIMEOUT_MS`; `0` disables) |
| Open txn timeout (Phase 93) | Honor InitProducerId `transaction_timeout_ms` (or broker default `VOLANT_OPEN_TXN_TIMEOUT_MS`, 60s; effective `0` disables); lazy auto-abort |
| Transaction max timeout (Phase 96) | Broker max default **15m** (`VOLANT_TRANSACTION_MAX_TIMEOUT_MS`; `0` = no max); Init over-max → **50**; effective open/prepared clamped |
| Background sweeper (Phase 97/101/106) | Periodic open/prepared + idle session expiry (default 1s / `VOLANT_SWEEP_INTERVAL_MS`); `0` pauses bg; always-spawn so 0→>0 without restart; graceful shutdown/join on stop; lazy paths remain |
| BROKER config (Phase 99–103) | Describe/Alter txn/session/sweep knobs; **sparse** durable under `__broker_config/state.json` (only altered keys; DELETE unfreezes env); resource name must be empty or local `node_id` decimal (Phase 103) |
| Durable fetch sessions (Phase 115) | `{data_dir}/__fetch_sessions/state.json`; restart restores session_id/epoch/omit cache within idle TTL; multi-broker sticky via 119 forward + **138/139 mirror** |
| Shared session mirror (Phase 138+139+143) | Best-effort peer put/delete (opcodes 90–93); foreign mirror table; promote on owner forward miss; **139:** coalesce dirty ops, debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50; Delete immediate), optional durable `__fetch_session_mirrors` (`VOLANT_FETCH_SESSION_MIRROR_DURABLE`), `mirror_gen` fence; **143:** `promoted_by` lowest-id claim fence on equal-fresh dual-promote (MirrorPut); **not** Raft; brief dual primary until claim exchange |

## Security (19–22)

| Feature | Behavior |
|---------|----------|
| mTLS identity | Client cert CN/SAN as principal |
| Principal ACLs | Topic / group / cluster (+ Kafka User resource store-only); allow/deny; durable file |
| Super-users | Bypass ACL checks |
| SCRAM-SHA-256 | Durable users; **native** + Kafka SASL |
| SCRAM-SHA-512 | Dual hashes per user; **Kafka SASL only** |

## Stream processing (Phase 4+ / 149 / 151)

In-process `volant-stream`: map, filter, flat_map, reduce, windows, foreach.
**State stores:** `MemoryStore` (default) and **Phase 149** `DurableStore` (redb
under a directory; `{state_dir}/kv.redb`). Use `count_reduce_durable(path)` or
`StreamBuilder::state_dir` + `reduce_count_durable`.

**Processing guarantees:**
| Mode | API | Behavior |
|------|-----|----------|
| At-least-once (default) | `StreamApp::start` / default builder | Produce sink → then `OffsetCommit`; crash between may redeliver |
| Exactly-once MVP (Phase 151) | `StreamBuilder::exactly_once(txn_id)` / `StreamApp::start_exactly_once` | Per non-empty step: `begin` → transactional produce → `add_offsets(group, positions)` → `commit`; empty poll skips txn; fence via `transactional_id` |

**Honesty:** EOS depends on Volant write-through transactions + soft markers — **not**
full Kafka Streams EOS / 2PC with durable stream state in the same txn. Durable
aggregates alone (149) do not imply EOS; pair with 151 for sink+offset atomicity.
No distributed stream workers; window buckets still process-local.

## Leader epochs (Phase 87)

| Feature | Behavior |
|---------|----------|
| Durable history | `{data_dir}/__leader_epochs/state.json` — `(epoch, start_offset)` per partition |
| OffsetForLeaderEpoch | Prior epochs → transition end offset; current / `-1` → HWM |
| Metadata | Live `leader_epoch` (not always `-1`) |
| Advance | Explicit bump / multi-node failover best-effort |

## Fetch DivergingEpoch + sessions (Phase 88 + 91 + 95 + 115 + 119 + 138/139) + preferred replica (126+133+140+144) + rack-aware create (145)

| Feature | Behavior |
|---------|----------|
| DivergingEpoch | Fetch v12+ tag 0 when `last_fetched_epoch` + `fetch_offset` past epoch end |
| Partition error | `OFFSET_OUT_OF_RANGE` with empty records; HWM/LSO still filled |
| Sessions | Owner-local create / merge / forgotten / FINAL close; durable under `__fetch_sessions` (115) |
| Incremental | Empty topics re-fetches session set |
| Omit-unchanged (91) | Empty-topics incremental omits partition when HWM+LSO unchanged and records empty |
| Idle TTL (95) | Default 60s (`VOLANT_FETCH_SESSION_IDLE_MS`; `0` disables); lazy on create/incremental |
| Max sessions (95) | Default 1000 (`VOLANT_FETCH_SESSION_MAX`; `0` unlimited); LRU-evict at cap |
| Multi-broker (119) | Cluster session_id encodes owner; peer miss → transparent inter-broker Fetch forward |
| Shared mirror (138+139+143) | Best-effort MirrorPut/Delete (90–93) after primary mutations; foreign mirror not served while owner alive; owner forward fail → `promote_from_mirror` then local serve; put lag/fail still **70**; **139:** one pending op per `session_id` (Delete supersedes Put); debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50; Delete immediate); optional durable `__fetch_session_mirrors/state.json` (`VOLANT_FETCH_SESSION_MIRROR_DURABLE`); `mirror_gen` fencing; **143:** `promoted_by` lowest-id claim fence (equal-fresh dual-promote converges via MirrorPut; metric `volant_fetch_session_promote_claim_reject_total`); metrics `volant_fetch_session_mirror_puts_total` / `_deletes_total` / `volant_fetch_session_promote_total` / `volant_fetch_sessions_mirrored` + `_puts_coalesced_total` / `_stale_put_rejects_total` / `volant_fetch_session_promote_supersede_total` / `volant_fetch_session_mirror_restored` / `volant_fetch_session_promote_claim_reject_total` |
| PreferredReadReplica (126+133+140+144) | Fetch v11+ client `rack_id`; leader may set PreferredReadReplica to same-rack live ISR peer with usable addr + LEO≥HWM (empty records redirect); **rank highest LEO then lowest id** (Phase 133); optional max lag vs leader LEO (`VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG`; unset = unlimited) (Phase 140); **suppressed when isolation=READ_COMMITTED** (leader serves aborted-marker filter; metric `volant_preferred_replica_suppressed_total` when a candidate existed) (Phase 140); **suppressed when established fetch session** (`req_session_id != 0`; metric `volant_preferred_replica_session_suppressed_total`) (Phase 144); gated off for followers (`replica_id` top-level ≤v14 / ReplicaState tag 1 on v15+); Metadata/DescribeCluster/NodeEndpoints advertise `cluster.toml` rack; metric `volant_preferred_replica_redirect_total` |
| Rack-aware create assignment (145) | When ≥2 distinct configured racks, create-topic / create-partitions places replicas to maximize rack diversity (deterministic; leader = first replica). Default on; `VOLANT_RACK_AWARE_ASSIGNMENT=0` forces legacy round-robin. No/single rack → legacy RR. Metric `volant_rack_aware_assignment_total`. No rebalance of existing topics. |
| Shared mirror (138+139+143+147) | Best-effort MirrorPut/Delete (90–93) after primary mutations; foreign mirror not served while owner alive (forward path); owner forward fail → **default serve-from-mirror without promote** (Phase 147; metric `volant_fetch_session_serve_from_mirror_total`); opt-in promote via `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` or legacy `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE=0`; put lag/fail still **70**; **139:** one pending op per `session_id` (Delete supersedes Put); debounced Puts (`VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS` default 50; Delete immediate); optional durable `__fetch_session_mirrors/state.json` (`VOLANT_FETCH_SESSION_MIRROR_DURABLE`); `mirror_gen` fencing; **143:** `promoted_by` lowest-id claim fence (equal-fresh dual-promote converges via MirrorPut; metric `volant_fetch_session_promote_claim_reject_total`); **147 residual:** dual-epoch (two peers may both serve mirrors without single SoT); metrics `volant_fetch_session_mirror_puts_total` / `_deletes_total` / `volant_fetch_session_promote_total` / `volant_fetch_sessions_mirrored` + `_puts_coalesced_total` / `_stale_put_rejects_total` / `volant_fetch_session_promote_supersede_total` / `volant_fetch_session_mirror_restored` / `volant_fetch_session_promote_claim_reject_total` / `volant_fetch_session_serve_from_mirror_total` |
| PreferredReadReplica (126+133+140) | Fetch v11+ client `rack_id`; leader may set PreferredReadReplica to same-rack live ISR peer with usable addr + LEO≥HWM (empty records redirect); **rank highest LEO then lowest id** (Phase 133); optional max lag vs leader LEO (`VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG`; unset = unlimited) (Phase 140); **suppressed when isolation=READ_COMMITTED** (leader serves aborted-marker filter; metric `volant_preferred_replica_suppressed_total` when a candidate existed) (Phase 140); gated off for followers (`replica_id` top-level ≤v14 / ReplicaState tag 1 on v15+); Metadata/DescribeCluster/NodeEndpoints advertise `cluster.toml` rack; metric `volant_preferred_replica_redirect_total` |

| Errors | 70 session id not found (incl. after TTL/LRU / dead owner without mirror); 71 invalid session epoch |

## Cluster ISR (Phase 6 + 108/110/118/125)

| Feature | Behavior |
|---------|----------|
| Death shrink | Follower death → local ISR drop + HWM recompute on every observer (Phase 108); non-controllers also via alive-set / expire (Phase 110) |
| Lag shrink | On `ReplicaFetch`, in-ISR members with lag > `replica_lag_max_messages` leave ISR even if alive (Phase 118) |
| Time lag shrink | On `ReplicaFetch` / leader apply, members whose last caught-up stamp is older than `replica_lag_max_ms` (default 30s; `0` off) leave ISR even if message lag is within threshold (Phase 125) |
| Rejoin | Recovering follower re-enters when fetch LEO ≥ HWM and lag ≤ `replica_lag_max_messages` (Phase 118); time lag does not block rejoin once caught up |
| Metrics | `volant_isr_expand_total`, `volant_isr_shrink_total`, `volant_isr_time_shrink_total` |

## Cluster admin fan-out (Phase 113)

| Feature | Behavior |
|---------|----------|
| DeleteRecords fan-out | Partition **leader** path. **Default (wait off):** truncates locally first, then best-effort `ReplicaDeleteRecords` / journal note / outbox at **achieved** `low_watermark` after whole-segment clamp (not client-requested `before_offset`). Client success does **not** wait on peer/journal majority. **Phase 148 wait on:** journal majority note **first**; fail → native **15** / Kafka **19** with **unchanged** `log_start` (no local truncate; provisional journal rolled back); ok → local truncate then replica/outbox only. Metric `volant_delete_records_fanout_errors_total`; wait metrics `volant_delete_records_majority_wait_*` + `volant_delete_records_majority_first_*`. **Phase 116:** peers pre-enqueued under `{data_dir}/__delete_records_outbox` (at-least-once) and drained when live. **Phase 123:** new leader reconciles from local `log_start`. **Phase 129/130:** truncate journal majority note + full-snapshot push; reconcile = `max(local log_start, journal watermark)`. **Phase 131–132:** heartbeat rejoin catch-up (non-blocking schedule + min-interval). **Phase 134:** peer-to-peer heartbeat mesh. **Phase 135/137:** env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` + native trailer `wait_majority` 0/1/2; Kafka still env/broker only. **Phase 137 journal GC:** assignment prune + push known-topic filter. Caps: 100k entries / 4 MiB push snapshot |
| BROKER config fan-out | Cluster **controller-only** Alter / IncrementalAlter for Phase 99 knobs **+** registry TTL (Phase 128; seven keys); generationed push to live peers; sparse durable on each node. Non-controller → `NotController` / Kafka **41**. **Phase 117:** durable gens + heartbeat lag re-push (full effective knobs) so offline peers converge on rejoin. **Phase 136:** catch-up is **non-blocking** (schedule + spawn from HeartbeatBroker; per-peer single-flight + min-interval via `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS`, default 500ms); metric `volant_admin_catchup_skipped_total` |
| ACL snapshot fan-out | Cluster **controller-only** Create/Delete Acls; generationed full snapshot push; peers install + persist `__acls`. List/authorize remain local after apply. **Phase 117:** same catch-up path on rejoin / controller restart. **Phase 136:** same non-blocking schedule/throttle as BROKER config catch-up |

## Multi-broker 2PC (Phase 114 MVP) + txn forward (Phase 120/122) + sticky FindCoordinator (Phase 121) + durable registry (Phase 124)

| Feature | Behavior |
|---------|----------|
| Open fan-out | After BeginTxn / successful AddPartitions, coordinator best-effort installs producer + empty open on live peers so partition leaders accept write-through; open carries `coordinator_node_id` (Phase 120) |
| Init registration | Successful transactional Init best-effort registers producer + coordinator on peers **without** opening (`install_open=false`) so EndTxn / offset APIs can forward early |
| Prepare / complete | Enable2Pc first EndTxn prepares locally then **strict** fan-out `TxnParticipantPrepare`; second EndTxn finalizes + `TxnParticipantComplete`. Non-2PC one-shot EndTxn also completes peers' open ranges. **Txn SoT is the Init-owner coordinator** |
| EndTxn forward | Non-coordinator Kafka EndTxn transparent-forwards via opcodes 84/85 (`KafkaTxnForward`) when coordinator is known (no dual prepare) |
| AddOffsets / TxnOffsetCommit forward | Phase 122: same 84/85 path; non-coordinator always forwards when owner known so deferred offsets buffer only on coordinator (no dual-commit) |
| Sticky FindCoordinator | Group/txn keys → murmur2 over sorted configured broker ids; next live if preferred dead; known transactional_id → Init owner (overrides hash) |
| Durable Init-owner registry (Phase 124) | `{data_dir}/__txn_coordinator/state.json`; restart restores by_id/by_pid; not cluster SoT / not full `__transaction_state` |
| Registry TTL GC (Phase 127) | Drop stale by_id/by_pid when `last_touch` age exceeds TTL (default **24h** / `VOLANT_TXN_COORDINATOR_TTL_MS`; `0` disables); background sweeper + metric `volant_txn_coordinator_registry_gc_total`. **Sharp edge:** long-lived open txns that never re-note lose Init-owner mapping → FindCoordinator override / EndTxn forward fall back to hash ring only until re-Init |
| Registry TTL BROKER config (Phase 128) | Describe/Alter `volant.txn.coordinator.registry.ttl.ms` (same default/semantics as env; `0` off); sparse durable + controller fan-out; live GC uses AtomicU64 |
| Durable prepared | Local `__txn_prepared/state.json` on each participant + controller `__txn_prepared/cluster.json` index (identity/decision only) |
| Fence | Init KeepPreparedTxn=false aborts local; peers force-abort via complete with `commit=false` even if prepared was PrepareCommit |
| Metrics | `volant_txn_2pc_fanout_errors_total`, `volant_cluster_prepared_txns`, `volant_txn_forward_total` / `_errors_total` (25/26/28) |

## Open limitations (native)

- Multi-language clients deferred  
- Long fuzz campaigns / chaos-mesh deferred (corpus smoke CI MVP: Phase 112)  
- No Raft metadata / dynamic membership  
- ISR rejoin/lag shrink yes (Phase 118 offset + Phase 125 time lag via `replica_lag_max_ms`; Metadata may lag when leader ≠ controller; not full Kafka replica.lag.time.max.ms)  
- Crash≡abort control batches yes (Phase 98); empty AddPartitions control yes (Phase 105)  
- Prepared 2PC multi-broker MVP yes (Phase 114; **not** full KIP-890/939 / `__transaction_state` topic); prepared timeout yes (Phase 92); open-txn timeout yes (Phase 93); TRANSACTION_ABORTABLE honest subset after timeout (Phase 94; FindCoordinator never); transaction max timeout clamp yes (Phase 96; default 15m; Init **50** over-max)  
- Transparent EndTxn + AddOffsets + TxnOffsetCommit forward yes (Phase 120/122; Init-owner registry + inter-broker); sticky FindCoordinator yes (Phase 121; murmur2 + registry override); pin Init still recommended if client skips FindCoordinator
- Durable Init-owner registry yes (Phase 124; local `__txn_coordinator`); TTL GC yes (Phase 127/128; default 24h wall-clock on `last_touch`; BROKER Describe/Alter `volant.txn.coordinator.registry.ttl.ms` / env `VOLANT_TXN_COORDINATOR_TTL_MS`; `0` disables; re-Init still overwrites). **Operators:** long-lived open txns must re-note (re-Init / open fan-out) within TTL or set TTL=`0` / longer — else Init-owner drop → hash-ring-only FindCoordinator / forward (wrong coordinator risk until re-Init)  

- Fetch sessions durable local (Phase 115) + multi-broker forward MVP (Phase 119) + best-effort shared mirror + promote (Phase 138) + coalesce/debounce + optional durable peer mirrors + `mirror_gen` fence (Phase 139) + promote claim fence lowest-id `promoted_by` (Phase 143; not Raft; brief dual primary until MirrorPut exchange; no session_id re-encode; no serve-without-promote); omit cache is HWM+LSO only (not byte-identical Kafka response cache); idle TTL + max/LRU yes (Phase 95); PreferredReadReplica MVP yes (Phase 126+133 LEO-desc/usable-addr ranking + Phase 140 optional max LEO lag + RC suppress metric + Phase 144 established-session suppress; not full selector/throttling; **no preferred under READ_COMMITTED**; **no preferred when client already has fetch session**); rack-aware create assignment MVP yes (Phase 145; create-time only; no rebalance)

- ACL / BROKER admin SoT is the **controller** (Phase 113 push + Phase 117 durable gens / rejoin catch-up + Phase 136 non-blocking/throttled schedule), not Raft consensus; brief lag until heartbeat catch-up is honest  

- DeleteRecords fan-out is **best-effort** at **achieved** low (post whole-segment clamp) by default; peers still clamp independently; journal max-merge SoT. Offline peers get **durable leader outbox retry** (Phase 116) + **new-leader reconcile** (Phase 123/129: `max(local log_start, journal watermark)`) — multi-controller majority journal MVP (Phase 130) + **heartbeat journal rejoin catch-up** (Phase 131) + **non-blocking/throttled catch-up** (Phase 132) + **p2p heartbeat mesh** (Phase 134; not full Raft log). **Phase 135/137 wait** surfaces majority fail to the client (native 15 / Kafka 19) without undoing local truncate; native per-request trailer (Phase 137) overrides the broker knob; Kafka still env/broker only; replica log truncate + outbox remain best-effort even in wait mode. Assignment remove prunes peer journal topics; push cannot resurrect unknown topics (Phase 137). **TruncateJournalNote** rejects negative/stale epochs / unknown TP (residual IT `phase132_journal_note_fence`); enable cluster auth for 86/88 (residual IT `phase133_journal_auth`; **current-epoch forge** under weak auth remains)
- Truncate-journal majority (Phase 130/135/137) uses **configured N** (`floor(N/2)+1`), not live-only: **N=2 + one down → permanent majority fail** (local note may persist; wait → NotEnoughReplicas). Prefer odd N (3+)

- Compaction simpler than Kafka (no tombstone retention window)  
- Inter-broker not ACL-gated; uses shared-token when configured  

See [ROADMAP.md](../ROADMAP.md) for the full deferred list.
