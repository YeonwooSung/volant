# Volant operations runbook

## Process flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--listen` | | `0.0.0.0:9092` | Client/broker TCP listen |
| `--data-dir` | | `./data` | Segment + offset store root |
| `--metrics-addr` | | *disabled* | Prometheus `GET /metrics` |
| `--metrics-token` | `VOLANT_METRICS_TOKEN` | *unset* | Optional Bearer for `/metrics` (Phase 21) |
| `--log-format` | | `text` | `text` or `json` |
| `--auth-token` | `VOLANT_AUTH_TOKEN` | *unset* | Shared-token auth (native port only) |
| `--scram-user USER:PASS` | | *unset* | Upsert SCRAM user at startup (repeatable; Phase 22) |
| `--kafka-listen` | | *disabled* | Kafka wire protocol shim (Phases 23–109; cluster ISR death Phase 108/110) |
| `--tls-cert` / `--tls-key` | | *unset* | Server TLS (feature `tls`) |
| `--tls-peer-insecure` | | `true` | Skip inter-broker cert verify (lab) |
| `--tls-ca` | | *unset* | CA PEM for inter-broker peer verify |
| `--no-tls-inter-broker` | | off | Keep inter-broker plaintext when server TLS on |
| `--cluster-config` / `--node-id` | | *unset* | Multi-node (Phase 6) |

Logging filter: `RUST_LOG` (e.g. `volant=info,volant_broker=debug`).

## Metrics scrape

```bash
# Start with metrics on localhost
volant-server --data-dir ./data --listen 127.0.0.1:9092 --metrics-addr 127.0.0.1:9102

curl -s http://127.0.0.1:9102/metrics | grep volant_
```

Key series (prefix `volant_`):

- `volant_produce_requests_total{result=...}`
- `volant_fetch_requests_total{result=...}`
- `volant_produce_messages_total` / `volant_fetch_messages_total`
- `volant_rpc_errors_total`
- `volant_connections_accepted_total`
- `volant_topics` / `volant_partitions` (gauges)
- `volant_fetch_sessions_active` / `volant_fetch_sessions_evicted_total` (Phase 95)
- `volant_fetch_sessions_idle_evicted_total` (Phase 97 idle-only subset)
- `volant_fetch_sessions_restored` / `volant_fetch_sessions_persist_errors_total` (Phase 115 durable)
- `volant_fetch_session_forward_total` / `volant_fetch_session_forward_errors_total` (Phase 119 multi-broker handoff)
- `volant_fetch_session_mirror_puts_total` / `volant_fetch_session_mirror_deletes_total` (Phase 138 peer mirror installs/removes applied)
- `volant_fetch_session_promote_total` (Phase 138 mirror→primary after owner miss; opt-in after Phase 147)
- `volant_fetch_sessions_mirrored` (Phase 138 gauge: foreign mirrors currently held)
- `volant_fetch_session_mirror_puts_coalesced_total` / `volant_fetch_session_mirror_stale_put_rejects_total` / `volant_fetch_session_promote_supersede_total` / `volant_fetch_session_mirror_restored` (Phase 139 coalesce / `mirror_gen` fence / durable restore)
- `volant_fetch_session_promote_claim_reject_total` (Phase 143 dual-promote / claim-lose rejects)
- `volant_fetch_session_serve_from_mirror_total` (Phase 147 owner-miss serve foreign mirror without promote)
- `volant_preferred_replica_redirect_total` (Phase 126 PreferredReadReplica redirects)
- `volant_preferred_replica_suppressed_total` (Phase 140: READ_COMMITTED suppress when a preferred candidate existed)
- `volant_preferred_replica_session_suppressed_total` (Phase 144: preferred suppress when client has established fetch session)
- `volant_rack_aware_assignment_total` (Phase 145: create/create-partitions used multi-rack diversity placement)
- `volant_assignment_consensus_success_total` / `_fail_total` (Phase 150: assignment generation majority commits / misses)
- `volant_assignment_committed_generation` (Phase 150: last majority-committed assignment gen gauge)
- `volant_assignment_metadata_committed_only` (Phase 152: Metadata uses committed assignment snapshot, gauge 0/1)
- `volant_assignment_generation_lag` (Phase 152: `max(0, live_gen - committed_gen)`)
- `volant_metadata_raft_term` / `volant_metadata_raft_commit_index` / `volant_metadata_raft_last_applied` (Phase 154: KRaft-style metadata log gauges)
- `volant_metadata_raft_append_success_total` / `_fail_total` (Phase 154: majority append commits / misses)
- `volant_txn_forward_total` / `volant_txn_forward_errors_total` (Phase 120/122 Kafka txn API forward: EndTxn / AddOffsets / TxnOffsetCommit)
- `volant_txn_coordinator_registry_restored` / `volant_txn_coordinator_registry_persist_errors_total` (Phase 124 durable Init-owner registry)
- `volant_txn_coordinator_registry_gc_total` (Phase 127 registry TTL GC drops)
- `volant_journal_catchup_success_total` / `volant_journal_catchup_errors_total` (Phase 131 truncate journal rejoin catch-up)
- `volant_journal_catchup_skipped_total` (Phase 132 schedule skips: in-flight single-flight or min-interval throttle; env `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`, default 500ms, `0` disables time throttle)
- `volant_admin_catchup_skipped_total` (Phase 136 admin ACL/config catch-up schedule skips: in-flight or min-interval; env `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS`, default 500ms, `0` disables time throttle)
- `volant_delete_records_majority_wait_success_total` / `_fail_total` (Phase 135/137/148; only when **effective** wait is on — broker env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` and/or native request trailer `wait_majority=1`; **Phase 148:** wait fail no longer truncates local log)
- `volant_delete_records_majority_first_success_total` / `_fail_total` (Phase 148: wait-mode majority-first path; fail = log_start unchanged)
- `volant_cluster_configured_brokers` / `volant_cluster_live_brokers` / `volant_cluster_majority_quorum` / `volant_cluster_majority_impossible` (Phase 141: journal majority health; `impossible=1` when `live < floor(N/2)+1` for configured N — classic N=2 one-down)
- `volant_open_txns` / `volant_prepared_txns` (Phase 97 gauges)
- `volant_open_txns_expired_total` / `volant_prepared_txns_expired_total` (Phase 97)
- `volant_build_info{version=...}`

Bind metrics to localhost in production; do not expose publicly without a proxy ACL.

## JSON logs

```bash
volant-server --log-format json --data-dir ./data --listen 127.0.0.1:9092
```

JSON fields include timestamp, level, target, message, and active span fields
(`opcode`, `correlation_id` on the `rpc` span; produce/fetch spans on hot paths).

## Shared-token auth

1. Start server with `VOLANT_AUTH_TOKEN=s3cret` (or `--auth-token s3cret`).
2. Clients set `ClientConfig.auth_token = Some("s3cret".into())` — Auth is sent on connect.
   CLI: `volant --auth-token s3cret …` or `VOLANT_AUTH_TOKEN`.
3. Wrong token → error **17** `AuthenticationFailed`.
4. Other opcodes before Auth → error **18** `AuthenticationRequired`.
5. When the token is **unset**, auth is disabled (Auth is a no-op success).

### Token rotation

1. Deploy new token to clients first (or dual-run a short window if you terminate and restart brokers).
2. Restart brokers with the new `VOLANT_AUTH_TOKEN`.
3. In-flight connections with the old token fail Auth on reconnect — clients should reconnect.

Phase 7 has no dual-token window; schedule a brief reconnect storm.

Inter-broker RPCs (ReplicaFetch, HeartbeatBroker, ClusterState) send Auth first
when the token is configured.

## SCRAM-SHA-256 (Phase 22)

User/password auth with durable credentials under `{data_dir}/__scram/users.json`.
Crypto follows RFC 5802 / 7677; the wire format is Volant binary (opcodes 60–69),
not Kafka SASL handshake bytes.

```bash
# Bootstrap users at process start (repeatable)
volant-server --data-dir ./data --listen 127.0.0.1:9092 \
  --scram-user alice:s3cret --scram-user bob:other

# Or bootstrap over the wire when the store is empty (no auth required yet):
volant user create --username alice --password s3cret --broker 127.0.0.1:9092

# After users exist, clients must SCRAM (or token / mTLS):
volant --scram-user alice --scram-password s3cret topic list --broker 127.0.0.1:9092
# env: VOLANT_SCRAM_USER / VOLANT_SCRAM_PASSWORD

volant --scram-user alice --scram-password s3cret user list
volant --scram-user alice --scram-password s3cret user delete --username bob
```

Rust client:

```rust
ClientConfig {
    brokers: vec!["127.0.0.1:9092".into()],
    scram_username: Some("alice".into()),
    scram_password: Some("s3cret".into()),
    ..ClientConfig::default()
}
```

Notes:

- `auth_required` when shared token **or** any SCRAM user **or** mTLS is configured.
- Successful SCRAM sets connection principal = username (feeds Phase 20 ACLs).
- Create/Delete/ListScramUsers need Cluster Alter/Describe when ACLs are on
  (except bootstrap Create when the store is empty).
- Password is sent in clear on CreateScramUser — use TLS in production.
- Inter-broker RPC still uses shared-token Auth, not SCRAM.

## Kafka wire shim

Optional second socket speaking Kafka framing (classic + flexible). Native
Volant protocol remains on `--listen`. API versions and honesty notes live in
**[KAFKA_COMPAT.md](./KAFKA_COMPAT.md)** (source of truth; Phases 23–93).

### Enable

```bash
volant-server \
  --data-dir ./data \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093
```

### Ops notes

- **Dual ports:** native clients on `--listen`; Kafka clients on `--kafka-listen`.
  Prefer binding the Kafka port to localhost / private networks; leave disabled
  unless you need Kafka-protocol discovery.
- **Auth:** shared-token (`--auth-token` / `VOLANT_AUTH_TOKEN`) is **native-only**.
  On the Kafka port use SASL (**PLAIN**, **SCRAM-SHA-256**, **SCRAM-SHA-512**) or
  anonymous + ACLs. When SCRAM users exist (`--scram-user` / `volant user create`),
  SASL is required before other APIs. Principal after SASL = username (feeds ACLs);
  without SASL the shim principal is `kafka-anonymous`.
- **Compression:** Produce accepts gzip/snappy/lz4/zstd RecordBatch (and gzip/
  snappy/lz4 MessageSet). Fetch re-encodes with `VOLANT_KAFKA_FETCH_COMPRESSION`
  (default **lz4**; `none`/`gzip`/`snappy`/`lz4`/`zstd`). MessageSet has no zstd —
  env `zstd` maps to lz4 for Fetch v0–3. Log storage remains uncompressed.
- **Topic config keys** (Describe/AlterConfigs TOPIC): `retention.ms`,
  `retention.bytes`, `segment.bytes`, `cleanup.policy` (`delete`|`compact`).
- **Broker config keys** (Describe/AlterConfigs BROKER, Phase 99–103 + **113**):
  `transaction.max.timeout.ms` (Kafka name),
  `volant.open.transaction.timeout.ms`,
  `volant.prepared.transaction.timeout.ms`,
  `volant.fetch.session.idle.ms`, `volant.fetch.session.max`,
  `volant.sweep.interval.ms`. Resource **name** must be empty or this broker's
  `node_id` decimal (default `"0"`); other names → `INVALID_REQUEST` (42)
  (Phase 103). Precedence: product default → env → **sparse**
  durable `{data_dir}/__broker_config/state.json` (only keys present) → runtime
  alter. Alter/Incremental SET/DELETE map to the same setters; successful
  non-validate_only **merges only altered keys** (atomic); DELETE restores
  product defaults live **and removes the key from the file** so env can
  re-apply on restart (Phase 102). **Cluster mode (Phase 113 + 117):** BROKER Alter is
  **controller-only** (Kafka **41** / native NotController on other brokers);
  controller pushes generationed knobs to live peers. Peers that miss a push
  (offline) catch up on heartbeat via controller re-push; generations live under
  `{data_dir}/__cluster_admin`. Target the controller
  (lowest live broker id / DescribeCluster) for cluster-wide knobs.
- **Transactions / isolation:** write-through + soft abort markers (Phase 86) +
  EndTxn control batches on finalize (Phase 89) + prepared 2PC MVP (Phase 90) +
  prepared timeout auto-abort (Phase 92, default 60s,
  `VOLANT_PREPARED_TXN_TIMEOUT_MS`; `0` disables) + open-txn timeout (Phase 93,
  InitProducerId `transaction_timeout_ms` or default 60s /
  `VOLANT_OPEN_TXN_TIMEOUT_MS`; effective `0` disables) + broker max timeout
  clamp (Phase 96, default **15m** / `VOLANT_TRANSACTION_MAX_TIMEOUT_MS`;
  `0` = no max; Init over-max → **50**; effective open/prepared clamped) +
  background sweeper (Phase 97/101/106, default **1s** / `VOLANT_SWEEP_INTERVAL_MS`;
  `0` = pause bg / lazy only; always-spawn so 0→>0 live without restart;
  graceful shutdown/join on server stop) +
  crash-promote ABORT control batches (Phase 98) + **empty AddPartitions control**
  (Phase 105: membership + control-only; no fake soft ranges) + **aborted
  soft-marker GC** (Phase 104/111: DeleteRecords / retention / load drop markers with
  `end_offset <= log_start` and clip straddlers to `first_offset = log_start`;
  drop metric `volant_aborted_markers_gc_total`);
  `READ_COMMITTED` caps at LSO and filters aborted; `READ_UNCOMMITTED` sees all.
  Open crash≡abort via `__txn_markers` (soft + ABORT control; empty membership
  via `open_added`); prepared durable under `__txn_prepared` until complete or
  timeout.
- **Leader epochs:** durable history under `{data_dir}/__leader_epochs` (Phase 87);
  OffsetForLeaderEpoch returns prior-epoch end offsets; Metadata advertises live
  epoch. Not a full KRaft epoch state machine.
- **Fetch DivergingEpoch / sessions (Phase 88 + 91 + 95 + 115 + 119 + 138/139):** truncation →
  OFFSET_OUT_OF_RANGE + DivergingEpoch tag 0 from history; fetch sessions
  (create / forgotten / errors 70–71); empty-topics incremental
  **omits** partitions when HWM+LSO unchanged and records empty (Phase 91);
  idle TTL (default 60s / `VOLANT_FETCH_SESSION_IDLE_MS`; `0` disables) + max
  concurrent sessions (default 1000 / `VOLANT_FETCH_SESSION_MAX`; `0` = unlimited;
  LRU eviction at cap) (Phase 95); idle also background-swept (Phase 97).
  **Durable per-broker** under `{data_dir}/__fetch_sessions/state.json` (Phase 115):
  restart on the same data_dir restores session_id / epoch / omit cache within idle
  TTL. **Multi-broker handoff MVP (Phase 119):** cluster session_ids embed the owner
  `node_id`; a peer that lacks the session transparent-forwards the Kafka Fetch body
  to the owner over inter-broker RPC (opcode 82/83) so epoch + omit-unchanged stay
  correct while the owner is alive. **Shared mirror MVP (Phase 138 + polish 139 + claim 143 + serve 147):**
  owner best-effort fans out MirrorPut/Delete (opcodes 90–93) to live peers; peers
  hold a foreign mirror table (not served while owner alive — still forward). Owner
  death / forward fail: if a mirror is present, **default serve from mirror without
  promote** (Phase 147; metric `volant_fetch_session_serve_from_mirror_total`); else
  **70**. Force promote with `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1`, or restore
  legacy always-promote with `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE=0`.
  **Phase 139:** dirty ops coalesce to one pending op per `session_id` (Delete
  supersedes Put); Puts debounce via `VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS`
  (default **50**; `0` = coalesce only, flush immediately); Deletes flush immediately;
  optional durable peer mirrors `VOLANT_FETCH_SESSION_MIRROR_DURABLE=1` →
  `{data_dir}/__fetch_session_mirrors/state.json` (default **off**; load filters idle
  TTL); `mirror_gen` fences stale apply/promote. **Phase 143:** `promoted_by`
  lowest-id claim fence on equal-fresh dual-promote (claim travels in MirrorPut;
  metric `volant_fetch_session_promote_claim_reject_total`). **Phase 147 residual:**
  dual-epoch (two peers may both serve mirrors without single SoT). Best-effort only
  (not Raft); put lag/fail still **70**; session_id owner bits are not re-encoded.
  Sticky routing still preferred for latency (one extra RTT on forward when owner is up).
- **PreferredReadReplica (Phase 126 + 133 + 140 + 144):** Fetch v11+ client
  `rack_id`; leader may redirect to same-rack live ISR peer with usable addr +
  LEO≥HWM (empty records; rank highest LEO then lowest id). Optional freshness:

  `VOLANT_PREFERRED_REPLICA_MAX_LEO_LAG` (parsed u64) skips peers with
  `leader_leo - follower_leo > lag`; **unset** = unlimited (126/133 behavior).
  **Suppressed** when isolation=`READ_COMMITTED` (leader keeps aborted-marker
  filter); metric `volant_preferred_replica_suppressed_total` when a candidate
  existed (Phase 140). **Also suppressed** when the client Fetch already has a
  non-zero `session_id` (established session; not FINAL close) so preferred does
  not send the session to a follower owner-miss thrash path; metric
  `volant_preferred_replica_session_suppressed_total` (Phase 144). Full fetch
  (`session_id == 0`) may still preferred-redirect. Not full Kafka
  selector/throttling.
- **Rack-aware create assignment (Phase 145):** when `cluster.toml` brokers
  declare ≥2 distinct `rack` values, new topic / create-partitions replica sets
  maximize rack diversity (leader = first replica). Default **on**; set
  `VOLANT_RACK_AWARE_ASSIGNMENT=0` (or `false`/`no`/`off`) for legacy
  round-robin. No racks / single rack → legacy RR unchanged. Metric
  `volant_rack_aware_assignment_total`. Does **not** rebalance existing topics.
- **ACLs:** Kafka ACL admin maps to Volant Phase 20/21 ACLs (LITERAL only;
  CreateAcls enables enforcement). Describe/Create/DeleteAcls **0–3**: v3 accepts
  Kafka **User** resource type (stored as `ResourceType::User`; not used on the
  produce/fetch authorize path; no SCRAM-admin gating).

Deep dives: [PHASE23_SPEC.md](./PHASE23_SPEC.md) … [PHASE100_SPEC.md](./PHASE100_SPEC.md).

## TLS (Phase 7 listen + Phase 9 verification / inter-broker)

```bash
cargo build -p volant-server --release --features tls
volant-server \
  --tls-cert /etc/volant/server.crt \
  --tls-key  /etc/volant/server.key \
  --listen 0.0.0.0:9092
```

- Default builds **without** the `tls` feature stay green on macOS/CI.
- Passing `--tls-cert` without the feature errors at startup.
- TLS listen is **TLS-only** (no plaintext dual-bind).
- **Inter-broker TLS** (Phase 9): when server TLS is enabled, peers also use TLS
  by default. Lab clusters keep `--tls-peer-insecure` (default `true`). For
  verified peers: `--tls-peer-insecure=false --tls-ca /etc/volant/ca.pem`.
  Escape hatch: `--no-tls-inter-broker` forces plaintext inter-broker.
- Client TLS: build `volant-client` with `--features tls`:
  - Lab: `ClientConfig { tls: true, tls_insecure: true, .. }`
  - Production: `tls: true`, `tls_insecure: false`, optional `tls_ca` PEM;
    public CAs via Mozilla roots (`webpki-roots`).

## Client leader redirect (Phase 8)

On `NotLeaderForPartition`, the Rust client:

1. Refreshes Metadata
2. Resolves the partition leader host:port
3. Reconnects (re-Auth if token set; re-TLS if enabled)
4. Retries (`ClientConfig.max_redirects`, default 1 extra attempt)

Set `max_redirects: 0` to disable (useful in tests that assert broker-level rejection).

Generate self-signed material for lab use only (see `examples/tls/`).

## Health checks

- TCP connect to `--listen`
- Optional: `GET /metrics` returns `200` with `volant_build_info` when
  unauthenticated; with `--metrics-token`, send `Authorization: Bearer …` or
  expect `401`
- Produce/fetch smoke via `volant` CLI

## Multi-node Helm (Phase 9)

```bash
helm install volant ./deploy/helm/volant \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set image.repository=volant \
  --set image.tag=0.2.0
```

Deploys a StatefulSet, headless Service, and ConfigMap `cluster.toml`
(`node-id = ordinal + 1`). Single-node Deployment remains the default
(`cluster.enabled=false`).

## Protocol fuzz + corpus smoke (Phase 9 / 112 / v0.15)

Deterministic chaos tests always run with `cargo test -p volant-protocol`
(`chaos_decode_does_not_panic`, `chaos_frame_decode_extended`).

**Phase 112 CI path (stable, no nightly):**

```bash
cargo test --workspace --all-targets
cargo test -p volant-protocol corpus_smoke
# or
./scripts/fuzz_corpus_smoke.sh test
```

Seed corpus: `fuzz/corpus/{decode_frame,decode_request,decode_extended}/`.
GitHub Actions: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).
Default CI is stable `corpus_smoke` only (includes `corpus_smoke_extended`).

**Optional local cargo-fuzz** (nightly; short capped runs only):

```bash
FUZZ_SMOKE_RUNS=200 ./scripts/fuzz_corpus_smoke.sh fuzz
# Capped wall-clock campaign (default 30s/target). CI does **not** run this
# on push/PR; optional workflow_dispatch job `fuzz-nightly` only.
FUZZ_LONG_SECS=30 ./scripts/fuzz_corpus_smoke.sh long
```

Details: [fuzz/README.md](../fuzz/README.md), [PHASE112_SPEC.md](./PHASE112_SPEC.md),
[V15_SPEC.md](./V15_SPEC.md).

## Native features after core (Phases 8–22)

Deep operator recipes (idempotence, groups, topic admin, txn CLI, mTLS, ACLs,
compaction) are summarized in **[features.md](./features.md)** and the binding
specs. Ops-critical notes only:

| Area | Ops fact |
|------|----------|
| Idempotent produce | Client `enable_idempotence`; durable PID under `__producer_state/` |
| Consumer lag | Metrics + `volant group lag` |
| Groups | list / describe / delete-offsets; static membership `group_instance_id` |
| Topic configs | `retention.ms` / `retention.bytes` / `segment.bytes` / `cleanup.policy` |
| Broker configs (Phase 99–102 + 113 + 117 + 136) | BROKER Describe/Alter: `transaction.max.timeout.ms` + `volant.*` open/prepared/session/sweep; **sparse** durable under `__broker_config/state.json`; **cluster:** controller-only Alter + fan-out to live peers (Phase 113); **catch-up** on rejoin via durable gens + heartbeat re-push (Phase 117; `__cluster_admin`); **non-blocking/throttled schedule** Phase 136 (`VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS`; metric `volant_admin_catchup_skipped_total`) |
| DeleteRecords | Truncates sealed segments; **best-effort fan-out at achieved low** (post whole-segment clamp) to other replicas (Phase 113) + **durable leader outbox retry** for offline/failed peers (`__delete_records_outbox`, Phase 116) + **new-leader reconcile from log_start** on leadership change (Phase 123; metric `volant_delete_records_outbox_reconcile_total`); **truncate journal** Phase 129/130 + **heartbeat lag rejoin catch-up** Phase 131 (`applied_journal_generation` → `TruncateJournalPush`) + **non-blocking/throttled schedule** Phase 132 (single-flight + `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`; metrics `volant_journal_catchup_*` incl. `_skipped_total`) + **peer-to-peer heartbeat mesh** Phase 134 (HB all configured peers; controller-only alive-set SoT); **optional client majority wait** Phase 135/137/148 (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY` default **off**; native trailer `wait_majority` 0/1/2); **Phase 148:** when **effective wait on**, journal majority runs **before** local truncate — fail → native **15** / Kafka **19** with **unchanged** `log_start` (provisional journal rolled back); wait **off** remains local-first best-effort (irreversible on majority miss); metrics `volant_delete_records_majority_wait_*` + `volant_delete_records_majority_first_*`; client `delete_records_with_wait_flag`; CLI `--wait-majority` / `--no-wait-majority`; **Kafka still env/broker knob only**; **Phase 137 journal GC:** assignment remove prunes topic watermarks; push apply filters to known topics (anti-resurrection); **TruncateJournalNote (86)** rejects negative/stale epochs / unknown TP; enable ACL/auth for inter-broker 86/88 in production; **GC/clip aborted soft markers** vs new log start (Phase 104/111) |
| Transactions (shipped) | **Write-through + soft markers** (Phase 86) + **control batches** on EndTxn finalize (Phase 89) + **empty AddPartitions control** (Phase 105) + **prepared 2PC MVP** (Phase 90) + **multi-broker Enable2Pc** (Phase 114: open/prepare/complete fan-out; controller `__txn_prepared/cluster.json`) + **EndTxn transparent forward** (Phase 120: Init-owner registry; non-coordinator Kafka EndTxn → opcodes 84/85) + **sticky FindCoordinator** (Phase 121: murmur2 over static membership; known txn → Init owner; preferred dead → next live) + **AddOffsets / TxnOffsetCommit forward** (Phase 122: same 84/85; offsets buffer only on coordinator) + **durable Init-owner registry** (Phase 124: `{data_dir}/__txn_coordinator`; restart restore) + **registry TTL GC** (Phase 127: drop by `last_touch` age; default **24h** / `VOLANT_TXN_COORDINATOR_TTL_MS`; `0` disables; metric `volant_txn_coordinator_registry_gc_total`) + **registry TTL BROKER config** (Phase 128: `volant.txn.coordinator.registry.ttl.ms`) + **prepared timeout** (Phase 92, default 60s / `VOLANT_PREPARED_TXN_TIMEOUT_MS`) + **open timeout** (Phase 93, InitProducerId / `VOLANT_OPEN_TXN_TIMEOUT_MS`) + **max timeout clamp** (Phase 96, default 15m / `VOLANT_TRANSACTION_MAX_TIMEOUT_MS`; Init **50** over-max) + **background sweeper** (Phase 97/101/106, default 1s / `VOLANT_SWEEP_INTERVAL_MS`; `0` = pause bg; always-spawn so 0→>0 without restart; graceful shutdown/join) + **BROKER config surface** (Phase 99) + **sparse durable restart** (Phase 100/102) + **BROKER name vs `node_id`** (Phase 103) + **marker GC/clip** (Phase 104/111); LSO/aborted filtering; open crash≡abort; prepared durable under `__txn_prepared` until complete or timeout; soft markers GC'd when `end_offset <= log_start`; straddlers clip `first_offset = log_start` (Phase 111). **Ops:** call **FindCoordinator** for group/txn keys (sticky); Init on the returned coordinator; EndTxn / AddOffsets / TxnOffsetCommit may hit any live broker once registration/open fan-out landed; produce still goes to partition leaders. **Sharp edge — registry TTL:** long-lived open txns that never re-touch (re-Init / note) can lose Init-owner mapping after TTL → FindCoordinator override / EndTxn forward fall back to hash ring only (wrong coordinator risk until re-Init). Set `VOLANT_TXN_COORDINATOR_TTL_MS=0` or lengthen TTL / Alter `volant.txn.coordinator.registry.ttl.ms`; clients should re-Init or otherwise re-note within TTL |
| mTLS | Feature `tls`; `--tls-client-ca` / optional `--tls-client-allow` |
| ACLs | `--acl-enable`; durable `__acls/acls.json`; User resource is Kafka admin store-only; **cluster:** Create/Delete controller-only + snapshot fan-out (Phase 113) + rejoin catch-up (Phase 117) + **non-blocking admin catch-up** (Phase 136) |
| Compaction | `cleanup.policy=compact` on **sealed** segments; empty value = tombstone |

**Assignment consensus (Phase 150/152) + metadata Raft (Phase 154):**

| Env | v0.2 default | Role |
|-----|--------------|------|
| `VOLANT_METADATA_RAFT` | **off** | `1`/`true`/`yes` prefers 154 AppendEntries 98/99; unset/`0` uses Phase 150 notes |
| `VOLANT_OPENRAFT_METADATA` | **off** | `1`/`true`/`yes`/`on` → `controller_id()` is the openraft leader (opcodes 108–111). Unset keeps lowest-id. |
| `VOLANT_ASSIGNMENT_METADATA_COMMITTED_ONLY` | **off** | `1` serves majority-committed Metadata snapshot + wait-like admin; unset/`0` is live assignment |
| `VOLANT_ASSIGNMENT_CONSENSUS` | **on** | Best-effort 96/97 push. Must **not** gate Metadata or fail CreateTopic |
| `VOLANT_ASSIGNMENT_CONSENSUS_WAIT` | **off** | `1` → native **15** on majority miss; **rolls back** live `assignment.json` (must_wait path only) |

CreateTopic / DeleteTopic / CreatePartitions succeed when the controller writes
`{data_dir}/cluster/assignment.json`. Kafka CreateTopics / DeleteTopics /
CreatePartitions honor the same wait/rollback (majority miss → Kafka **19**).
A 96/97 majority miss does **not** fail
the client unless wait or committed-only is on. On that **must_wait** path a
miss **rolls back** the live assignment (client 15 / Kafka 19, Metadata, and
`assignment.json` match the pre-mutation snapshot). `!must_wait` keeps the
local write as SoT. Metadata serves the **live**
assignment. Set `VOLANT_METADATA_RAFT=1` to append `SetAssignment` to
`{data_dir}/__metadata_raft/`, fan out `MetadataRaftAppend` (opcodes 98/99),
advance `commit_index` only on majority match_index, then apply + Phase 152
committed snapshot. Majority = configured N same as journal. Committed-only
snapshot lives under `__assignment_consensus/committed_snapshot.json`. Gauges:
`volant_assignment_generation_lag`,
`volant_metadata_raft_{term,commit_index,last_applied}`. **Not** full openraft
election (lowest-id controller remains leader unless v0.11 is on).

## v0.11 openraft metadata election

Opt-in only (`VOLANT_OPENRAFT_METADATA=1`). Default **off**: controller stays
lowest live id. When on, a 3+ node cluster runs an in-process **openraft**
group over native opcodes **108/109** (AppendEntries) and **110/111**
(RequestVote). `controller_id()` / Metadata controller is that leader.
Gauges: `volant_openraft_leader_id`, `volant_openraft_term`. This does **not**
replicate `assignment.json` through openraft and does **not** implement
InstallSnapshot. Homemade 154 log is unchanged. See [V11_SPEC.md](./V11_SPEC.md).

**Cluster sharp edges:** Truncate-journal majority (Phase 130), assignment
majority (Phase 150/154), and Phase 135/137/148 wait mode use **configured N**
(`floor(N/2)+1`), not live-only. For **N=2**, majority
is 2 — one peer down → permanent journal majority fail (`consensus_fail` / wait
`NotEnoughReplicas`). **Phase 148:** wait-on fail does **not** truncate local log
(provisional journal rolled back); wait-off still local-first. Prefer **odd N (3+)**
for majority journal / wait mode. Native clients may override the broker wait knob
per request (Phase 137); Kafka clients cannot.

**Phase 141 ops signal:** scrape
`volant_cluster_majority_impossible` (gauge 0/1) plus
`volant_cluster_configured_brokers` / `volant_cluster_live_brokers` /
`volant_cluster_majority_quorum`. Alert when `majority_impossible == 1` (or when
`live < quorum` for configured N). Single-node always reports impossible `0`.
Gauges are **local membership view** (death-detect lag can briefly disagree
across brokers).

## v0.2 ISR / chaos

What v0.2 **proves** in CI (do not re-open as new work):

| Scenario | Already tested by |
|----------|-------------------|
| 3-node `acks=all` leader kill; no acknowledged data loss | `cluster_failover::three_node_acks_all_survives_leader_kill` |
| Follower death ISR shrink; leader still accepts `acks=all` (Phase 108) | `phase8_redirect_restart::rolling_restart_follower_preserves_data` |
| Rolling follower restart while leader accepts `acks=all` | same Phase 8 test (stop accept → produce mid-down → rebind → produce) |
| Non-controller alive-set / expire death (Phase 110) | `phase110_alive_set_death` |
| ISR rejoin after catch-up + lag shrink (Phase 118) | `phase118_isr_rejoin` |
| Time-based ISR lag shrink (Phase 125) | `phase125_isr_time_lag` |
| N=2 majority health gauges after in-process death (Phase 141) | `phase141_n2_majority_ops` |
| Lowest-id controller death → next-lowest controller; `acks=all` + CreateTopic continue | `v02_isr_chaos::controller_death_lowest_id_failover_produce_continues` |
| N=2 one-dead: `volant_cluster_majority_impossible=1` **and** CreateTopic wait → native **15** | `v02_isr_chaos::n2_majority_impossible_create_topic_wait` |

**Operator recipe (RF=3, `min_insync_replicas=2`):**

1. Produce with `acks=all`. Restart **followers first** (ISR shrinks; remaining live ISR still ≥ min ISR).
2. Restart the controller last. Next-lowest live id becomes controller; clients refresh Metadata on `NotLeaderForPartition`. Brief Metadata lag on the new controller is allowed ([consistency.md](./consistency.md)).
3. Prefer **odd N (3+)**. On **N=2**, one peer down flips `majority_impossible=1` — journal majority and `VOLANT_ASSIGNMENT_CONSENSUS_WAIT=1` CreateTopic cannot succeed (must_wait miss **rolls back** live `assignment.json`; default wait **off** does not fail the client and keeps the local write).

**Wontfix in v0.2** (not a test gap to close here):

- Long chaos-mesh suites and uncapped fuzz campaigns (corpus smoke is Phase 112;
  v0.15 adds a capped local `long` campaign + operator Chaos Mesh YAMLs, not CI)
- Asymmetric / partial network-partition mesh → **closed by v0.15** in-process
  (`v15_asymmetric_isolate`) + `deploy/chaos/network-partition.yaml` (operator)

CLI examples: [features.md](./features.md), [../README.md](../README.md).

## v0.5 ops confidence

Closes the three v0.2 holes operators actually hit. Honest limits: **EACCES not ENOSPC**; **in-process isolate not chaos-mesh**; **no asymmetric partial mesh**.

| Scenario | Test | Honest limit |
|----------|------|----------------|
| Unwritable data dir: next produce errors (no panic); already-written records still fetch | `v05_ops_confidence::unwritable_data_dir_produce_errors_fetch_still_works` | CI `chmod`s the partition dir to `0o555` (and/or `.log` read-only). That is **EACCES**, not a full-disk **ENOSPC** volume. Operator path is the same: append fails. |
| Minority isolate of the partition leader (split-brain honesty) | `v05_ops_confidence::minority_isolate_leader_split_brain_honesty` | Abort `serve_listener` + outbound `inter_broker_rpc` hook. Process stays up. Survivors expire past `session_timeout`, elect, and accept `acks=all`. Isolated `acks=all` does not commit within the 10s HWM wait. Isolated `acks=1` may append locally (not cluster-committed). Not chaos-mesh; no asymmetric partial mesh. |
| Leader dies while `acks=all` is in flight | `v05_ops_confidence::leader_abort_mid_inflight_acks_all` | Pre-kill successful `acks=all` responses are present on the new leader. The in-flight batch may timeout or fail and is **not** required to be committed. |

Long chaos-mesh suites and uncapped fuzz remain deferred. v0.15 adds a capped
local `long` fuzz helper + operator Chaos Mesh YAMLs (not default CI) and an
in-process A→B isolate test. See [V15_SPEC.md](./V15_SPEC.md).

## v0.10 dynamic membership

Add or remove a broker without rewriting `cluster.toml` and restarting
every node. Overlay SoT: `{data_dir}/cluster/membership.json`. First
add/remove seeds the file from the current list.

```bash
volant cluster add-broker --id 3 --host 127.0.0.1 --port 9094 --broker 127.0.0.1:9092
volant cluster remove-broker --id 3 --broker 127.0.0.1:9092
volant cluster members --broker 127.0.0.1:9092
```

- New brokers are **configured immediately**, **live on heartbeat**.
- Majority N follows the overlay (add increases N; remove decreases N).
- Existing topic replicas are **not** moved onto a new broker.
- Push is **best-effort** (`MembershipPut` 100/101); no majority wait.
- Not Raft joint consensus. Isolated nodes can both accept add.

## v0.12 cluster metadata topic

Opt-in KRaft-**shaped** assignment snapshot topic. Default **off**. Not
Kafka KRaft record schemas and not a Raft metadata log.

| Env | Default | Role |
|-----|---------|------|
| `VOLANT_CLUSTER_METADATA_TOPIC` | **off** | `1`/`true`/`yes`: controller ensures `__cluster_metadata` (1 partition, RF=`min(3,N)`). CreateTopic / DeleteTopic / CreatePartitions also append a JSON assignment snapshot (`key` = generation decimal, header `volant-cluster-metadata=1`). On start, if `assignment.json` is missing/empty, rebuild from the last record. |
| `VOLANT_PARTITION_RAFT` | **off** | `1`/`true`/`yes`: new topics get a dual-write Raft log under `{data_dir}/__partition_raft/{topic}/{partition}/`. Does **not** replace ISR HWM. No second election (reuse partition leader). |

ISR + `assignment.json` stay the data-plane / assignment SoT when the
flags are off (and remain SoT for produce even when they are on).

## v0.15 fuzz / chaos

Closes the Phase 112 “corpus smoke only” leftover and the v0.5 “no
asymmetric / partial mesh” leftover **without** a multi-hour CI job.

| Path | What it is | CI? |
|------|------------|-----|
| `cargo test -p volant-protocol corpus_smoke` | Deterministic replay of `fuzz/corpus/*` including `decode_extended` (membership 100–107, txn 32/50/52) | Yes (stable) |
| `./scripts/fuzz_corpus_smoke.sh long` | `cargo +nightly fuzz run -max_total_time=$FUZZ_LONG_SECS` (default 30s/target) | **No** on push/PR. Optional `workflow_dispatch` job `fuzz-nightly` only |
| `deploy/chaos/*.yaml` | Chaos Mesh `PodChaos` (kill `volant-0`) + `NetworkChaos` (A→B `direction: to`) matching Helm labels | **No.** Operator-applied. See [deploy/chaos/README.md](../deploy/chaos/README.md) |
| `v15_asymmetric_isolate` | In-process A→B dest-block via `test_block_inter_broker_peer`. Listeners stay up | Yes |

Honesty: A→B RPC fails; B→A and C stay open. Phase 134 marks live on
successful **outbound**, so B does **not** expire A (unlike v0.5
symmetric isolate). Controller stays lowest-id. `acks=1` to a leader
that still reaches a majority of ISR still appends. Not a full Chaos
Mesh suite (no ENOSPC / slow-disk). Not a security-audit fuzz campaign.

## Metrics auth (Phase 21)

```bash
cargo run -p volant-server -- \
  --metrics-addr 127.0.0.1:9102 \
  --metrics-token "$VOLANT_METRICS_TOKEN"

curl -s -H "Authorization: Bearer $VOLANT_METRICS_TOKEN" \
  http://127.0.0.1:9102/metrics | head
```

- When `--metrics-token` is unset, `/metrics` stays open (prefer bind localhost).
- Wrong/missing token → `401` + `WWW-Authenticate: Bearer`.
- Does not automatically reuse `--auth-token`; set both if they should match.

## v0.13 `__transaction_state` (opt-in)

Default **off**. Set `VOLANT_TRANSACTION_STATE_TOPIC=1` to create the internal
`__transaction_state` topic (1 partition, RF `min(3, N)`) on first
InitProducerId / prepare. Records are **Volant JSON**
(`volant-txn-state=1`), not Kafka KIP-890 / KRaft schemas. Replay on start
uses the topic as SoT when the flag is on; `__txn_prepared` still stores
prepared ranges. `__txn_coordinator` remains the FindCoordinator routing map.
See [V13_SPEC.md](./V13_SPEC.md).

## Still deferred

- Multi-language clients
- Full chaos-mesh suites / long fuzz campaigns (corpus **smoke CI MVP** → **closed by Phase 112**; capped local `long` + operator Chaos Mesh YAMLs + in-process A→B isolate → **closed by v0.15**; multi-hour CI / ENOSPC mesh still deferred)
- Full KIP-890/939 / Kafka `__transaction_state` topic (multi-broker Enable2Pc MVP → **closed by Phase 114**)
- Multi-broker session affinity / durable sessions → **closed by Phase 115/119**; shared mirror + promote → **closed by Phase 138**; mirror polish (coalesce/debounce + optional durable + fence) → **closed by Phase 139** (best-effort residual: Raft registry / serve-without-promote / incremental put)
- Full preferred-replica selector / throttling residual (beyond 126/133/140/144; rack-aware create assignment → **closed by Phase 145**)
- Multi-broker session affinity / durable sessions → **closed by Phase 115/119**; shared mirror + promote → **closed by Phase 138**; mirror polish → **closed by Phase 139**; claim fence → **closed by Phase 143**; serve-from-mirror without promote → **closed by Phase 147** (best-effort residual: Raft registry / dual-epoch converge / incremental put)
- Full preferred-replica selector / throttling / rack-aware partition assignment (beyond 126/133/140 lag+suppress metric)
- Byte-identical Kafka compressed response cache (omit is HWM+LSO based)
- Accept-loop drain + single-flight background tasks → **closed by Phase 109** (bg join: Phase 106)
- Non-controller alive-set auto-death → **closed by Phase 110**
- Straddle soft-marker clip → **closed by Phase 111**
- cargo-fuzz corpus smoke + CI MVP → **closed by Phase 112**
- Cluster admin fan-out (DeleteRecords / BROKER config / ACL snapshot) → **closed by Phase 113**
- Controller failover / rejoin ACL+BROKER catch-up → **closed by Phase 117**
- Durable DeleteRecords outbox for offline replicas → **closed by Phase 116**
- DeleteRecords outbox leadership handoff → **closed by Phase 123** (new leader reconcile from log_start)
- ISR rejoin + lag-based shrink → **closed by Phase 118**
- Time-based ISR lag shrink → **closed by Phase 125** (`replica_lag_max_ms` / `VOLANT_REPLICA_LAG_MAX_MS`)
- Transparent EndTxn forward to txn coordinator → **closed by Phase 120**
- Hash-based sticky FindCoordinator → **closed by Phase 121**
- Transparent AddOffsetsToTxn / TxnOffsetCommit forward → **closed by Phase 122**
- Durable txn coordinator registry → **closed by Phase 124** (local `__txn_coordinator`)

Full list: [ROADMAP.md](../ROADMAP.md).

## v0.7 preferred throttle / probe

Opt-in only (`VOLANT_PREFERRED_REPLICA_THROTTLE_MS` / `VOLANT_PREFERRED_REPLICA_TCP_PROBE`; both default **off**). Throttle is a Fetch top-level `throttle_time_ms` on preferred **redirect**, not a Kafka client-quota. Probe is a short advertised `host:port` TCP connect (~75ms), not a broker-to-broker Fetch health check and not an async probe cache. See [V07_SPEC.md](./V07_SPEC.md).

## v0.14 Python client

Native **sync** TCP client in [`clients/python/`](../clients/python/) (import
`volant`). Not `kafka-python`; does not use `--kafka-listen`. Install with
`pip install -e "clients/python[dev]"` and run `pytest` (or
`scripts/python_client_smoke.sh`). Live e2e: `VOLANT_E2E=1` after
`cargo build -p volant-server`. See [V14_SPEC.md](./V14_SPEC.md).

## Shipped (not gaps)

Kafka wire shim **Phases 23–109** (ApiVersions **0–5**, Fetch **0–18**, ACL admin
**0–3** User resource, prepared 2PC MVP + prepared/open timeout + max clamp,
TRANSACTION_ABORTABLE honest subset after timeout, omit-unchanged sessions,
session idle TTL + max/LRU, background txn/session sweeper + expiry metrics
(always-spawn / 0→>0 live; graceful shutdown/join Phase 106; accept-loop drain +
single-flight bg Phase 109), BROKER
Describe/AlterConfigs + durable restart restore, empty-AddPartitions control
batches, ~38 keys; **ISR shrink on follower death** Phase 108 + **non-controller
alive-set auto-death** Phase 110 + **ISR rejoin + lag shrink** Phase 118 +
**time-based ISR lag** Phase 125;
**cluster admin fan-out** Phase 113), SCRAM-SHA-256/512,
SASL PLAIN/SCRAM — see [KAFKA_COMPAT.md](./KAFKA_COMPAT.md).
