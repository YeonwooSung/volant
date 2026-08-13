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
- `volant_fetch_session_promote_total` (Phase 138 mirror→primary after owner miss)
- `volant_fetch_sessions_mirrored` (Phase 138 gauge: foreign mirrors currently held)
- `volant_fetch_session_mirror_puts_coalesced_total` / `volant_fetch_session_mirror_stale_put_rejects_total` / `volant_fetch_session_promote_supersede_total` / `volant_fetch_session_mirror_restored` (Phase 139 coalesce / `mirror_gen` fence / durable restore)
- `volant_fetch_session_promote_claim_reject_total` (Phase 143 dual-promote / claim-lose rejects)
- `volant_preferred_replica_redirect_total` (Phase 126 PreferredReadReplica redirects)
- `volant_preferred_replica_suppressed_total` (Phase 140: READ_COMMITTED suppress when a preferred candidate existed)
- `volant_preferred_replica_session_suppressed_total` (Phase 144: preferred suppress when client has established fetch session)
- `volant_rack_aware_assignment_total` (Phase 145: create/create-partitions used multi-rack diversity placement)
- `volant_txn_forward_total` / `volant_txn_forward_errors_total` (Phase 120/122 Kafka txn API forward: EndTxn / AddOffsets / TxnOffsetCommit)
- `volant_txn_coordinator_registry_restored` / `volant_txn_coordinator_registry_persist_errors_total` (Phase 124 durable Init-owner registry)
- `volant_txn_coordinator_registry_gc_total` (Phase 127 registry TTL GC drops)
- `volant_journal_catchup_success_total` / `volant_journal_catchup_errors_total` (Phase 131 truncate journal rejoin catch-up)
- `volant_journal_catchup_skipped_total` (Phase 132 schedule skips: in-flight single-flight or min-interval throttle; env `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`, default 500ms, `0` disables time throttle)
- `volant_admin_catchup_skipped_total` (Phase 136 admin ACL/config catch-up schedule skips: in-flight or min-interval; env `VOLANT_ADMIN_CATCHUP_MIN_INTERVAL_MS`, default 500ms, `0` disables time throttle)
- `volant_delete_records_majority_wait_success_total` / `_fail_total` (Phase 135/137; only when **effective** wait is on — broker env `VOLANT_DELETE_RECORDS_WAIT_MAJORITY` and/or native request trailer `wait_majority=1`)
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
  correct while the owner is alive. **Shared mirror MVP (Phase 138 + polish 139 + claim 143):**
  owner best-effort fans out MirrorPut/Delete (opcodes 90–93) to live peers; peers
  hold a foreign mirror table (not served while owner alive). Owner death / forward
  fail: if a mirror is present, promote into primary and serve locally; else **70**.
  **Phase 139:** dirty ops coalesce to one pending op per `session_id` (Delete
  supersedes Put); Puts debounce via `VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS`
  (default **50**; `0` = coalesce only, flush immediately); Deletes flush immediately;
  optional durable peer mirrors `VOLANT_FETCH_SESSION_MIRROR_DURABLE=1` →
  `{data_dir}/__fetch_session_mirrors/state.json` (default **off**; load filters idle
  TTL); `mirror_gen` fences stale apply/promote. **Phase 143:** `promoted_by`
  lowest-id claim fence on equal-fresh dual-promote (claim travels in MirrorPut;
  metric `volant_fetch_session_promote_claim_reject_total`). Best-effort only (not
  Raft); put lag/fail still **70**; brief dual primary until MirrorPut claim
  exchange; session_id owner bits are not re-encoded. Sticky routing still preferred
  for latency (one extra RTT on forward when owner is up).
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
  --set image.tag=0.1.0
```

Deploys a StatefulSet, headless Service, and ConfigMap `cluster.toml`
(`node-id = ordinal + 1`). Single-node Deployment remains the default
(`cluster.enabled=false`).

## Protocol fuzz + corpus smoke (Phase 9 / 112)

Deterministic chaos tests always run with `cargo test -p volant-protocol`
(`chaos_decode_does_not_panic`, `chaos_frame_decode_extended`).

**Phase 112 CI path (stable, no nightly):**

```bash
cargo test --workspace --all-targets
cargo test -p volant-protocol corpus_smoke
# or
./scripts/fuzz_corpus_smoke.sh test
```

Seed corpus: `fuzz/corpus/{decode_frame,decode_request}/`. GitHub Actions:
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

**Optional local cargo-fuzz** (nightly; short capped runs only):

```bash
FUZZ_SMOKE_RUNS=200 ./scripts/fuzz_corpus_smoke.sh fuzz
```

Details: [fuzz/README.md](../fuzz/README.md), [PHASE112_SPEC.md](./PHASE112_SPEC.md).

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
| DeleteRecords | Truncates sealed segments; **best-effort fan-out at achieved low** (post whole-segment clamp) to other replicas (Phase 113) + **durable leader outbox retry** for offline/failed peers (`__delete_records_outbox`, Phase 116) + **new-leader reconcile from log_start** on leadership change (Phase 123; metric `volant_delete_records_outbox_reconcile_total`); **truncate journal** Phase 129/130 + **heartbeat lag rejoin catch-up** Phase 131 (`applied_journal_generation` → `TruncateJournalPush`) + **non-blocking/throttled schedule** Phase 132 (single-flight + `VOLANT_JOURNAL_CATCHUP_MIN_INTERVAL_MS`; metrics `volant_journal_catchup_*` incl. `_skipped_total`) + **peer-to-peer heartbeat mesh** Phase 134 (HB all configured peers; controller-only alive-set SoT); **optional client majority wait** Phase 135 (`VOLANT_DELETE_RECORDS_WAIT_MAJORITY` default **off**; when effective wait on, native error **15** / Kafka **19** `NOT_ENOUGH_REPLICAS` if journal majority fails; local low still returned — **no rollback**; metrics `volant_delete_records_majority_wait_*`); **Phase 137 native per-request trailer** `wait_majority` u8 after `before_offset` (`0`=broker default / absent trailer, `1`=force wait, `2`=force no-wait); client `delete_records_with_wait_flag`; CLI `volant topic delete-records … --wait-majority` / `--no-wait-majority`; **Kafka still env/broker knob only** (no per-request wire field); **Phase 137 journal GC:** assignment remove prunes topic watermarks; push apply filters to known topics (anti-resurrection); **TruncateJournalNote (86)** rejects negative/stale epochs / unknown TP (residual `phase132_journal_note_fence`; no orphan keys; fanout never stamps `-1`); enable ACL/auth for inter-broker 86/88 in production (residual `phase133_journal_auth`; current-epoch forge under weak auth remains); **GC/clip aborted soft markers** vs new log start (Phase 104/111) |
| Transactions (shipped) | **Write-through + soft markers** (Phase 86) + **control batches** on EndTxn finalize (Phase 89) + **empty AddPartitions control** (Phase 105) + **prepared 2PC MVP** (Phase 90) + **multi-broker Enable2Pc** (Phase 114: open/prepare/complete fan-out; controller `__txn_prepared/cluster.json`) + **EndTxn transparent forward** (Phase 120: Init-owner registry; non-coordinator Kafka EndTxn → opcodes 84/85) + **sticky FindCoordinator** (Phase 121: murmur2 over static membership; known txn → Init owner; preferred dead → next live) + **AddOffsets / TxnOffsetCommit forward** (Phase 122: same 84/85; offsets buffer only on coordinator) + **durable Init-owner registry** (Phase 124: `{data_dir}/__txn_coordinator`; restart restore) + **registry TTL GC** (Phase 127: drop by `last_touch` age; default **24h** / `VOLANT_TXN_COORDINATOR_TTL_MS`; `0` disables; metric `volant_txn_coordinator_registry_gc_total`) + **registry TTL BROKER config** (Phase 128: `volant.txn.coordinator.registry.ttl.ms`) + **prepared timeout** (Phase 92, default 60s / `VOLANT_PREPARED_TXN_TIMEOUT_MS`) + **open timeout** (Phase 93, InitProducerId / `VOLANT_OPEN_TXN_TIMEOUT_MS`) + **max timeout clamp** (Phase 96, default 15m / `VOLANT_TRANSACTION_MAX_TIMEOUT_MS`; Init **50** over-max) + **background sweeper** (Phase 97/101/106, default 1s / `VOLANT_SWEEP_INTERVAL_MS`; `0` = pause bg; always-spawn so 0→>0 without restart; graceful shutdown/join) + **BROKER config surface** (Phase 99) + **sparse durable restart** (Phase 100/102) + **BROKER name vs `node_id`** (Phase 103) + **marker GC/clip** (Phase 104/111); LSO/aborted filtering; open crash≡abort; prepared durable under `__txn_prepared` until complete or timeout; soft markers GC'd when `end_offset <= log_start`; straddlers clip `first_offset = log_start` (Phase 111). **Ops:** call **FindCoordinator** for group/txn keys (sticky); Init on the returned coordinator; EndTxn / AddOffsets / TxnOffsetCommit may hit any live broker once registration/open fan-out landed; produce still goes to partition leaders. **Sharp edge — registry TTL:** long-lived open txns that never re-touch (re-Init / note) can lose Init-owner mapping after TTL → FindCoordinator override / EndTxn forward fall back to hash ring only (wrong coordinator risk until re-Init). Set `VOLANT_TXN_COORDINATOR_TTL_MS=0` or lengthen TTL / Alter `volant.txn.coordinator.registry.ttl.ms`; clients should re-Init or otherwise re-note within TTL |
| mTLS | Feature `tls`; `--tls-client-ca` / optional `--tls-client-allow` |
| ACLs | `--acl-enable`; durable `__acls/acls.json`; User resource is Kafka admin store-only; **cluster:** Create/Delete controller-only + snapshot fan-out (Phase 113) + rejoin catch-up (Phase 117) + **non-blocking admin catch-up** (Phase 136) |
| Compaction | `cleanup.policy=compact` on **sealed** segments; empty value = tombstone |

**Cluster sharp edges:** Truncate-journal majority (Phase 130) and Phase 135/137 wait
mode use **configured N** (`floor(N/2)+1`), not live-only. For **N=2**, majority
is 2 — one peer down → permanent journal majority fail (local note may persist;
`consensus_fail` / wait `NotEnoughReplicas`). Prefer **odd N (3+)** for majority
journal / wait mode. Native clients may override the broker wait knob per request
(Phase 137); Kafka clients cannot.

**Phase 141 ops signal:** scrape
`volant_cluster_majority_impossible` (gauge 0/1) plus
`volant_cluster_configured_brokers` / `volant_cluster_live_brokers` /
`volant_cluster_majority_quorum`. Alert when `majority_impossible == 1` (or when
`live < quorum` for configured N). Single-node always reports impossible `0`.
Gauges are **local membership view** (death-detect lag can briefly disagree
across brokers).

CLI examples: [features.md](./features.md), [../README.md](../README.md).

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

## Still deferred

- Multi-language clients
- Full chaos-mesh suites / long fuzz campaigns (corpus **smoke CI MVP** → **closed by Phase 112**)
- Full KIP-890/939 / Kafka `__transaction_state` topic (multi-broker Enable2Pc MVP → **closed by Phase 114**)
- Multi-broker session affinity / durable sessions → **closed by Phase 115/119**; shared mirror + promote → **closed by Phase 138**; mirror polish (coalesce/debounce + optional durable + fence) → **closed by Phase 139** (best-effort residual: Raft registry / serve-without-promote / incremental put)
- Full preferred-replica selector / throttling residual (beyond 126/133/140/144; rack-aware create assignment → **closed by Phase 145**)
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
