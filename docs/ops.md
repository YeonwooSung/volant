# Volant operations runbook (Phase 7–9)

## Process flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--listen` | | `0.0.0.0:9092` | Client/broker TCP listen |
| `--data-dir` | | `./data` | Segment + offset store root |
| `--metrics-addr` | | *disabled* | Prometheus `GET /metrics` |
| `--log-format` | | `text` | `text` or `json` |
| `--auth-token` | `VOLANT_AUTH_TOKEN` | *unset* | Shared-token auth |
| `--scram-user USER:PASS` | | *unset* | Upsert SCRAM user at startup (repeatable; Phase 22) |
| `--kafka-listen` | | *disabled* | Kafka wire protocol shim (Phase 23–42) |
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

## Kafka wire shim (Phase 23–24)

Optional second socket speaking Kafka framing (classic + selected flexible APIs).
Native Volant protocol remains on `--listen`.

```bash
cargo run -p volant-server -- \
  --data-dir ./data \
  --listen 127.0.0.1:9092 \
  --kafka-listen 127.0.0.1:9093
```

Supported APIs:

| API | Versions | Notes |
|-----|----------|-------|
| ApiVersions | 0–3 | v0–2 classic; **v3 flexible** (compact api_keys + tag buffers); software name/version ignored; no feature tags; response header always v0 |
| Metadata | 0–9 | Classic 0–8; **v9 flexible** (compact brokers/topics + response header v1); cluster_id=`volant`; leader_epoch=-1; TopicId v10+ unsupported |
| OffsetForLeaderEpoch | 0–3 | End offset by leader epoch; no epoch history (eligible → HWM); fencing via current_leader_epoch |
| Produce | 0–9 | Classic 0–8; **v9 flexible** compact transactional_id/topics/records + response header v1; MessageSet or RecordBatch; compression + idempotent PID/seq; empty record_errors; v10 KIP-951 unsupported |
| Fetch | 0–12 | Classic 0–11; **v12 flexible** compact topics/records + response header v1; v0–3 MessageSet + v4+ RecordBatch (default lz4); session header (no real sessions); leader-epoch fence; preferred_read_replica=-1; TopicId v13+ unsupported |
| InitProducerId | 0–1 | plain + transactional_id fencing; timeout ignored |
| FindCoordinator | 0–4 | Classic 0–2; **v3 flexible** compact key/host; **v4 batch** CoordinatorKeys; all keys → this broker; response header v1 for v3+ |
| AddPartitionsToTxn | 0–2 | opens txn (Kafka has no BeginTxn); v1–2 wire-identical to v0 |
| AddOffsetsToTxn | 0–2 | registers group for transactional offsets; v1–2 = v0 |
| EndTxn | 0–2 | commit/abort; flushes buffered produces + offsets; v1–2 = v0 |
| TxnOffsetCommit | 0–2 | buffers offsets until EndTxn commit; v2+ leader_epoch ignored |
| SaslHandshake | 0–1 | mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 |
| SaslAuthenticate | 0–1 | PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 against Volant SCRAM store |
| ListOffsets | 0–5 | -1 latest / -2 earliest; v2+ isolation (LSO≡HWM) + throttle; v4+ leader-epoch fencing; flexible v6+ unsupported |
| CreateTopics | 0–4 | partition count; RF/assignment ignored; error_message v1+; throttle v2+; validate_only; v4 partitions=-1 → 1 |
| DeleteTopics | 0–3 | by name; throttle v1+ (leading) |
| JoinGroup | 0–9 | Classic 0–5; **v6+ flexible** + response header v1; v5+ group.instance.id → static:{id}; **v7** ProtocolType; **v8** Reason (ignored); **v9** SkipAssignment=false; throttle v2+ |
| SyncGroup | 0–5 | Classic 0–3; **v4+ flexible** + response header v1; v3 group.instance.id; **v5** ProtocolType/Name echo (no consistency check) |
| Heartbeat | 0–4 | Classic 0–3; **v4 flexible** + response header v1; v3 group.instance.id |
| LeaveGroup | 0–5 | Classic 0–3; **v4+ flexible** + response header v1; v3 batch members; **v5** Reason (ignored) |
| OffsetCommit | 0–8 | Classic 0–7; **v8 flexible** compact topics + response header v1; durable `__consumer_offsets`; throttle v3+; leader epoch ignored; group.instance.id v7+; v9 KIP-848 unsupported |
| OffsetFetch | 0–7 | Classic 0–5; **v6–7 flexible** compact topics + response header v1; null=all; top-level error; leader_epoch=-1; RequireStable ignored (v7); multi-group v8+ unsupported |
| ListGroups | 0–2 | active + offset-backed groups; throttle v1+ |
| DescribeGroups | 0–4 | state + members; throttle v1+; authorized_ops v3+; group_instance_id v4+ (from `static:`) |
| DeleteGroups | 0–1 | empty groups only (`NON_EMPTY_GROUP` if live); throttle all versions |
| CreatePartitions | 0–1 | total partition count; throttle all versions; validate_only dry-run |
| DescribeConfigs | 0–3 | TOPIC resources; throttle; v1+ config_source + empty synonyms; v3+ type/docs |
| AlterConfigs | 0–1 | TOPIC resources; throttle all versions; validate_only |
| IncrementalAlterConfigs | 0 | SET/DELETE on TOPIC keys |
| DeleteRecords | 0–1 | whole sealed segments only (Phase 14) |
| DescribeAcls | 0–1 | filter → Volant ACL list |
| CreateAcls | 0–1 | maps Kafka types/ops; enables ACL store |
| DeleteAcls | 0–1 | filter match → exact delete |
| OffsetDelete | 0 | group offset delete (Phase 12) |

Topic config keys: `retention.ms`, `retention.bytes`, `segment.bytes`,
`cleanup.policy` (`delete`|`compact`).

Limitations:

- Compression: **Produce** accepts compressed RecordBatch (gzip/snappy/lz4/zstd)
  and compressed MessageSet wrappers (gzip/snappy/lz4). **Fetch** re-encodes
  with `VOLANT_KAFKA_FETCH_COMPRESSION` (default **lz4**;
  `none`/`gzip`/`snappy`/`lz4`/`zstd`). MessageSet has no zstd — env `zstd`
  maps to lz4 for v0–3. Log storage remains plain.
- Idempotent Produce requires RecordBatch magic 2 + InitProducerId; MessageSet
  cannot carry PID/sequence.
- Kafka transactions: InitProducerId(`transactional_id`) + AddPartitionsToTxn
  opens a txn; Produce buffers until EndTxn commit/abort; TxnOffsetCommit
  offsets apply only on commit. No control markers / `READ_COMMITTED` filtering;
  crash ≡ abort open txns.
- Kafka SASL: **PLAIN**, **SCRAM-SHA-256**, and **SCRAM-SHA-512** (no GSSAPI /
  OAUTHBEARER). New users store both SHA-256 and SHA-512 credentials from one
  password. Legacy single-credential users (pre–Phase 34) are SHA-256 only until
  re-upsert. When SCRAM users exist (`--scram-user` / `volant user create`),
  SASL is **required** before other APIs. Shared-token Auth does not apply on
  the Kafka port. Principal after SASL = username (feeds ACLs).
- Flexible (compact) Kafka versions: **ApiVersions v3 only** so far (KIP-482
  primitives). All other APIs remain classic; clients must negotiate classic
  max versions for Produce/Fetch/admin/group APIs.
- Consumer assignment is **coordinator-driven** (not Kafka leader assignor).
- CreateTopics / CreatePartitions ignore Kafka replica assignment arrays.
- DescribeConfigs is TOPIC-only (no broker configs).
- FindCoordinator host/port is the Volant advertised address (often `--listen`).
- When ACLs are enabled and SASL is unused, the shim principal is `kafka-anonymous`.
- **DeleteRecords** only drops whole sealed segments (same as native Phase 14).
- **ACL admin** maps Kafka resource types (Topic=2, Group=3, Cluster=4),
  operations, and permission types to Volant Phase 20/21 ACLs. Principals strip
  / re-add `User:`; cluster resource name `kafka-cluster` ⇄ `volant`. Host is
  always `*`; only LITERAL patterns. CreateAcls enables enforcement — after that
  Cluster Alter/Describe is required for further ACL admin (or use a super-user).
- **OffsetDelete** maps to Phase 12 `delete_offsets` (listed partitions only;
  empty topic list is a no-op, not delete-all). Requires Group Delete when ACLs
  are on.
- **IncrementalAlterConfigs** (44): SET/DELETE on TOPIC Volant keys; APPEND/SUBTRACT rejected; `validate_only` supported.
- **Fetch isolation** (`READ_UNCOMMITTED` / `READ_COMMITTED`): uncommitted
  transactional data never hits the log (buffer-until-commit), so LSO always
  equals HWM and `aborted_transactions` is always empty. No control markers.
- Prefer binding to localhost / private networks; leave disabled in production
  unless you need Kafka-protocol discovery.

See [PHASE23_SPEC.md](./PHASE23_SPEC.md) … [PHASE51_SPEC.md](./PHASE51_SPEC.md).

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
- Optional: `GET /metrics` returns `200` with `volant_build_info`
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

## Protocol fuzz (optional)

Deterministic chaos tests always run with `cargo test -p volant-protocol`.
Optional nightly: see [fuzz/README.md](../fuzz/README.md).

## Idempotent produce & retries (Phase 10)

Rust client:

```rust
ClientConfig {
    enable_idempotence: true,
    max_retries: 3,
    retry_backoff_ms: 50,
    ..Default::default()
}
```

- On first produce, the client calls `InitProducerId` and tracks per-partition sequences.
- Exact retries of the same batch return the same offsets (no double-append).
- Producer state is **durable** under `{data_dir}/__producer_state/state.json` (Phase 11).
  Broker restart reloads PIDs so duplicate sequences still de-dupe.

## Consumer lag (Phase 10)

```bash
volant group lag --group my-group --broker 127.0.0.1:9092
# optional: --topic events
```

Metrics (when `--metrics-addr` is set):

```
volant_consumer_group_lag{group="my-group",topic="events",partition="0"} 12
```

## Group describe (Phase 11)

```bash
volant group describe --group my-group --broker 127.0.0.1:9092
```

Shows live members, subscribed topics, and partition assignments. Empty /
unknown groups return NotFound.

Rebalance uses a **sticky** assignor by default (minimize ownership churn).

## Cooperative rebalance (Phase 17)

On re-JoinGroup after a generation bump, consumers keep in-memory fetch
positions for partitions they still own and only OffsetFetch newly assigned
partitions. JoinGroup responses include a trailing **`revoked`** list
(partitions lost since the member's last join).

`GroupConsumer` applies this automatically; CLI group consume prints
`revoked=[...]` on join.

Not Kafka cooperative-sticky (no two-phase revoke barrier).

## Group list & delete offsets (Phase 12)

```bash
volant group list --broker 127.0.0.1:9092
volant group delete-offsets --group my-group --broker 127.0.0.1:9092
# optional single partition:
volant group delete-offsets --group my-group --topic events --partition 0
```

`list` shows live (**Stable**) and offset-only (**Empty**) groups.

## Static membership (Phase 12)

Pass a stable `group_instance_id` on join (Rust: `join_group_with_instance` /
`GroupConsumer::join_static`). The broker assigns `member_id = static:{id}` so
redeploys rejoin the same member without an extra generation bump when still
in-session.

## Topic configs & retention (Phase 13)

```bash
volant topic create events --partitions 4 \
  --retention-ms 86400000 \
  --retention-bytes 1073741824 \
  --segment-bytes 268435456

volant topic describe events
volant topic config get events
volant topic config set events --key retention.ms --value 3600000
volant topic config set events --key retention.ms --value ''   # clear
```

Keys: `retention.ms`, `retention.bytes`, `segment.bytes`. Stored under
`{data_dir}/__topic_configs/`. Broker applies retention about every 5 seconds.

## Durable topics & delete-records (Phase 14)

Single-node topic metadata is stored under `{data_dir}/__topics/catalog.json`.
After a broker restart, topics and partition logs reload automatically (no need
to re-create topics). Multi-node continues to use `cluster/assignment.json`.

```bash
# Drop sealed segments entirely before offset N on partition P
volant topic delete-records events --partition 0 --before-offset 1000
```

DeleteRecords only truncates **whole sealed segments** (same as storage
`delete_records`). On a multi-node cluster it runs on the leader only; followers
are not notified (use retention for cluster-wide cleanup).

## Create partitions & list offsets (Phase 15)

```bash
# Grow a topic to 8 partitions (must be greater than current)
volant topic add-partitions events --total 8

# Earliest (log start) and latest (LEO) per partition
volant topic offsets events
volant topic offsets events --partition 0
```

Multi-node: `add-partitions` must hit the **controller**. New partitions start
empty (no data redistribution).

## Transactions (Phase 18)

Multi-partition atomic produce with a transactional id:

```bash
volant txn produce --transactional-id app-1 \
  --topic events --partition 0 --value a \
  --topic2 events --partition2 1 --value2 b
```

Rust:

```rust
let mut tp = TransactionalProducer::connect(vec!["127.0.0.1:9092".into()], "app-1").await?;
tp.begin().await?;
tp.produce("events", Some(0), msgs).await?;
tp.add_offsets("cg", vec![("events".into(), 0, next_offset)]);
let results = tp.commit().await?; // or tp.abort().await?
```

Produces inside a txn are **buffered off-log** until commit (abort leaves no
records). Broker crash aborts open txns. Not Kafka control-marker EOS.

## mTLS identity (Phase 19)

Build with TLS and require client certificates signed by a CA:

```bash
cargo run -p volant-server --features tls -- \
  --listen 0.0.0.0:9092 \
  --tls-cert server.crt --tls-key server.key \
  --tls-client-ca client-ca.crt \
  --tls-client-allow alice,bob   # optional CN allowlist
```

- Verified client cert **CN** (else first DNS SAN) becomes the connection principal
  and authenticates the connection (no shared Auth token required).
- Empty / omitted `--tls-client-allow` accepts any client cert signed by the CA.
- Auth opcode / shared token still work when configured (either path may authenticate).
- Inter-broker TLS automatically presents the server cert as the client identity
  when mTLS is on — sign server certs with the same client CA in lab clusters
  (or use a dual-purpose CA).

Rust client:

```rust
let client = Client::connect(ClientConfig {
    brokers: vec!["127.0.0.1:9092".into()],
    tls: true,
    tls_ca: Some("ca.crt".into()),
    tls_cert: Some("client.crt".into()),
    tls_key: Some("client.key".into()),
    ..ClientConfig::default()
})
.await?;
```

Principal is logged for ops correlation and used by **Phase 20 ACLs**.
Metrics remain unauthenticated.

## Principal ACLs (Phase 20)

Enable authorization (default deny when on):

```bash
cargo run -p volant-server -- \
  --listen 0.0.0.0:9092 \
  --auth-token secret \
  --auth-principal alice \
  --acl-enable \
  --acl-super-users admin \
  --acl-file acls.json   # optional JSON array; implies enable
```

Example `acls.json`:

```json
[
  {
    "principal": "alice",
    "resource_type": "Topic",
    "resource": "events",
    "operation": "Write",
    "permission": "Allow"
  },
  {
    "principal": "alice",
    "resource_type": "Topic",
    "resource": "events",
    "operation": "Read",
    "permission": "Allow"
  }
]
```

CLI:

```bash
volant --auth-token secret acl create \
  --principal alice --resource-type Topic --resource events \
  --operation Write --permission Allow

volant --auth-token secret acl list
volant --auth-token secret acl delete \
  --principal alice --resource-type Topic --resource events \
  --operation Write --permission Allow
```

- Matching **Deny** beats **Allow**; no match → deny when enabled.
- Super-users bypass all checks (runtime flag; not stored in the ACL file).
- Token Auth sets principal to `--auth-principal` (default `token`).
- mTLS CN is the principal when using Phase 19 client certs.
- SCRAM sets principal to the SCRAM username (Phase 22).
- ACLs are durable under `{data_dir}/__acls/acls.json` (Phase 21); CreateAcls /
  DeleteAcls persist automatically. `--acl-file` imports then saves there.
- Inter-broker opcodes are not ACL-gated.

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

## Log compaction (Phase 16)

```bash
volant topic create kv --partitions 1 \
  --cleanup-policy compact \
  --segment-bytes 1048576

volant topic config set kv --key cleanup.policy --value compact
volant topic config set kv --key cleanup.policy --value delete
```

When `cleanup.policy=compact`, the broker periodically rewrites **sealed**
segments keeping the latest value per key. An **empty value** is a tombstone
(removes the key). Null-key records are not compacted away. The active segment
is only compacted after it rolls.

## Deferred

Kafka wire shim, multi-language clients, SCRAM / full SASL, full chaos-mesh
suites, cargo-fuzz corpus CI. See [ROADMAP.md](../ROADMAP.md).
