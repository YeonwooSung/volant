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
| `--kafka-listen` | | *disabled* | Kafka wire protocol shim (Phases 23–86) |
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

## Kafka wire shim

Optional second socket speaking Kafka framing (classic + flexible). Native
Volant protocol remains on `--listen`. API versions and honesty notes live in
**[KAFKA_COMPAT.md](./KAFKA_COMPAT.md)** (source of truth; Phases 23–86).

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
- **Topic config keys** (Describe/AlterConfigs): `retention.ms`, `retention.bytes`,
  `segment.bytes`, `cleanup.policy` (`delete`|`compact`).
- **Transactions / isolation:** write-through + soft abort markers (Phase 86);
  `READ_COMMITTED` caps at LSO and filters aborted; `READ_UNCOMMITTED` sees all.
  Crash promotes open ranges to aborted via `__txn_markers`.
- **ACLs:** Kafka ACL admin maps to Volant Phase 20/21 ACLs (LITERAL only;
  CreateAcls enables enforcement). Describe/Create/DeleteAcls **0–3**: v3 accepts
  Kafka **User** resource type (stored as `ResourceType::User`; not used on the
  produce/fetch authorize path; no SCRAM-admin gating).

Deep dives: [PHASE23_SPEC.md](./PHASE23_SPEC.md) … [PHASE86_SPEC.md](./PHASE86_SPEC.md).

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

## Protocol fuzz (optional)

Deterministic chaos tests always run with `cargo test -p volant-protocol`.
Optional nightly: see [fuzz/README.md](../fuzz/README.md).

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
| DeleteRecords | Truncates sealed segments; no follower fan-out |
| Transactions (shipped) | **Write-through + soft markers** (Phase 86): LSO/aborted filtering; crash ≡ abort open ranges; not Kafka control batches |
| mTLS | Feature `tls`; `--tls-client-ca` / optional `--tls-client-allow` |
| ACLs | `--acl-enable`; durable `__acls/acls.json`; User resource is Kafka admin store-only |
| Compaction | `cleanup.policy=compact` on **sealed** segments; empty value = tombstone |

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
- Full chaos-mesh suites / cargo-fuzz **corpus CI** (scaffold under `fuzz/` only)
- Kafka control batches on the data log / real 2PC / prepared transactions
- Durable leader-epoch history; real fetch sessions

Full list: [ROADMAP.md](../ROADMAP.md).

## Shipped (not gaps)

Kafka wire shim **Phases 23–86** (ApiVersions **0–5**, Fetch **0–18**, ACL admin
**0–3** User resource, ~38 keys), SCRAM-SHA-256/512, SASL PLAIN/SCRAM — see
[KAFKA_COMPAT.md](./KAFKA_COMPAT.md).
