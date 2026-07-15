# Volant operations runbook (Phase 7–9)

## Process flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--listen` | | `0.0.0.0:9092` | Client/broker TCP listen |
| `--data-dir` | | `./data` | Segment + offset store root |
| `--metrics-addr` | | *disabled* | Prometheus `GET /metrics` |
| `--log-format` | | `text` | `text` or `json` |
| `--auth-token` | `VOLANT_AUTH_TOKEN` | *unset* | Shared-token auth |
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

Principal is logged for ops correlation; it is **not** yet enforced on per-topic
ACLs. Metrics remain unauthenticated.

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
suites, cargo-fuzz corpus CI.
See [ROADMAP.md](../ROADMAP.md).
