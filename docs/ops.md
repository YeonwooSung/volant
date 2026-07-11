# Volant operations runbook (Phase 7–8)

## Process flags

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--listen` | | `0.0.0.0:9092` | Client/broker TCP listen |
| `--data-dir` | | `./data` | Segment + offset store root |
| `--metrics-addr` | | *disabled* | Prometheus `GET /metrics` |
| `--log-format` | | `text` | `text` or `json` |
| `--auth-token` | `VOLANT_AUTH_TOKEN` | *unset* | Shared-token auth |
| `--tls-cert` / `--tls-key` | | *unset* | Server TLS (feature `tls`) |
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

## TLS

```bash
cargo build -p volant-server --release --features tls
volant-server --features ... # binary already built with tls
  --tls-cert /etc/volant/server.crt \
  --tls-key  /etc/volant/server.key \
  --listen 0.0.0.0:9092
```

- Default builds **without** the `tls` feature stay green on macOS/CI.
- Passing `--tls-cert` without the feature errors at startup.
- TLS listen is **TLS-only** (no plaintext dual-bind).
- **Inter-broker remains plaintext** — place brokers on a private network.
- Client TLS: build `volant-client` with `--features tls` and set
  `ClientConfig { tls: true, tls_insecure: true, .. }` for lab/self-signed certs.
  Production cert verification against a custom CA is a follow-up.

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

## Deferred (not in Phase 7)

Kafka wire shim, multi-language clients, full Helm chart, SCRAM, mTLS identity,
chaos-mesh suites. See [PHASE7_SPEC.md](./PHASE7_SPEC.md) and [ROADMAP.md](../ROADMAP.md).
