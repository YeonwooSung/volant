# Phase 7 Implementation Plan

## Design (locked from PHASE7_SPEC)

Observability + security baseline + packaging for operable single/multi-node Volant.
No Kafka shim, multi-lang clients, full Helm, SCRAM, or chaos mesh.

## Layers

### A. Protocol (`volant-protocol`)
- ErrorCode: AuthenticationFailed=17, AuthenticationRequired=18
- Opcodes: Auth request=30, Auth response=31
- Request::Auth { token }, Response::Auth { error_code }
- encode/decode + unit roundtrip
- Chaos tests: random/truncated/oversized → no panic

### B. Metrics + tracing (`volant-broker`)
- `metrics.rs`: atomic counters + `render_prometheus()`
- Broker holds `Arc<Metrics>`
- Gauges for topics/partitions computed at scrape time
- HTTP `GET /metrics` via raw tokio TcpListener (no axum)
- Spans: `rpc`, `produce`, `fetch` on dispatch hot paths

### C. Auth (`volant-broker` net + client)
- Connection `authenticated: bool`
- When `auth_token` set: require Auth before other ops
- Wrong token → 17; missing → 18
- Inter-broker short-lived RPCs: Auth first when token configured
- Client: `ClientConfig.auth_token`; Auth on connect

### D. Logging + server flags (`volant-server`)
- `--log-format text|json`
- `--metrics-addr` (disabled if unset)
- `--auth-token` / `VOLANT_AUTH_TOKEN`
- Feature `tls`: `--tls-cert` `--tls-key` (tokio-rustls)
- Default build without `tls` stays green on macOS

### E. Packaging
```
deploy/Dockerfile
deploy/docker-compose.yml
deploy/volant.service
deploy/README.md
docs/ops.md
examples/tls/ (notes only)
```

### F. Tests
- Protocol chaos + Auth roundtrip
- Metrics smoke (in-process HTTP)
- Auth success/failure over TCP
- `cargo test --workspace` green without `--features tls`

## Implementation order

1. Protocol auth + chaos
2. Metrics module
3. Net: auth gate, metrics record, spans, metrics HTTP, inter-broker Auth
4. Broker: Arc<Metrics>, topic/partition counts, auth_token
5. Server flags + JSON log + optional TLS
6. Client auth_token
7. Packaging + docs
8. Integration tests + review

## Pragmatic choices

- Inter-broker remains **plaintext** even when client TLS is on (document in ops.md)
- TLS-only listen when certs provided (no dual plain/TLS)
- Metrics unauthenticated; bind localhost in production examples
- No Prometheus client crate — atomic + text renderer
