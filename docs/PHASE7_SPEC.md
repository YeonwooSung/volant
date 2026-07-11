# Phase 7 — Ecosystem & Production Readiness (binding)

## Goals

Make single-node and multi-node Volant operable with confidence:

1. **Observability** — Prometheus metrics + tracing spans on hot paths + JSON logs
2. **Security baseline** — optional TLS (rustls) + shared-token auth (SASL-style simplified)
3. **Packaging** — Docker image, systemd unit, docker-compose
4. **Hardening** — protocol parser robustness tests (fuzz-style random inputs)
5. **Ops docs** — runbook updates in README + deploy/

## Non-goals (explicitly deferred)

| Item | Why deferred |
|------|----------------|
| Full SASL PLAIN/SCRAM handshake | Token auth is enough for private networks; SCRAM later |
| Kafka wire protocol shim | Large surface; optional future crate |
| Multi-language clients / FFI | Rust client remains primary |
| Full Helm chart with operators | Docker + systemd cover common deploys; Helm stub optional |
| Full chaos mesh suite | Basic disk/protocol chaos tests only |
| mTLS client cert identity mapping | Optional stretch if time; default is server TLS + token |

## 1. Metrics

### Endpoint

- HTTP `GET /metrics` on `--metrics-addr` (default **disabled**; enable with e.g. `127.0.0.1:9102`)
- Prometheus **text exposition format 0.0.4**
- No auth on metrics by default (bind to localhost in production examples)

### Metric names (prefix `volant_`)

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `volant_produce_requests_total` | counter | `result` | Produce RPCs (ok/error) |
| `volant_produce_messages_total` | counter | | Messages appended |
| `volant_produce_bytes_total` | counter | | Approximate value bytes produced |
| `volant_fetch_requests_total` | counter | `result` | Fetch RPCs |
| `volant_fetch_messages_total` | counter | | Messages returned |
| `volant_fetch_bytes_total` | counter | | Approximate value bytes fetched |
| `volant_rpc_errors_total` | counter | `code` | Error responses by error_code |
| `volant_connections_accepted_total` | counter | | TCP accepts |
| `volant_messages_coalesced_total` | counter | | From existing broker metric |
| `volant_topics` | gauge | | Topic count |
| `volant_partitions` | gauge | | Partition count |
| `volant_build_info` | gauge | `version` | Always 1 |

Implement with `std::sync::atomic` + simple text renderer in `volant-broker` (no Prometheus client crate required). Optional later: `metrics` crate.

### Tracing

- Spans: `rpc` (opcode, correlation_id), `produce`, `fetch` on hot paths
- Use `tracing::info_span!` / `Instrument` where async

## 2. Structured logging

Server flags:

```text
--log-format text|json     # default text
RUST_LOG / tracing filter  # existing EnvFilter
```

JSON via `tracing_subscriber` `json` feature. Fields: timestamp, level, target, message, span fields.

## 3. Auth (shared token)

### Wire

New opcodes:

| Opcode | Name |
|--------|------|
| 30 | Auth request |
| 31 | Auth response |

```
# Auth request
token: string

# Auth response
error_code: u16   # 0 ok; 17 = AuthenticationFailed
```

Error code **17 = AuthenticationFailed**, **18 = AuthenticationRequired**.

### Server

- `--auth-token <secret>` or env `VOLANT_AUTH_TOKEN`
- When set: connections must successfully Auth **before** any other client opcode
- Inter-broker RPCs (ReplicaFetch, HeartbeatBroker, ClusterState): use same token when configured (client side of inter-broker includes Auth first)
- When unset: Auth is optional no-op (accept any / ignore)

### Client

- `ClientConfig.auth_token: Option<String>`
- On connect, if token set, send Auth before other calls

## 4. TLS

Feature `tls` on `volant-server` (+ client optional):

```text
--tls-cert <path.pem>
--tls-key <path.pem>
```

- Server: `tokio-rustls` + `rustls-pemfile`
- Client: `ClientConfig.tls = true` + optional custom CA later; Phase 7: `danger_accept_invalid_certs` only for tests, default webpki roots or skip client TLS in e2e and use plain TCP in tests
- **macOS default build without `tls` feature remains green**
- When TLS enabled without cert/key → error at startup

Inter-broker TLS: Phase 7 optional same certs; if TLS on listen, inter-broker also uses TLS to peers (document). If too heavy, inter-broker stays plaintext on private network and document the limitation honestly.

**Pragmatic choice:** TLS wraps client-facing accept path; inter-broker may remain plaintext in Phase 7 with ROADMAP note (common for internal VPC). Prefer implementing both if straightforward via shared connector helper.

## 5. Packaging

```
deploy/
  Dockerfile
  docker-compose.yml
  volant.service          # systemd
  README.md               # how to run
examples/
  tls/                    # optional gen script notes only (no committed secrets)
```

Dockerfile: multi-stage `cargo build --release -p volant-server`, distroless or debian-slim, expose 9092 and 9102.

## 6. Hardening tests

1. **Protocol chaos:** feed random / truncated / oversized payloads to `decode_frame` / `decode_request` / `decode_response` — must not panic
2. **Auth required:** without token → AuthenticationRequired; wrong token → AuthenticationFailed
3. **Metrics scrape:** start metrics server, produce once, `GET /metrics` contains counters
4. Existing workspace tests still pass without auth/tls/metrics

## 7. Docs

- This spec
- ROADMAP Phase 7 checkboxes (honest)
- README: metrics, auth, TLS, Docker
- `docs/ops.md` short runbook (metrics scrape, log format, token rotation note)

## Exit criteria

- [x] Prometheus `/metrics` with produce/fetch counters
- [x] JSON log format flag
- [x] Tracing spans on produce/fetch RPC path
- [x] Optional shared-token auth (wire + server + client)
- [x] Optional TLS feature (or documented reverse-proxy-only if blocked — prefer real TLS)
- [x] Docker + systemd + compose
- [x] Protocol non-panic chaos tests
- [x] `cargo test --workspace` green (default features, no TLS required)
- [x] Kafka shim / multi-lang / full Helm marked deferred honestly

## Workstreams

1. **protocol-auth** — opcodes 30/31, error codes 17/18
2. **metrics-tracing** — Metrics registry, HTTP server, spans, counters in dispatch
3. **auth-tls-server-client** — token gate, TLS feature, client support
4. **packaging-docs** — deploy/*, PHASE7, ROADMAP, README, ops.md
5. **hardening-tests** — chaos + auth + metrics integration tests
