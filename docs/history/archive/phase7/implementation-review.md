# Phase 7 Implementation Review

## Evidence

### Test results

```
cargo test --workspace   # default features, no tls required
```

**Result: all green** (2026-07-11 worktree run)

Notable new tests:

| Suite | Tests | Notes |
|-------|-------|-------|
| `volant-protocol` unit | +3 | Auth roundtrip, auth error codes, chaos_decode_does_not_panic |
| `volant-broker` unit | +1 | metrics render_contains_volant_prefix |
| `volant-broker` integration `phase7_metrics_auth` | 6 | metrics HTTP smoke, auth required/wrong/success/disabled |
| Prior workspace suites | all pass | cluster_failover, e2e_tcp/group, storage, stream, … |

```
cargo check -p volant-server --features tls   # also green
```

### Exit criteria checklist (PHASE7_SPEC)

- [x] Prometheus `/metrics` with produce/fetch counters (`volant_*` prefix)
- [x] JSON log format flag (`--log-format text|json`)
- [x] Tracing spans on produce/fetch RPC path (`rpc`, `produce`, `fetch`)
- [x] Optional shared-token auth (wire 30/31, errors 17/18, server + client)
- [x] Optional TLS feature (`tls` on volant-server; rustls)
- [x] Docker + systemd + compose (`deploy/`)
- [x] Protocol non-panic chaos tests
- [x] `cargo test --workspace` green without `--features tls`
- [x] Kafka shim / multi-lang / full Helm marked deferred honestly

## Files touched

### Protocol
- `crates/volant-protocol/src/request.rs` — Auth opcode 30
- `crates/volant-protocol/src/response.rs` — Auth 31, ErrorCode 17/18
- `crates/volant-protocol/src/payload.rs` — encode/decode + tests + chaos

### Broker
- `crates/volant-broker/src/metrics.rs` — **new** atomic Prometheus renderer
- `crates/volant-broker/src/broker.rs` — Arc\<Metrics\>, auth_token, topic/partition counts
- `crates/volant-broker/src/net.rs` — auth gate, metrics HTTP, spans, inter_broker Auth
- `crates/volant-broker/src/replica/follower.rs` — use inter_broker_rpc (Auth-aware)
- `crates/volant-broker/src/lib.rs` — exports
- `crates/volant-broker/tests/phase7_metrics_auth.rs` — **new**

### Client
- `crates/volant-client/src/config.rs` — `auth_token`
- `crates/volant-client/src/client.rs` — Auth on connect, `connect_with_auth`

### Server
- `crates/volant-server/src/main.rs` — flags, JSON logs, metrics spawn, TLS module
- `crates/volant-server/Cargo.toml` — features `tls`, deps

### Workspace / packaging / docs
- `Cargo.toml` — clap env, tracing-subscriber json
- `deploy/Dockerfile`, `docker-compose.yml`, `volant.service`, `README.md`
- `docs/ops.md`, `docs/PHASE7_SPEC.md` (pre-existing), `docs/phase7/*`
- `examples/tls/README.md`
- `README.md`, `ROADMAP.md`, `.gitignore`

## Design notes

1. **Metrics** use `std::sync::atomic` only; HTTP is raw tokio TCP (no axum).
2. **Auth** is connection-scoped; Auth opcode always allowed; other opcodes gated when token set.
3. **Inter-broker** short-lived connections Auth first when token configured; remain **plaintext** even under client TLS.
4. **TLS** is feature-gated; when certs provided, listen is TLS-only. Default macOS build has no rustls link requirement for tests.
5. **Gauges** `volant_topics` / `volant_partitions` computed at scrape time from broker state.

## Gaps / deferred

| Item | Status |
|------|--------|
| Kafka wire shim | Deferred (documented) |
| Multi-lang clients | Deferred |
| Full Helm chart | Deferred (Docker+systemd only) |
| SCRAM / full SASL | Deferred (shared token only) |
| mTLS client identity | Deferred |
| Chaos mesh / disk full | Deferred (protocol chaos only) |
| Client TLS connector | Not shipped (server TLS only; clients still plaintext TCP) |
| Dual plain+TLS bind | Not shipped (TLS-only when enabled) |
| Metrics auth | Not shipped (bind localhost) |
| CLI `--auth-token` flag | Not added (Rust client config only; env works for server) |
| `cargo fuzz` CI | Deferred (deterministic chaos unit test instead) |

## Residual risks

- TLS accept path duplicates the auth gate (calls shared `dispatch_request` for non-Auth); keep in sync if gate rules change.
- Produce bytes metric uses approximate value lengths recorded separately from request counters.
- Inter-broker plaintext under cluster+TLS is intentional; operators must isolate the cluster network.
