# Phase 9 — TLS hardening, multi-node deploy, fuzz (binding)

## Goals

1. **Client TLS verification** via Mozilla roots (`webpki-roots`) + optional custom CA PEM path
2. **Inter-broker TLS** when the server listens with TLS (feature `tls`)
3. **Multi-node Helm** StatefulSet (3 brokers, static cluster.toml via ConfigMap)
4. **Protocol fuzz harness** (`fuzz/` cargo-fuzz targets + expanded chaos tests)
5. Docs honesty in ROADMAP / ops.md

## Non-goals

- Kafka wire shim, multi-lang clients, SCRAM, mTLS mutual identity

## Client TLS

| Config | Meaning |
|--------|---------|
| `tls: true` | Use rustls |
| `tls_insecure: true` | Skip cert verify (lab) |
| `tls_ca: Some(path)` | Trust extra CA PEM (and webpki roots) |
| neither insecure nor CA with public certs | webpki-roots only |

## Inter-broker TLS

When server has `--tls-cert`/`--tls-key`:

- Default: inter-broker also uses TLS
- `--tls-peer-insecure` (default **true** for self-signed lab clusters) skips peer verify
- `--tls-ca <pem>` loads CA for peer verification when peer-insecure is false
- `--no-tls-inter-broker` forces plaintext inter-broker (escape hatch)

## Helm multi-node

`deploy/helm/volant` gains:

- `cluster.enabled: true` → StatefulSet replicas=3, headless Service, ConfigMap `cluster.toml`
- node-id = ordinal+1
- Single-node Deployment path remains default

## Fuzz

```
fuzz/
  Cargo.toml
  fuzz_targets/decode_request.rs
  fuzz_targets/decode_frame.rs
```

`cargo +nightly fuzz run decode_frame` when cargo-fuzz installed.
Workspace always has expanded deterministic chaos tests (no nightly required).

## Exit criteria

1. Client TLS verifies with webpki-roots; optional `tls_ca` loads extra PEMs
2. Server TLS enables inter-broker TLS by default; peer-insecure lab path works
3. Helm `cluster.enabled=true` renders StatefulSet + headless Service + ConfigMap
4. `cargo test --workspace` green without `tls` feature
5. `cargo check -p volant-server --features tls` and `-p volant-client --features tls` green
6. Protocol chaos tests expanded; `fuzz/` scaffold present
