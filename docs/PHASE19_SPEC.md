# Phase 19 — mTLS identity mapping (binding)

## Goals

1. **Mutual TLS** — server can require and verify client certificates
2. **Identity mapping** — map verified client cert subject CN → connection principal
3. **Auth gate** — verified (and allowlisted) client cert authenticates the connection
   without a shared Auth token
4. **Client cert presentation** — `ClientConfig` TLS client cert/key
5. **Inter-broker** — when mTLS is on, peers present the server cert as client identity
6. Docs honesty

## Non-goals

- Full X.509 RBAC / ACLs per topic
- SPIFFE / URI SAN principal schemes (CN only for MVP)
- SCRAM / SASL
- Kafka shim / multi-lang clients

## Server flags (`volant-server --features tls`)

| Flag | Meaning |
|------|---------|
| `--tls-cert` / `--tls-key` | Server identity (existing) |
| `--tls-client-ca <pem>` | **Enable mTLS**: require client certs signed by this CA |
| `--tls-client-allow <list>` | Optional comma-separated CN allowlist (empty = any verified client) |
| `--tls-ca` | Inter-broker / documented client trust (existing) |

When `--tls-client-ca` is set:

1. rustls `WebPkiClientVerifier` requires a client certificate chain
2. After handshake, extract **CN** from the leaf client cert (fallback: first DNS SAN)
3. If allowlist is non-empty and CN ∉ allowlist → not authenticated via mTLS
   (Auth token may still succeed if configured)
4. If allowlist empty or CN allowed → `authenticated = true`, principal = CN
5. Auth opcode still accepted (token path unchanged)

## Client (`ClientConfig`, feature `tls`)

| Field | Meaning |
|-------|---------|
| `tls_cert` | Client certificate PEM path |
| `tls_key` | Client private key PEM path |

When both are set with `tls: true`, the client presents the cert during handshake.

## Inter-broker

When server TLS + mTLS (`--tls-client-ca`) and inter-broker TLS is on:

- `InterBrokerTls` gains optional `client_cert` / `client_key`
- Server sets them to its own `--tls-cert` / `--tls-key` so peers can complete mTLS
- Lab clusters: sign the server cert with the same client CA (or use a dual-purpose CA)

## Principal

Logged at connection accept: `principal=<cn>`. Not yet attached to produce ACLs
(deferred). Available for ops correlation.

## Exit criteria

1. Client without cert fails handshake when `--tls-client-ca` is set
2. Client with cert signed by CA can produce/fetch without Auth token
3. Allowlist rejects unknown CN (Auth required / denied without token)
4. Inter-broker still works with mTLS when peers present server cert
5. `cargo test --workspace` green (default features)
6. `cargo test -p volant-client --features tls --test phase19_mtls` green
7. `cargo check -p volant-server --features tls` green

## Honest limitations

- Principal is CN (or first DNS SAN) only — no SPIFFE / email SAN
- No per-topic ACL enforcement from principal yet
- Metrics endpoint still unauthenticated
- Inter-broker uses server cert as client cert (no separate peer identity file)
