# v0.27 — TLS for Python, Go, and Java native clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python / Go / Java clients have no TLS”
by wrapping the existing sync TCP sockets with optional TLS, matching
the Rust `volant-client` knobs as closely as each stdlib allows.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change broker TLS (Phase 8/19).

## Goals

1. **Python** `Client(..., tls=True, tls_ca=..., tls_insecure=...,
   tls_cert=..., tls_key=...)` via stdlib `ssl`.
2. **Go** `DialTLS(addr, TLSConfig{CAFile, Insecure, CertFile, KeyFile})`
   via `crypto/tls`.
3. **Java** `Client.connectTls(host, port, TlsOptions.ca("ca.pem"))`
   via `SSLSocket`.
4. **Plain TCP default unchanged.** `tls=false` / `Dial` /
   `Client.connect` stay plaintext.
5. **mTLS** when `tls_cert` + `tls_key` (PEM) are both set.
6. **Unit tests** wrap a local TLS server and generate ephemeral certs
   in-process (`cryptography` or `openssl` / Go `crypto/x509` / JDK
   `keytool`). Skip the TLS class if the generator is missing.
7. **Optional e2e** against `volant-server --tls-cert --tls-key` gated
   on `VOLANT_E2E=1` (skip if the binary was not built with `--features
   tls`).

## Non-goals

| Deferred | Why |
|----------|-----|
| Broker TLS / rustls / Phase 19 mTLS mapping | Already shipped; do not touch |
| Kafka API keys / `--kafka-listen` TLS | Native clients only |
| System-root-only “required CA” hard-fail | Rust also has webpki-roots; `tls_ca` is how you trust a private CA |
| Hostname override APIs beyond Go `ServerName` | Dial host is enough |
| Async / reconnect / leader redirect over TLS | Same single-connection MVP |
| Required CI language job | Existing optional smoke scripts only |

## Knobs (align with Rust `ClientConfig`)

| Knob | Rust | Python | Go | Java |
|------|------|--------|----|------|
| Enable | `tls: true` | `tls=True` | `DialTLS` | `connectTls` |
| Skip verify | `tls_insecure` | `tls_insecure` | `TLSConfig.Insecure` | `TlsOptions.insecure()` |
| Extra / private CA PEM | `tls_ca` | `tls_ca` | `TLSConfig.CAFile` | `TlsOptions.ca(path)` |
| Client cert PEM | `tls_cert` | `tls_cert` | `TLSConfig.CertFile` | `TlsOptions.clientCert` |
| Client key PEM | `tls_key` | `tls_key` | `TLSConfig.KeyFile` | (paired with cert) |

`tls_cert` and `tls_key` must both be set or both unset. Handshake
failures close the TCP socket.

When `tls_ca` is set, Python and Go **add** the PEM to the default
system / Mozilla-style trust store (same idea as rustls +
`webpki-roots` + optional `tls_ca`). Java `TlsOptions.ca` uses **only**
that PEM as the trust store (JDK has no cheap “append to cacerts”
helper). `tls_insecure` skips verification on all three (tests / lab
only). With neither `tls_ca` nor insecure, the language default roots
are used.

## API

```python
Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem", tls_insecure=False)
```

```go
DialTLS("127.0.0.1:9092", TLSConfig{CAFile: "ca.pem"})
```

```java
Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"));
```

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

TLS unit tests need no broker. They generate a throwaway CA + server
(+ client) cert, listen on `127.0.0.1:0`, and run Metadata over the
wrapped socket.

| Language | Cert source |
|----------|-------------|
| Python | `cryptography` if installed, else `openssl` on PATH; skip if neither |
| Go | `crypto/x509` + `ecdsa.P256` in-process |
| Java | JDK `keytool` PKCS12, then export PEM; skip if `keytool` missing |

Live `volant-server --tls-*` is `VOLANT_E2E=1` only. The binary must be
built with `--features tls`; otherwise the e2e test skips.

## Honesty leftovers

- Not a Kafka TLS story. Native port only.
- Does not change `volant-server` / `volant-client` Rust TLS.
- Java private keys: PKCS#8 (`BEGIN PRIVATE KEY`) and RSA PKCS#1
  (`BEGIN RSA PRIVATE KEY`) only. No encrypted keys, no PKCS#12 input
  on the public API (files are PEM, matching Rust).
- Java `TlsOptions.ca` replaces the JVM default trust store rather than
  appending (documented).
- Hostname verification uses the dial host (Python `server_hostname`,
  Go `ServerName` / `tls.Client`, Java HTTPS endpoint identification).
  Self-signed lab certs need a SAN for `127.0.0.1` / `localhost`.
- `tls_insecure` is a test/lab escape hatch, same as Rust.
- No SCRAM / shared-token Auth on these clients (unchanged leftover).

See [ops.md](./ops.md) (`## v0.27 client TLS`) and
[examples/tls/README.md](../examples/tls/README.md).
