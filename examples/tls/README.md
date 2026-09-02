# TLS lab material (no secrets committed)

Generate a self-signed cert for local testing:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 365 \
  -subj "/CN=localhost"
```

Build and run:

```bash
# Server TLS also enables inter-broker TLS by default (Phase 9).
# Lab: peer cert verify is skipped (--tls-peer-insecure default true).
# Production peers: --tls-peer-insecure=false --tls-ca ./ca.crt
# Escape hatch: --no-tls-inter-broker
cargo run -p volant-server --features tls -- \
  --data-dir /tmp/vdata-tls \
  --listen 127.0.0.1:9092 \
  --tls-cert ./examples/tls/server.crt \
  --tls-key ./examples/tls/server.key
```

Do not commit `*.crt` / `*.key` files.

## Native multi-lang clients (v0.27)

Same certs work with the Python / Go / Java clients (plain TCP is still
the default). After the server is listening with `--tls-cert` /
`--tls-key`:

```python
from volant import Client
c = Client("127.0.0.1:9092", tls=True, tls_ca="server.crt")  # or a CA PEM
```

```go
c, err := volant.DialTLS("127.0.0.1:9092", volant.TLSConfig{CAFile: "server.crt"})
```

```java
try (Client c = Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("server.crt"))) {
  c.metadata();
}
```

Self-signed lab certs need a SAN for the dial host (`localhost` /
`127.0.0.1`). `tls_insecure` / `Insecure` / `TlsOptions.insecure()`
skips verify (tests only). Optional mTLS: pass a client cert + key PEM
pair. See [docs/V27_SPEC.md](../../docs/V27_SPEC.md).
