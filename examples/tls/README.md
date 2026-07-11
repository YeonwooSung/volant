# TLS lab material (no secrets committed)

Generate a self-signed cert for local testing:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt -days 365 \
  -subj "/CN=localhost"
```

Build and run:

```bash
cargo run -p volant-server --features tls -- \
  --data-dir /tmp/vdata-tls \
  --listen 127.0.0.1:9092 \
  --tls-cert ./examples/tls/server.crt \
  --tls-key ./examples/tls/server.key
```

Do not commit `*.crt` / `*.key` files.
