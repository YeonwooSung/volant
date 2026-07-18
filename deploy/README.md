# Deploying Volant

Phase 7–8 packaging: Docker image, docker-compose, systemd unit, and a minimal Helm chart.

## Docker

```bash
# From repository root
docker build -f deploy/Dockerfile -t volant:local .
docker run --rm -p 9092:9092 -p 9102:9102 \
  -v volant-data:/var/lib/volant/data volant:local
```

With TLS (requires certs mounted):

```bash
docker build -f deploy/Dockerfile --build-arg FEATURES=tls -t volant:tls .
docker run --rm -p 9092:9092 \
  -v /path/to/certs:/certs:ro \
  -v volant-data:/var/lib/volant/data volant:tls \
  --data-dir /var/lib/volant/data \
  --listen 0.0.0.0:9092 \
  --tls-cert /certs/server.crt \
  --tls-key /certs/server.key
```

## docker-compose

```bash
cd deploy
docker compose up --build -d
curl -s localhost:9102/metrics | head
```

## systemd

1. Install the `volant-server` binary to `/usr/local/bin/volant-server`
2. Create user/group `volant` and data dir `/var/lib/volant/data`
3. Install `volant.service` to `/etc/systemd/system/`
4. `systemctl daemon-reload && systemctl enable --now volant`

## Helm

```bash
# Build/push image first
docker build -f deploy/Dockerfile -t volant:0.1.0 .

# Single-node (default)
helm install volant ./deploy/helm/volant \
  --set image.repository=volant \
  --set image.tag=0.1.0 \
  --set authToken=s3cret

# Multi-node StatefulSet (Phase 9)
helm install volant ./deploy/helm/volant \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set image.repository=volant \
  --set image.tag=0.1.0
```

See [helm/volant/README.md](./helm/volant/README.md).

## Auth

Set `VOLANT_AUTH_TOKEN` (or `--auth-token`) on the server. Clients must set
`ClientConfig.auth_token` (or CLI equivalent when available).

## Metrics

`GET /metrics` on `--metrics-addr` (Prometheus text 0.0.4). Open by default;
optional Bearer via `--metrics-token` / `VOLANT_METRICS_TOKEN` (Phase 21). Prefer
binding to `127.0.0.1` in production when unauthenticated.

## TLS notes

- Build with `--features tls` and pass `--tls-cert` + `--tls-key` (PEM).
- When TLS is enabled the broker listens **TLS-only** (no dual plain/TLS).
- **Inter-broker TLS** (Phase 9) is enabled by default with server TLS
  (`--tls-peer-insecure` for lab; `--tls-ca` + `--tls-peer-insecure=false` for
  verified peers; `--no-tls-inter-broker` to force plaintext peers).
- Client: `webpki-roots` + optional `tls_ca` / `tls_insecure` on `ClientConfig`.

See [docs/ops.md](../docs/ops.md) for the operator runbook.
