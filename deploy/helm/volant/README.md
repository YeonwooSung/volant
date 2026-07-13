# Volant Helm chart

Single-node **Deployment** by default; multi-node **StatefulSet** when
`cluster.enabled=true` (Phase 9).

## Single-node

```bash
# Build and load image first (see deploy/Dockerfile)
helm install volant ./deploy/helm/volant \
  --set authToken=s3cret \
  --set image.repository=volant \
  --set image.tag=0.1.0
```

## Multi-node cluster

```bash
helm install volant ./deploy/helm/volant \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set cluster.defaultReplicationFactor=3 \
  --set cluster.minInsyncReplicas=2 \
  --set image.repository=volant \
  --set image.tag=0.1.0 \
  --set authToken=s3cret
```

What this deploys:

| Resource | Purpose |
|----------|---------|
| StatefulSet | `replicas` pods; `node-id = ordinal + 1` |
| Headless Service | Stable DNS: `{name}-{i}.{name}-headless` |
| ConfigMap | Generated `cluster.toml` for all pods |
| Service (ClusterIP) | Client-facing VIP (load-balanced) |

Pods start with a shell wrapper that derives `--node-id` from the hostname
ordinal and sets `--advertised-host` to the headless DNS name.

### TLS in cluster

Build the image with `--build-arg FEATURES=tls`, mount PEMs, and pass via
`extraArgs` (and matching client `tls_ca` / `tls_insecure` as appropriate):

```yaml
extraArgs:
  - --tls-cert=/certs/server.crt
  - --tls-key=/certs/server.key
  # lab self-signed:
  - --tls-peer-insecure=true
```

Inter-broker TLS is on by default whenever server TLS is enabled
(see [docs/ops.md](../../../docs/ops.md)).
