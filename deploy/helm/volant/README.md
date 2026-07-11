# Volant Helm chart (minimal)

Single-node broker for lab / small deployments.

```bash
# Build and load image first (see deploy/Dockerfile)
helm install volant ./deploy/helm/volant \
  --set authToken=s3cret \
  --set image.repository=volant \
  --set image.tag=0.1.0
```

Multi-node clustering is **not** fully automated by this chart (static
`cluster.toml` + multiple StatefulSet pods is a future improvement). Use
`examples/cluster.toml` and process supervisors for multi-node today.
