# Chaos Mesh operator artifacts (v0.15)

Operator-applied [Chaos Mesh](https://chaos-mesh.org/) experiments for a
Helm-installed Volant cluster. **Not run in GitHub Actions.** CI still uses
in-process isolate (`v05_ops_confidence`, `v15_asymmetric_isolate`).

## Prerequisites

1. A cluster with Chaos Mesh installed (`chaos-mesh` namespace / CRDs).
2. Volant Helm release with `cluster.enabled=true` (StatefulSet):

```bash
helm install volant ./deploy/helm/volant \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set image.repository=volant \
  --set image.tag=0.2.0
```

Default release name `volant` in namespace `default` produces:

| Pod | `node-id` | Role at boot |
|-----|-----------|--------------|
| `volant-0` | 1 | lowest-id controller (typical p0 leader) |
| `volant-1` | 2 | follower |
| `volant-2` | 3 | follower |

Selector labels (from `deploy/helm/volant/templates/_helpers.tpl`):

```
app.kubernetes.io/name: volant
app.kubernetes.io/instance: <release>
```

If the release or namespace differs, edit `metadata` / `spec.selector` /
`spec.target.selector` in the YAMLs (`pods.<namespace>` keys must match).

## Apply

```bash
# Kill one broker (volant-0). StatefulSet restarts the pod.
kubectl apply -f deploy/chaos/pod-kill-leader.yaml

# Asymmetric partition: volant-0 ↛ volant-1 (B→A and C stay open).
kubectl apply -f deploy/chaos/network-partition.yaml
```

`network-partition.yaml` sets `duration: 60s`. Delete the object to stop
early:

```bash
kubectl delete -f deploy/chaos/pod-kill-leader.yaml
kubectl delete -f deploy/chaos/network-partition.yaml
```

## Honesty

- These manifests are **not** a CI suite. They are starting points for a
  lab cluster. No GitHub Actions job applies them.
- `pod-kill-leader` targets `volant-0` (lowest-id controller). After
  failover the partition leader may be another pod — retarget
  `spec.selector.pods`.
- `network-partition` is **asymmetric** (`direction: to`). It is not a
  full island isolate (that is v0.5 / `mode: all` + `direction: both`).
- Chaos Mesh itself is out of process. The in-process counterpart is
  `crates/volant-broker/tests/v15_asymmetric_isolate.rs` (dest-specific
  `inter_broker_rpc` hook).
- Expected lab behavior matches that test: A→B drops; B→A and C stay
  open. Volant’s Phase 134 mesh marks a peer live on successful
  **outbound**, so B typically does **not** expire A. `acks=1` to a
  leader that still reaches a majority of ISR still appends. A full
  island (both directions) is v0.5 / `direction: both`.

See [docs/V15_SPEC.md](../../docs/V15_SPEC.md) and
[docs/ops.md](../../docs/ops.md).
