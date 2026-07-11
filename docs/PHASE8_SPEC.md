# Phase 8 — Client polish, ops packaging, hardening (binding)

## Goals

Close the highest-value gaps left after Phase 7:

1. **Client leader redirect** — on `NotLeaderForPartition`, refresh metadata and **reconnect to the partition leader**
2. **Client TLS** — optional feature to talk to TLS-enabled brokers
3. **CLI auth** — global `--auth-token` / `VOLANT_AUTH_TOKEN`
4. **Helm chart** — minimal Kubernetes deploy under `deploy/helm/volant`
5. **Rolling restart test** — follower restart does not lose committed data / block produce
6. **Docs** — ROADMAP Phase 8, ops.md updates

## Non-goals

- Kafka wire shim, multi-lang clients, SCRAM, inter-broker TLS, full chaos mesh

## Client behavior

### Leader redirect

On produce/fetch `NotLeaderForPartition` (or Error with that code):

1. Call Metadata
2. Resolve leader broker host:port for the topic partition
3. Reconnect TCP (and re-Auth if token set; re-TLS if enabled)
4. Retry once (total 2 attempts)

If partition was `-1` (broker assign) and NotLeader returns a partition id, use that partition for redirect.

### TLS (`volant-client` feature `tls`)

- `ClientConfig.tls = true` enables rustls client
- `ClientConfig.tls_insecure = true` skips cert verification (dev/test only)
- Default build without feature: `tls=true` returns clear error

## CLI

```text
volant --auth-token SECRET topic list --broker 127.0.0.1:9092
# or VOLANT_AUTH_TOKEN
```

Global flag on root `Cli` struct.

## Helm

Minimal chart:

- Deployment (single replica default)
- Service (9092, optional 9102 metrics)
- ConfigMap for args
- values.yaml: image, authToken secret ref, resources

## Tests

- Integration: produce to wrong broker → client redirects to leader (3-node or 2-broker setup)
- Rolling restart: stop follower accept loop, restart, produce continues, data intact
- Existing workspace green without client `tls` feature

## Exit criteria

- [x] Leader redirect reconnects to leader host
- [x] Optional client TLS feature
- [x] CLI auth token
- [x] Helm chart files
- [x] Rolling restart integration test
- [x] Docs updated
