# Phase 21 — Durable ACLs + metrics auth (binding)

## Goals

1. **Durable ACLs** — persist ACL entries under the broker data dir; survive restart
2. **Auto-save** — CreateAcls / DeleteAcls (and flag-driven enable/import) rewrite the store
3. **Metrics auth** — optional shared token on `GET /metrics` (Bearer)
4. Tests + docs honesty

## Non-goals

- Cluster-wide ACL consensus / multi-node replication of ACL file
- mTLS for the metrics HTTP endpoint
- Prometheus remote-write / basic-auth multi-user
- SCRAM / Kafka shim

## Durable ACL layout

```
{data_dir}/__acls/acls.json
```

```json
{
  "enabled": true,
  "entries": [
    {
      "principal": "alice",
      "resource_type": "Topic",
      "resource": "events",
      "operation": "Write",
      "permission": "Allow"
    }
  ]
}
```

- Atomic write (temp + rename), same pattern as topic catalog.
- Loaded on `Broker::new` / `with_cluster`.
- Super-users and `--auth-principal` remain **runtime flags** (not in the file).
- `--acl-file` still imports a JSON **array** (Phase 20) or full snapshot object; import enables + persists to `__acls/acls.json`.
- `--acl-enable` sets `enabled=true` and persists (even with zero entries → default deny).

## Metrics auth

| Flag | Meaning |
|------|---------|
| `--metrics-token` / `VOLANT_METRICS_TOKEN` | When set, `/metrics` requires auth |

Accepted request headers (case-insensitive name):

```
Authorization: Bearer <token>
Authorization: Token <token>
```

- Missing/wrong token → `401 Unauthorized` with `WWW-Authenticate: Bearer`
- Unset token → open scrape (Phase 7 behavior; still prefer bind localhost)
- Does **not** automatically reuse `--auth-token` (set both explicitly if desired)

## Exit criteria

1. CreateAcls then restart broker → ListAcls returns same entries; enforcement still on
2. DeleteAcls persists removal across restart
3. Metrics without token when configured → 401
4. Metrics with correct Bearer → 200 + volant_ body
5. Existing phase7 metrics smoke still passes (no token configured)
6. `cargo test --workspace` green

## Honest limitations

- Single-node file only (no raft/replication of ACLs)
- Super-users not durable (must pass flags each start)
- Metrics auth is shared-token only (no mTLS on metrics port)
- Metrics HTTP remains minimal (not a full HTTP stack)
