# Phase 20 — Principal-based ACLs (binding)

## Goals

1. **Authorization** — enforce allow/deny ACLs against the connection principal
2. **Principal sources** — mTLS CN (Phase 19); shared-token Auth → configurable principal
3. **Admin wire API** — CreateAcls / DeleteAcls / ListAcls
4. **Bootstrap** — `--acl-file`, `--acl-enable`, `--acl-super-users`, `--auth-principal`
5. Client + CLI + tests + docs honesty

## Non-goals

- Kafka ACL wire parity / ZooKeeper store
- Prefix/literal pattern types beyond exact + `*` wildcard
- SCRAM / SASL
- Per-IP or network ACLs
- Durable ACL replication across cluster (file load is local; CreateAcls is memory + optional rewrite)

## Model

```
AclEntry {
  principal: String,       // exact CN / token principal, or "*"
  resource_type: Topic | Group | Cluster
  resource: String,        // name or "*"
  operation: All | Read | Write | Create | Delete | Describe | Alter | ClusterAction
  permission: Allow | Deny
}
```

### Matching

1. If ACLs **disabled** → allow all (backward compatible).
2. Super-users always allowed.
3. Inter-broker opcodes (ReplicaFetch, HeartbeatBroker, ClusterState) skip ACL checks.
4. A rule matches when principal, resource_type, resource, and operation match
   (`*` / `All` wildcards).
5. Any matching **Deny** → deny.
6. Else any matching **Allow** → allow.
7. Else → **deny** (default deny when enabled).

### Request → operation map

| Request | Resource | Operation |
|---------|----------|-----------|
| Produce | Topic name | Write |
| Fetch, ListOffsets | Topic name | Read |
| CreateTopic | Topic name | Create |
| DeleteTopic, DeleteRecords | Topic name | Delete |
| Metadata (specific) | each Topic | Describe |
| Metadata (all) | Cluster `volant` | Describe |
| DescribeConfigs | Topic | Describe |
| AlterConfigs, CreatePartitions | Topic | Alter |
| OffsetCommit/Fetch, Join/Heartbeat/Leave | Group id | Read |
| DescribeGroup | Group id | Describe |
| DeleteOffsets | Group id | Delete |
| ListGroups | Cluster `volant` | Describe |
| InitProducerId, BeginTxn, EndTxn | Cluster `volant` | Write |
| CreateAcls, DeleteAcls | Cluster `volant` | Alter |
| ListAcls | Cluster `volant` | Describe |
| Auth | — | (no ACL) |

## Protocol

| Dir | Opcode | Name |
|-----|--------|------|
| Req/Resp | 54/55 | CreateAcls |
| Req/Resp | 56/57 | DeleteAcls |
| Req/Resp | 58/59 | ListAcls |

Error code **23** = `AuthorizationFailed`.

### AclEntry wire

```
principal: string
resource_type: u8   # 0=Topic 1=Group 2=Cluster
resource: string
operation: u8       # 0=All 1=Read 2=Write 3=Create 4=Delete 5=Describe 6=Alter 7=ClusterAction
permission: u8      # 0=Deny 1=Allow
```

### CreateAcls / DeleteAcls

```
count: u32
  entries: AclEntry × count
```

Response: `error_code: u16`.

Delete removes **exact** matching entries.

### ListAcls

```
filter_principal: string   # empty = any
filter_resource_type: u8   # 255 = any
filter_resource: string    # empty = any
```

Response: `error_code` + `count` + entries.

## Server flags

| Flag | Meaning |
|------|---------|
| `--acl-enable` | Turn on enforcement (default deny) |
| `--acl-file <path>` | Load JSON ACL array at startup; implies enable |
| `--acl-super-users <list>` | Comma-separated principals bypass ACLs |
| `--auth-principal <name>` | Principal after successful token Auth (default `token`) |

### ACL file JSON

```json
[
  {
    "principal": "alice",
    "resource_type": "Topic",
    "resource": "events",
    "operation": "Write",
    "permission": "Allow"
  }
]
```

## Principal on connection

- mTLS: CN / DNS SAN (Phase 19); already authenticated.
- Token Auth success: set principal to `--auth-principal`.
- Neither: principal `None` → denied when ACLs enabled.

## Exit criteria

1. With ACLs enabled and no allow rule, produce/fetch fails with AuthorizationFailed
2. Matching Allow lets the operation through
3. Deny overrides Allow
4. Super-user bypasses
5. CreateAcls / ListAcls / DeleteAcls round-trip
6. Token Auth principal is used for authorization
7. `cargo test --workspace` green
8. Docs honesty

## Honest limitations

- In-memory ACL store (optional file load; CreateAcls does not auto-persist unless rewritten)
- No cluster-wide ACL consensus
- No resource pattern types beyond `*`
- Metadata-all uses Cluster Describe only (not per-topic filter)
- Inter-broker traffic is not ACL-gated
