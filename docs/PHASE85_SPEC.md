# Phase 85 — ACL admin v3 (User resource type; Kafka max)

## Goals

1. Raise **DescribeAcls** / **CreateAcls** / **DeleteAcls** max from **0–2** to **0–3**
2. Accept flexible **v3** with the same wire framing as **v2** (compact + empty tags)
3. Accept Kafka **User** resource type (`ResourceType = 7`) on v3 only
4. Persist User ACLs in the Phase 20/21 store (`ResourceType::User`)
5. Response header **v1** for v2–3 (already true for v ≥ 2)
6. Classic 0–1 and flexible v2 paths unchanged for Topic/Group/Cluster
7. **v4** → UnsupportedVersion with response header **v1**
8. Tests + docs honesty

## Non-goals

- TransactionalId / DelegationToken resource types
- Host-dimension ACL matching or PREFIXED patterns
- Using User ACLs to gate SCRAM credential admin on the Kafka port
  (no DescribeUserScramCredentials / AlterUserScramCredentials APIs)
- Changing produce/fetch authorize paths to consult User resources
- Multi-lang clients / cargo-fuzz CI / true READ_COMMITTED / 2PC
- Durable OffsetForLeaderEpoch history

## Wire summary

Apache Kafka documents Describe/Create/DeleteAcls **v3** as:

> Version 3 adds support for the User resource type.

### Framing (flexible v2+)

**CreateAcls request** (v2 and v3 identical):

```
Creations: COMPACT_ARRAY[{
  ResourceType: INT8,          # 2 Topic / 3 Group / 4 Cluster / 7 User (v3+)
  ResourceName: COMPACT_STRING,
  ResourcePatternType: INT8,   # LITERAL (3) only
  Principal: COMPACT_STRING,
  Host: COMPACT_STRING,        # ignored; always stored as *
  Operation: INT8,
  PermissionType: INT8,
  TAG_BUFFER
}],
TAG_BUFFER
```

**CreateAcls response** (header v1):

```
ThrottleTimeMs: INT32,
Results: COMPACT_ARRAY[{
  ErrorCode: INT16,
  ErrorMessage: COMPACT_NULLABLE_STRING,
  TAG_BUFFER
}],
TAG_BUFFER
```

**DescribeAcls** / **DeleteAcls** likewise share v2 framing; only the
allowed `ResourceType` values change at v3.

**v3 delta vs v2:** none on the wire. Brokers that advertise max 3 accept
`ResourceType = 7` (User). Volant still rejects TransactionalId (5) and
DelegationToken (6) at every version.

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v3 Create User | Store `ResourceType::User` + resource name; principal strip `User:` |
| v3 Describe User | Return matching User ACLs with wire type 7 |
| v3 Delete User | Filter-match and remove; echo matching ACLs |
| v3 Topic/Group/Cluster | Same as v2 |
| v2 Create User | InvalidRequest (42) — User requires v3+ |
| v0–1 | Unchanged; no User resource |
| Host / pattern | Host ignored; LITERAL only (ANY on filters) |
| Authorize path | User ACLs are **not** consulted for produce/fetch/group |
| v4+ | Header v1 + UnsupportedVersion (35) |

## Exit criteria

1. ApiVersions: Describe/Create/DeleteAcls **0–3**
2. CreateAcls **v3** User resource round-trips into durable store
3. DescribeAcls **v3** returns User type 7 + principal/host/op/perm
4. DeleteAcls **v3** removes User ACLs and echoes matching entries
5. CreateAcls **v2** with User type → InvalidRequest
6. CreateAcls **v3** Topic still works
7. ACL **v4** → header v1 + UnsupportedVersion (35)
8. phase64 max assertions updated; phase35/64 still green
9. ROADMAP / README / ops / KAFKA_COMPAT / WHITEPAPER / PHASE_HISTORY / INDEX honesty

## Honest limitations

- User ACLs are storage + admin round-trip only; no SCRAM credential API gating
- No TransactionalId / DelegationToken resource types
- Host always `*`; LITERAL patterns only
- No cluster ACL consensus
- DeleteRecords max remains 2
