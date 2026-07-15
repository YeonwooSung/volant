# Phase 37 — Kafka IncrementalAlterConfigs

## Goals

1. **IncrementalAlterConfigs** (API key 44, classic v0) on the Kafka shim
2. Map SET / DELETE ops onto Volant Phase 13 topic configs
3. Support `validate_only` without mutating state
4. Advertise in ApiVersions; tests + docs honesty

## Non-goals

- Flexible (compact) v1+
- BROKER / BROKER_LOGGER / CLIENT_METRICS resources (TOPIC only)
- APPEND / SUBTRACT ops (Volant has no list-typed topic configs)
- Synonyms / config sources beyond existing DescribeConfigs

## Wire (classic v0)

**Request:**

```
resources: [{
  resource_type: INT8,   # 2 = TOPIC
  resource_name: STRING,
  configs: [{
    name: STRING,
    config_operation: INT8,  # 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT
    value: NULLABLE_STRING
  }]
}]
validate_only: BOOLEAN
```

**Response:**

```
throttle_time_ms: INT32
responses: [{
  error_code: INT16,
  error_message: NULLABLE_STRING,
  resource_type: INT8,
  resource_name: STRING
}]
```

## Operation mapping

| Op | Kafka | Volant |
|----|-------|--------|
| 0 | SET | `alter_configs` with `(key, value)` |
| 1 | DELETE | `alter_configs` with `(key, "")` (clears) |
| 2 | APPEND | reject `INVALID_CONFIG` |
| 3 | SUBTRACT | reject `INVALID_CONFIG` |

Supported keys (unchanged from Phase 13/27):

- `retention.ms`
- `retention.bytes`
- `segment.bytes`
- `cleanup.policy` (`delete` \| `compact`)

## Authorization

Topic **Alter** when ACLs are enabled.

## Exit criteria

1. ApiVersions advertises 44 with max version 0
2. SET updates a topic config; DescribeConfigs reflects it
3. DELETE clears a key back to default
4. APPEND/SUBTRACT return InvalidConfig
5. `validate_only=true` does not persist
6. Non-TOPIC resource → InvalidRequest
7. ACL deny → TopicAuthorizationFailed
8. Tests green

## Honest limitations

- TOPIC resources only
- No APPEND/SUBTRACT (no list configs)
- No flexible versions
- No broker-wide configs
