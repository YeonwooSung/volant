# Phase 83 — ApiVersions v4–5 (Kafka max)

## Goals

1. Raise **ApiVersions** max from **0–3** to **0–5** (Apache Kafka max)
2. Accept flexible **v4** and **v5** with honest wire framing
3. Response header stays **v0** for all ApiVersions versions (Kafka special case)
4. Empty feature tagged fields (no SupportedFeatures / FinalizedFeatures /
   ZkMigrationReady registry)
5. v0–3 paths unchanged
6. v6 → UnsupportedVersion with response header **v0**
7. Tests + docs honesty

## Non-goals

- Real feature negotiation (`SupportedFeatures` / `FinalizedFeatures`)
- Emitting `ZkMigrationReady` or KRaft migration state
- ClusterId / NodeId identity checks or `REBOOTSTRAP_REQUIRED` (KIP-1242)
- Fetch v14+ / multi-lang clients / cargo-fuzz / READ_COMMITTED / 2PC

## Wire summary

Apache Kafka documents ApiVersions **validVersions 0–5**, **flexibleVersions 3+**:

> Version 3 is the first flexible version and adds ClientSoftwareName and
> ClientSoftwareVersion.
>
> Version 4 fixes KAFKA-17011, which blocked SupportedFeatures.MinVersion in
> the response from being 0.
>
> Version 5 introduces ClusterId and NodeId checking and REBOOTSTRAP_REQUIRED
> error (KIP-1242).

Response note (Kafka): tagged fields are only supported in the **body**; the
response **header** length must not change (stays correlation-only / v0).

### Request

```
# v0–2: empty body
# header for v3+: classic ClientId + TAG_BUFFER (RequestHeader v2)

# v3–4 body:
ClientSoftwareName: COMPACT_STRING,       # parsed, ignored
ClientSoftwareVersion: COMPACT_STRING,    # parsed, ignored
TAG_BUFFER

# v5 body:
ClientSoftwareName: COMPACT_STRING,
ClientSoftwareVersion: COMPACT_STRING,
ClusterId: COMPACT_NULLABLE_STRING,       # parsed, ignored (default null)
NodeId: INT32,                            # parsed, ignored (default -1)
TAG_BUFFER
```

### Response (header v0 + flexible body for v3–5)

```
correlation_id: INT32              # response header v0 only — never v1
error_code: INT16
api_keys: COMPACT_ARRAY[{
  api_key, min, max,
  TAG_BUFFER                       # empty
}]
throttle_time_ms: INT32            # always 0 on Volant
TAG_BUFFER                         # empty (no feature tags 0–3)
```

**v4 delta vs v3:** none on Volant. Kafka v4 only fixes serialization of
`SupportedFeatures` entries with `MinVersion = 0`. Volant emits no feature
arrays, so the body is wire-identical to v3.

**v5 delta vs v4:** request-only (ClusterId + NodeId). Response body unchanged.
Volant never returns `REBOOTSTRAP_REQUIRED`.

## Semantics (honest)

| Case | Behavior |
|------|----------|
| v4 success | Same compact keys + empty tags as v3 |
| v5 success (any ClusterId/NodeId) | Same as v4; fields ignored |
| Feature tags | Always empty (no registry) |
| Classic v0–2 / flex v3 | Unchanged |
| v6+ | Header v0 + UnsupportedVersion (35) |

## Exit criteria

1. ApiVersions advertises max **5**
2. ApiVersions **v4** flexible round-trip (header v0, empty feature tags)
3. ApiVersions **v5** flexible round-trip with ClusterId/NodeId (success always)
4. ApiVersions **v3** still works
5. ApiVersions **v0** classic still works
6. ApiVersions **v6** → header v0 + UnsupportedVersion (35)
7. phase50 / phase51 max assertions updated; phase83 green
8. ROADMAP / README / ops / KAFKA_COMPAT / WHITEPAPER / PHASE_HISTORY / INDEX honesty

## Honest limitations

- No SupportedFeatures / FinalizedFeatures / FinalizedFeaturesEpoch /
  ZkMigrationReady (empty top-level TAG_BUFFER only)
- No REBOOTSTRAP_REQUIRED / cluster-id or node-id identity checks (KIP-1242)
- ThrottleTimeMs always 0
- Response header always v0 (correct Kafka special case)
- No real feature negotiation beyond the ApiKeys table
