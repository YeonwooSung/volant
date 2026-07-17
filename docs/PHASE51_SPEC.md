# Phase 51 — Flexible wire foundation + ApiVersions v3

## Goals

1. Add **KIP-482 flexible/compact** codec primitives (unsigned varint, compact
   string/array, tag buffer)
2. Support **ApiVersions v3** (first flexible API on the shim)
3. Advertise ApiVersions max **3**; tests + docs honesty

## Non-goals

- Flexible versions of other APIs (Metadata v9+, Produce v9+, Fetch v12+, …)
- Response header v1 (tag buffer) — ApiVersions response header stays **v0**
- SupportedFeatures / FinalizedFeatures tagged fields (empty tag buffer)
- Full flexible request-header helper for all APIs (only ApiVersions v3 path)

## Flexible primitives

| Type | Encoding |
|------|----------|
| UNSIGNED_VARINT | 7-bit continuation (no zig-zag) |
| COMPACT_STRING | `uvarint(len+1) + bytes` (`0` = null for nullable) |
| COMPACT_ARRAY | `uvarint(n+1) + elements` (`0` = null) |
| TAG_BUFFER | `uvarint(count)` + `[tag, len, bytes]…` |

Request header note: **ClientId is always classic** nullable string, even for
flexible requests; header ends with TAG_BUFFER (RequestHeader v2).

## ApiVersions v3

### Request (flexible header + body)

```
# header: classic ClientId + TAG_BUFFER
ClientSoftwareName: COMPACT_STRING
ClientSoftwareVersion: COMPACT_STRING
TAG_BUFFER
```

Software name/version are parsed and ignored.

### Response (header v0 + flexible body)

```
correlation_id: INT32              # response header v0 only
error_code: INT16
api_keys: COMPACT_ARRAY[{
  api_key, min, max,
  TAG_BUFFER                       # empty
}]
throttle_time_ms: INT32
TAG_BUFFER                         # empty (no features)
```

## Exit criteria

1. Codec unit tests: unsigned varint, compact string/array, flexible request header
2. ApiVersions advertises max **3**
3. ApiVersions v3 round-trip: compact keys + trailing throttle + empty tags
4. ApiVersions v0–2 still work (phase50)
5. ApiVersions v4 → UnsupportedVersion
6. phase51 tests green

## Honest limitations

- Only ApiVersions is flexible so far (closed for other APIs by later phases)
- No SupportedFeatures / feature flags (still true after Phase 83 empty tags)
- Other APIs remain classic-only at Phase 51 ship; later phases add flexible
  Metadata etc.

**Superseded max:** Phase 83 raised ApiVersions to **0–5** (v4–5 flexible;
still empty feature tags; response header still v0).
