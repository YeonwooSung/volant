# v0.240 — Native ListOffsets isolation trailer

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Native **ListOffsets** (opcodes 48/49) accepts **isolation**.
v0.239 added timestamp; leftover is “no isolation”. Kafka ListOffsets
(key **2**) already has isolation — **do not** change Kafka key 2.

This is residual **v0.240**. It is **not** a new Kafka API key. Native
opcodes **48/49** only. Keep the v0.239 timestamp trailer intact. Do
**not** add Kafka keys. Do **not** touch quotas, UnregisterBroker,
UpdateFeatures, `__metadata_raft`, or `group.rs` join/state.

## Goals

1. `Request::ListOffsets` optional `u8 isolation` after the v0.239
   `timestamp_ms` i64 trailer.
2. Broker: isolation `1` + timestamp `-1` (latest) returns LSO via
   `Broker::last_stable_offset`. Isolation `0` / missing stays LEO.
3. Isolation other than `0` / `1` is `InvalidArg`.
4. Clients default isolation `0`. Existing `list_offsets` /
   `list_offsets_at` stay uncommitted. Add `list_offsets_committed` /
   `list_offsets_at(..., isolation)` helpers.

## Non-goals

| Deferred | Why |
|----------|-----|
| New Kafka API keys | Frozen |
| Kafka ListOffsets key 2 | Already has isolation |
| Time index | Scan records (v0.239) |
| Quotas / UnregisterBroker / UpdateFeatures | Orthogonal |
| `__metadata_raft` / `group.rs` | Orthogonal |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (native ListOffsets)

```
string topic
u32    partition_count
u32    partitions[partition_count]
i64    timestamp_ms     // v0.239; omit on legacy
u8     isolation        // v0.240; omit on legacy
```

| `isolation` | Meaning |
|------------:|---------|
| missing / 0 | READ_UNCOMMITTED (latest = LEO) |
| 1 | READ_COMMITTED: timestamp `-1` latest = LSO |
| other | `InvalidArg` |

`timestamp_ms` decode is unchanged:

| `timestamp_ms` | Meaning |
|---------------:|---------|
| missing / -1 | latest (LEO, or LSO when isolation is 1) |
| -2 | earliest = log_start |
| >= 0 | first record with `timestamp_ms >= T`; if none, latest. Isolation 1 caps at LSO |
| other negative | `InvalidArg` |

Response is unchanged: `(partition, earliest, latest)`. `earliest` is
always log start. `latest` is the resolved offset.

Legacy payloads without the `u8` stay isolation 0. Isolation `0` is
omitted on encode so default clients stay v0.239-compatible on the wire.

## Clients

- Rust `Client::list_offsets` / `list_offsets_at` stay isolation **0**.
  New `list_offsets_committed` (isolation 1, timestamp `-1`) and
  `list_offsets_at_isolated(..., isolation)`.
- Python / Go / Java: same. Existing helpers stay uncommitted.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --test v240_list_offsets_isolation -- --test-threads=1
```

- Protocol: no isolation byte → 0; explicit 1 roundtrip.
- Broker: open txn produce; uncommitted latest > committed latest (LSO).
- Isolation 2 → error.
