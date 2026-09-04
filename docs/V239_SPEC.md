# v0.239 — Native ListOffsets timestamp trailer

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Native **ListOffsets** (opcodes 48/49) accepts a timestamp.
Today `latest` is LEO and there is **no** timestamp. Kafka ListOffsets
(key 2) already has specials — **do not** change Kafka key 2.

This is residual **v0.239**. It is **not** a new Kafka API key. Native
opcodes **48/49** only. Keep the ScramFirst hash trailer from v0.238
intact. Do **not** touch ElectLeaders, DescribeLogDirs,
DescribeTopicPartitions, or `group.rs`.

## Goals

1. `Request::ListOffsets` optional `i64 timestamp_ms` trailer after
   existing fields.
2. Broker `list_offsets_at` implements `-1` / `-2` / `>= 0`. Other
   negatives are `InvalidArg`. Isolation is still **not** on native
   ListOffsets.
3. `>= 0` scans records (no time index). No match → latest LEO.
4. All four clients keep `list_offsets` as latest (`-1`) and add
   `list_offsets_at` (or equivalent).

## Non-goals

| Deferred | Why |
|----------|-----|
| New Kafka API keys | Frozen |
| Kafka ListOffsets key 2 | Already has specials |
| Time index | Scan records |
| Isolation on native ListOffsets | Still LEO / log start |
| SCRAM / ElectLeaders / DescribeLogDirs / DTP | Sibling leftovers |
| `group.rs` | Orthogonal |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (native ListOffsets)

```
string topic
u32    partition_count
u32    partitions[partition_count]
i64    timestamp_ms     // v0.239; omit on legacy
```

| `timestamp_ms` | Meaning |
|---------------:|---------|
| missing / -1 | latest = LEO (today) |
| -2 | earliest = log_start |
| >= 0 | first record with `timestamp_ms >= T`; if none, latest LEO |
| other negative | `InvalidArg` |

Response is unchanged: `(partition, earliest, latest)`. `earliest` is
always log start. `latest` is the resolved offset for the timestamp.

`ScramFirst.hash` trailer from v0.238 is unchanged.

## Clients

- Rust `Client::list_offsets` default timestamp **-1**. New
  `list_offsets_at(topic, partitions, timestamp_ms)`.
- Python / Go / Java: same. Existing helpers stay latest.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --test v239_list_offsets_timestamp -- --test-threads=1
```

- Protocol: missing trailer decodes -1; explicit -2 / 0 roundtrip.
  Existing ScramFirst hash=2 roundtrip must still pass.
- Broker: produce two records with distinct timestamps;
  `list_offsets_at` between them returns the second; -2 is log_start;
  -1 is LEO.
- Invalid other negative → error.
