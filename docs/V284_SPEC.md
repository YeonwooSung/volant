# v0.284 — Kafka DescribeShareGroupOffsets key 90 v0 reject

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeShareGroupOffsets** (API key **90**,
version **0** only, always flexible). This is **not** KIP-932 share
offsets. Parse the official v0 body and reject each requested group
with **42** `INVALID_REQUEST` (`not KIP-932 share offsets`). Do **not**
persist. Do **not** wrap OffsetFetch **9** / `describe_group` /
ConsumerGroupDescribe **69** / ShareGroupDescribe **77**.

This is residual **v0.284**, not Phase 155. Official Apache Kafka
`DescribeShareGroupOffsetsRequest.json` uses apiKey **90**. Official
`validVersions` is **0–1** (v1 adds Lag / KIP-1226). Volant advertises
**v0 only**. Official response has `throttleTimeMs` and **per-group**
error after `Topics[]` (no top-level error besides throttle). Closer
to ShareGroupDescribe **77** than InitializeShareGroupState **83**.

## Goals

1. Advertise `(ApiKey::DescribeShareGroupOffsets, 0, 0)` in
   `SUPPORTED_APIS` (numeric order after InitializeShareGroupState
   **83**, before UnregisterController **94**). Soft length assert
   `>= 80`. Do **not** change hard `== 79` asserts in `group.rs` /
   v206 / v225 / v228 / v233.
2. Always flexible (official `flexibleVersions` is `0+`). Dispatch
   **v0** only. v1+ → **35**.
3. Official `DescribeShareGroupOffsetsRequest.json` v0. Parse group
   ids (skip topics/partitions). Do **not** persist. Do **not** wrap
   OffsetFetch 9 / `describe_group` / ConsumerGroupDescribe 69 /
   ShareGroupDescribe 77.
4. Official response has **throttleTimeMs** and **per-group** error
   (no top-level error besides throttle). Echo each requested groupId
   with empty `Topics[]`, error **42**, errorMessage
   `"not KIP-932 share offsets"`. Do **not** write Lag (v1+).
   Unparseable body → throttle **0** + empty `Groups[]` (no top-level
   0 success). Prefer echoing parsed group ids with per-group **42**.
5. Controller is **not** required (group-local reject).
6. ACL: Group **DESCRIBE** per group id. Disabled ACLs allow the
   **42** path. Denied → that group **30**, errorMessage **null**,
   empty topics.

## Non-goals

| Deferred | Why |
|----------|-----|
| KIP-932 share-offset store | Reject only |
| Wrap OffsetFetch 9 / describe_group / ConsumerGroupDescribe 69 / ShareGroupDescribe 77 | Clients keep using 9 / 15 / 69 / 77 |
| AlterShareGroupOffsets 91 / DeleteShareGroupOffsets 92 | Sibling leftovers |
| Advertise v1 (Lag / KIP-1226) | Out of range |
| join-set, unclean, live reassignment, txn default-on, Kafka Fetch group tags | Out of slice |
| `group.rs` hard `== 79` | Intentionally untouched |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (v0 always flexible)

Official request v0 (`DescribeShareGroupOffsetsRequest.json`;
`flexibleVersions: 0+`):

```
Groups[] {
  GroupId compact string
  Topics[] nullable {
    TopicName compact string
    Partitions[] i32
    tagged
  }
  tagged
}
tagged
```

`Topics[]` null means all topic-partitions (official). Volant skips
the array and does not invent offsets.

Official response v0 (`DescribeShareGroupOffsetsResponse.json`;
**has** `throttleTimeMs`; group-level error is **after** `Topics[]`):

```
throttleTimeMs i32 = 0
Groups[] {
  GroupId compact string           // echo
  Topics[] empty                   // no Lag / no partition rows
  ErrorCode i16 = 42               // or 30 if Group DESCRIBE denied
  ErrorMessage compact nullable string
                                   // "not KIP-932 share offsets";
                                   // null on ACL deny
  tagged
}
tagged
```

Do **not** write Lag (official versions `1+`). Official
`validVersions` is **0–1**; Volant advertises **0** only.

## Semantics

```
DescribeShareGroupOffsets v0
  │
  ├─ Group DESCRIBE fail → that group 30, errorMessage null,
  │                         empty Topics[]; no persist
  ├─ Unparseable body → throttle 0 + empty Groups[] (no top-level 0)
  ├─ Controller not required
  │
  └─ else → echo groupId, empty Topics[], per-group 42 INVALID_REQUEST
            (not KIP-932 share offsets)
            nothing persisted
            OffsetFetch / describe_group / 69 / 77 unchanged
```

- Response throttle is always 0. No top-level error besides throttle.
- Does **not** wrap OffsetFetch **9**, `describe_group`,
  ConsumerGroupDescribe **69**, or ShareGroupDescribe **77**.
- Official Apache Kafka first flexible version is **0+**; Volant
  advertises v0 only as flexible (matches official). v1 is not
  advertised and returns **35**.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --test v284_describe_share_group_offsets -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | key **90** min=max=0; key **83** still listed; `SUPPORTED_APIS.len() >= 80` |
| v0 describe one group | throttle **0**, one group, error **42** after empty Topics[], nothing persisted / OffsetFetch unchanged |
| v1 | **35** |
| ACL deny | that group **30**, errorMessage **null**, empty topics |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 90 + `SUPPORTED_APIS` |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch v0 + always-flex header |
| `crates/volant-broker/src/kafka/group_api.rs` | encode (parse + per-group reject) |
| `crates/volant-broker/tests/v284_describe_share_group_offsets.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | key 90 v0 reject |
| `docs/V284_SPEC.md` | This spec |

## Honesty leftovers

- **Not KIP-932.** Official validVersions is **0–1**. Volant
  advertises **0** only (v1 Lag / KIP-1226 is out of range).
- Does **not** wrap OffsetFetch / classic describe / 69 / 77.
- Official response has throttle and no top-level error; reject is
  per-group **42** after empty `Topics[]`.
- `group.rs` hard `== 79` untouched.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
- [V276_SPEC.md](./V276_SPEC.md) — ShareGroupDescribe 77 reject
- [V279_SPEC.md](./V279_SPEC.md) — InitializeShareGroupState 83 reject
- OffsetFetch key **9** — classic consumer offsets; not share offsets
