# v0.24 — Python + Go offset commit / fetch

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python/Go clients have produce/fetch/metadata
only — no groups” by exposing native **OffsetCommit** (opcode 6) and
**OffsetFetch** (opcode 7) on both clients.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker protocol.

## Goals

1. **Python** `Client.offset_commit` / `Client.offset_fetch` matching
   `crates/volant-protocol/src/payload.rs`.
2. **Go** `Client.OffsetCommit` / `Client.OffsetFetch` with the same
   little-endian payloads.
3. **Admin commit path:** empty `member_id`, `generation = 0` (skip
   generation check), same as the CLI / Rust `commit_offsets`.
4. **BrokerError** on nonzero `error_code` (same as produce/fetch).
5. **Codec unit tests** with exact-byte fixtures from `payload.rs`.
6. **E2E** gated by `VOLANT_E2E=1`: create topic, produce, commit, fetch
   offset matches. Skip if no server.

## Non-goals

| Deferred | Why |
|----------|-----|
| JoinGroup / Heartbeat / LeaveGroup | Optional; keep the slice to commit/fetch |
| Consumer assignor / session membership | Admin offsets only |
| Kafka OffsetCommit / OffsetFetch API keys | Native opcodes 6/7; no Kafka keys |
| Multi-entry convenience API | One topic/partition per call is enough |
| Java client | Sibling leftover |
| TLS / SCRAM / shared-token Auth | Unchanged plaintext MVP |
| Required CI language job | Existing optional smoke scripts only |
| Broker / protocol changes | Wire already exists |

## Wire

Unchanged from Phase 3 / `payload.rs`. Payloads are little-endian.
Strings are `u16_le` length + UTF-8.

### OffsetCommit request (opcode 6)

```
group_id: string
member_id: string          # empty for admin commits
generation: u32            # 0 = skip generation check
entry_count: u32
entries: repeated {
  topic: string
  partition: u32
  offset: u64              # next offset to read
  metadata: string         # may be empty
}
```

### OffsetCommit response

```
error_code: u16
```

### OffsetFetch request (opcode 7)

```
group_id: string
entry_count: u32           # 0 = all committed offsets for the group
entries: repeated {
  topic: string
  partition: u32
}
```

### OffsetFetch response

```
error_code: u16
entry_count: u32
entries: repeated {
  topic: string
  partition: u32
  offset: u64              # u64::MAX = unknown / not committed
  metadata: string
}
```

The convenience APIs take a **topic** and send empty OffsetFetch entries
(all group offsets), then filter to that topic client-side (same as the
CLI).

## API

```python
c.offset_commit(group="g", topic="t", partition=0, offset=5)
offs = c.offset_fetch(group="g", topic="t")  # [(partition, offset), ...]
```

```go
err = c.OffsetCommit("g", "t", 0, 5)
offs, err := c.OffsetFetch("g", "t")  // []Offset{Partition, Offset}
```

Python also accepts optional `member_id=`, `generation=`, `metadata=` on
`offset_commit` for callers that already joined a group.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_codec.py` | Exact-byte OffsetCommit/OffsetFetch fixtures |
| `clients/python/tests/test_e2e.py` | Live commit → fetch; skip unless `VOLANT_E2E=1` |
| `clients/go/codec/codec_test.go` | Same fixtures |
| `clients/go/e2e_test.go` | Live commit → fetch; skip unless `VOLANT_E2E=1` |

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
```

## Honesty leftovers

- No JoinGroup / Heartbeat / LeaveGroup / cooperative assignor on Python/Go.
- Convenience commit is admin-only (`generation=0`).
- OffsetFetch topic filter is client-side (empty wire entries).
- Still no Java client, Kafka-wire SDK, TLS, or leader redirect.
- Broker and Rust `volant-client` are unchanged.

See [clients/python/README.md](../clients/python/README.md) and
[clients/go/README.md](../clients/go/README.md).
