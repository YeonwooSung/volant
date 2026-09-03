# v0.128 — Go/Java OffsetCommit metadata

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V119_SPEC.md](./V119_SPEC.md):
Python `offset_commit(..., metadata="")` and Rust
`OffsetCommitEntry.metadata` already set per-entry metadata. Go
`OffsetCommit` and Java 4-arg / 6-arg `offsetCommit` always send
`Metadata: ""`.

Add a convenience that sets per-entry metadata without breaking
existing signatures. Reuse the existing OffsetCommit send loop
(v0.78 retry + v0.105 14). This is **not** Kafka OffsetCommit
versions.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, or Rust client.

## Goals

1. **Go:** public `func (c *Client) OffsetCommitMeta(group, topic string,
   partition int, offset int64, metadata string) error` wrapping
   `CommitOffsets` with one entry. `OffsetCommit` stays
   `return c.OffsetCommitMeta(..., "")`.
2. **Java:** public `offsetCommit(group, topic, partition, offset,
   metadata)` and a 7-arg with member + generation + metadata.
   Existing 4-arg calls the metadata overload with `""`. Existing
   6-arg member/generation overload still calls the batch path with
   metadata `""`.
3. Reuse the existing OffsetCommit send loop (v0.78 retry + v0.105
   error 14). `generation = 0` still skips the broker generation check.
4. No new constructor args. Default retry / redirect knobs unchanged.
5. Existing OffsetCommit retry / 14 tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `offset_commit(..., metadata="")` | Already public |
| Rust `entry.metadata` | Already on `OffsetCommitEntry` |
| Kafka OffsetCommit versions / txn offset commit | Native opcode 6 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping CreateTopic / JoinGroup / Produce / acks | Sibling residuals |
| Changing GroupConsumer commit policy | Thin Client only |

## API

```go
_ = c.OffsetCommit("g", "t", 0, 5)                    // still empty metadata
_ = c.OffsetCommitMeta("g", "t", 0, 5, "consumer-1")  // one entry, admin path
```

```java
c.offsetCommit("g", "t", 0, 5);                         // still empty metadata
c.offsetCommit("g", "t", 0, 5, "consumer-1");           // 5-arg metadata
c.offsetCommit("g", "t", 0, 5, "m1", 3L);               // 6-arg still ""
c.offsetCommit("g", "t", 0, 5, "m1", 3L, "consumer-1"); // 7-arg
```

`generation = 0` skips the broker generation check (same as today).

## Semantics

- 4-arg / `OffsetCommit` still send empty per-entry metadata.
- New convenience encodes the given metadata string on the one entry.
- 6-arg member + generation still sends metadata `""`.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetCommit loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Not Kafka OffsetCommit versions / `TxnOffsetCommit`.

## Tests

Fake TCP stub that decodes OffsetCommit request entries.
Existing OffsetCommit retry / 14 tests still pass.

| Case | Expect |
|------|--------|
| Existing `OffsetCommit` / 4-arg | empty metadata |
| New `OffsetCommitMeta` / 5-arg | given metadata string |
| Existing 6-arg member/generation | still metadata `""` |
| First 7 then ok (`max_retries=2`) | still two RPCs (existing retry tests) |

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** OffsetCommit versions / `TxnOffsetCommit`.
- Native opcode **6** only. Per-entry metadata is already on the wire.
- `generation = 0` still skips the broker generation check.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust clients are unchanged.
- Java 6-arg member/generation still hard-codes empty metadata.

## Merge notes

Sibling slices that also edit Go/Java `Client` should keep this hunk
local to OffsetCommit:

- **Keep the metadata convenience only.** Do not change the
  OffsetCommit send loop (v0.78 retry + v0.105 14).
- Do not wrap CreateTopic, JoinGroup, Produce, or acks.
- Do not change Python or Rust.
- Do not change the broker, Kafka shim, or protocol in this merge.

Expect conflicts on:

- Go `clients/go/client.go` (`OffsetCommitMeta` next to `OffsetCommit`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`offsetCommit` 5-arg / 7-arg next to 4-arg / 6-arg)

The hunk is local to OffsetCommit.

## Related

- [V78_SPEC.md](./V78_SPEC.md) — OffsetCommit / OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — OffsetCommit / OffsetFetch error 14
- [V119_SPEC.md](./V119_SPEC.md) — public CommitOffsets batch
