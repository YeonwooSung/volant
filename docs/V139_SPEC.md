# v0.139 — Go OffsetCommit member + generation

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V119_SPEC.md](./V119_SPEC.md) /
[V128_SPEC.md](./V128_SPEC.md): native OffsetCommit (opcode 6) already
carries `member_id` + `generation`. Python
`offset_commit(..., member_id=, generation=)` and Java 6-arg / 7-arg
`offsetCommit(group, topic, partition, offset, memberId, generation[,
metadata])` already send them. Go only has:

- `OffsetCommit` / `OffsetCommitMeta` — admin path: empty member,
  generation 0
- `CommitOffsets(group, memberID, generation, entries)` — batch,
  already public (v0.119)

Add a one-entry convenience that sends caller member + generation
without breaking existing signatures. Reuse the existing OffsetCommit
send loop (v0.78 retry + v0.105 14). This is **not** Kafka OffsetCommit
versions.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Java, or Rust client.

## Goals

1. **Go:** public `func (c *Client) OffsetCommitMember(group, topic
   string, partition int, offset int64, memberID string, generation
   uint32) error` wrapping `OffsetCommitMemberMeta` with
   `metadata=""`.
2. **Go:** public `func (c *Client) OffsetCommitMemberMeta(group, topic
   string, partition int, offset int64, memberID string, generation
   uint32, metadata string) error` wrapping `CommitOffsets` with one
   entry.
3. `OffsetCommit` / `OffsetCommitMeta` stay admin-only (empty member,
   generation 0). Do **not** change `CommitOffsets`.
4. Reuse the existing OffsetCommit send loop (v0.78 retry + v0.105
   error 14). Do not reimplement the RPC loop. `generation = 0` still
   skips the broker generation check.
5. Empty `memberID` + `generation=0` is allowed (same as admin path).
   Non-empty member + nonzero generation is the group-consumer path.
6. No new constructor args. Default retry / redirect knobs unchanged.
7. Existing OffsetCommit / OffsetCommitMeta / CommitOffsets tests
   still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `offset_commit(..., member_id=, generation=)` | Already public |
| Java 6-arg / 7-arg `offsetCommit` | Already public |
| Change `OffsetCommit` / `OffsetCommitMeta` | Stay admin-only |
| Change `CommitOffsets` | Already public (v0.119) |
| Kafka OffsetCommit versions / txn offset commit | Native opcode 6 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Wrapping CreateTopic / JoinGroup / Produce / acks | Sibling residuals |
| Changing GroupConsumer commit policy | Thin Client only; Go already batches via `CommitOffsets` |

## API

```go
_ = c.OffsetCommit("g", "t", 0, 5)                         // still empty member, gen 0
_ = c.OffsetCommitMeta("g", "t", 0, 5, "consumer-1")       // still admin path
_ = c.OffsetCommitMember("g", "t", 0, 5, "m1", 3)          // one entry, member+gen
_ = c.OffsetCommitMemberMeta("g", "t", 0, 5, "m1", 3, "consumer-1")
_ = c.CommitOffsets("g", "m1", 3, []codec.OffsetCommitEntry{...}) // unchanged
```

`OffsetCommitMember` calls `OffsetCommitMemberMeta` with
`metadata=""`. `OffsetCommitMemberMeta` calls existing
`CommitOffsets(group, memberID, generation, []codec.OffsetCommitEntry{{Topic,
Partition, Offset, Metadata}})`.

`generation = 0` skips the broker generation check (same as today).

## Semantics

- `OffsetCommit` / `OffsetCommitMeta` still send empty member and
  generation 0.
- New convenience encodes the given member + generation on the one
  OffsetCommit RPC.
- `OffsetCommitMember` sends empty per-entry metadata.
- `OffsetCommitMemberMeta` encodes the given metadata string on the
  one entry.
- Empty `memberID` + `generation=0` is allowed (admin path).
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetCommit loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Not Kafka OffsetCommit versions / `TxnOffsetCommit`.

## Tests

Fake TCP stub that decodes OffsetCommit request entries
(`offsetCommitReqs` / `copyOffsetCommits`). Existing OffsetCommit /
OffsetCommitMeta / CommitOffsets tests still pass.

| Case | Expect |
|------|--------|
| Existing `OffsetCommit` / `OffsetCommitMeta` | empty member, generation 0 |
| `OffsetCommitMember("g", "t", 0, 5, "m1", 3)` | member `m1`, generation 3, empty metadata |
| `OffsetCommitMemberMeta(..., "consumer-1")` | same member+generation; metadata `"consumer-1"` |
| First 7 then ok (`max_retries=2`) | still two RPCs (existing retry tests) |

```bash
(cd clients/go && go test ./...)
```

## Honesty leftovers

- **Not Kafka** OffsetCommit versions / `TxnOffsetCommit`.
- Native opcode **6** only. Member + generation are already on the wire.
- `generation = 0` still skips the broker generation check.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python, Java, and Rust clients are unchanged.
- `OffsetCommit` / `OffsetCommitMeta` stay admin-only.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to OffsetCommit:

- **Keep the member+generation convenience only.** Do not change the
  OffsetCommit send loop (v0.78 retry + v0.105 14).
- Do not change `OffsetCommit` / `OffsetCommitMeta` / `CommitOffsets`.
- Do not wrap CreateTopic, JoinGroup, Produce, or acks.
- Do not change Python, Java, or Rust.
- Do not change the broker, Kafka shim, or protocol in this merge.

Expect conflicts on:

- Go `clients/go/client.go` (`OffsetCommitMember` next to
  `OffsetCommitMeta` / `CommitOffsets`)
- Go `clients/go/client_test.go` (scripted-broker OffsetCommit tests)

The hunk is local to OffsetCommit.

## Related

- [V78_SPEC.md](./V78_SPEC.md) — OffsetCommit / OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — OffsetCommit / OffsetFetch error 14
- [V119_SPEC.md](./V119_SPEC.md) — public CommitOffsets batch
- [V128_SPEC.md](./V128_SPEC.md) — Go/Java OffsetCommit metadata
