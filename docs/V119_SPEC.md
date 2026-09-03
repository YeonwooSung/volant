# v0.119 — language public CommitOffsets batch

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V78_SPEC.md](./V78_SPEC.md) /
[V105_SPEC.md](./V105_SPEC.md) / Rust `Client::commit_offsets`: the
native OffsetCommit opcode already carries `member_id`, `generation`,
and a list of entries (including per-entry metadata). Language
convenience APIs still commit **one** topic/partition. Rust
`volant-client` already exposes the batch path.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Rust client (`Client::commit_offsets`
is already public).

## Goals

1. **Python:** public `commit_offsets(self, group, entries, *,
   member_id="", generation=0)` where `entries` are existing
   `OffsetCommitEntry` or `(topic, partition, offset)` /
   `(topic, partition, offset, metadata)` tuples. `offset_commit(...)`
   stays the one-entry wrapper and calls the batch path.
2. **Go:** public `func (c *Client) CommitOffsets(group, memberID string,
   generation uint32, entries []codec.OffsetCommitEntry) error`.
   Rename-export the existing unexported `commitOffsets`. `OffsetCommit`
   stays the one-entry wrapper.
3. **Java:** make `offsetCommit(String group, String memberId, long
   generation, List<Codec.OffsetCommitEntry> entries)` **public**. Keep
   the existing one-entry overloads.
4. Reuse the existing OffsetCommit send loop (v0.78 retry + v0.105
   error 14). `generation = 0` still skips the broker generation check.
5. No new constructor args. Default retry / redirect knobs unchanged.
6. Existing OffsetCommit retry / 14 tests still pass.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `Client::commit_offsets` | Already public |
| Metadata / CreateTopic / OffsetFetch wraps | Siblings v0.116–v0.118 |
| Kafka OffsetCommit versions / txn offset commit | Native opcode 6 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Changing GroupConsumer commit policy | Thin Client only; Go already batches via the same path |

## API

```python
c.offset_commit("g", "t", 0, 5)  # still one entry
c.commit_offsets("g", [OffsetCommitEntry("t", 0, 5), ("t", 1, 9)])
c.commit_offsets("g", [("t", 0, 5)], member_id="m1", generation=3)
```

```go
_ = c.OffsetCommit("g", "t", 0, 5) // still one entry
_ = c.CommitOffsets("g", "m1", 3, []codec.OffsetCommitEntry{
    {Topic: "t", Partition: 0, Offset: 5, Metadata: ""},
    {Topic: "t", Partition: 1, Offset: 9, Metadata: ""},
})
```

```java
c.offsetCommit("g", "t", 0, 5); // still one entry
c.offsetCommit("g", "m1", 3L, List.of(
    new Codec.OffsetCommitEntry("t", 0, 5L, ""),
    new Codec.OffsetCommitEntry("t", 1, 9L, "")));
```

`generation = 0` skips the broker generation check (same as today).

## Tests

Fake TCP. Existing OffsetCommit retry / 14 tests must still pass.

| Case | Expect |
|------|--------|
| Batch of two entries | stub decodes both on one OffsetCommit RPC |
| One-entry `offset_commit` / `OffsetCommit` | still works (empty member, generation 0) |
| `member_id` + `generation` provided | sent on the wire |
| First 7 then ok (`max_retries=2`) | still two RPCs (existing retry tests) |

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** OffsetCommit versions / `TxnOffsetCommit`.
- Native opcode **6** only. Per-entry metadata is already on the wire.
- `generation = 0` still skips the broker generation check.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Rust `volant-client` is unchanged.
- Python `GroupConsumer.commit` still loops one-entry `offset_commit`
  (same as before). Go `GroupConsumer.Commit` already sent a batch via
  the now-public `CommitOffsets`.

## Merge notes

Sibling slice **v0.118** (OffsetFetch) also edits the three `Client`
files. When merging:

- **Keep the public CommitOffsets / commit_offsets / offsetCommit
  batch wrapper only.** Do not change the OffsetCommit send loop
  (v0.78 retry + v0.105 14).
- Do not wrap Metadata, CreateTopic, or OffsetFetch.
- Do not change `_redirect_to_controller` / `redirectToController`.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (public
  `commit_offsets` next to `offset_commit`)
- Go `clients/go/client.go` (public `CommitOffsets` next to
  `OffsetCommit`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`offsetCommit` list overload visibility + javadoc)

The hunk is local to OffsetCommit.

## Related

- [V78_SPEC.md](./V78_SPEC.md) — OffsetCommit / OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — OffsetCommit / OffsetFetch error 14
- [V31_SPEC.md](./V31_SPEC.md) / [V32_SPEC.md](./V32_SPEC.md) /
  [V33_SPEC.md](./V33_SPEC.md) — GroupConsumer commit with member + generation
