# v0.118 — language OffsetFetch all-group

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V24_SPEC.md](./V24_SPEC.md) /
[V33_SPEC.md](./V33_SPEC.md): language `offset_fetch` / `OffsetFetch` /
`offsetFetch` already send **empty wire entries** (all group offsets)
then **filter to one topic client-side**. Rust `fetch_offsets` already
returns the full group list when entries are empty.

Add a public “all group offsets” API. Keep the existing topic-filtered
method. Reuse the OffsetFetch send loop (v0.78 retry + v0.105 14
redirect). Empty entries on the wire stay “all”. This is **not** Kafka
OffsetFetch versions / require-stable.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Rust client.

## Goals

1. **Python:** public `offset_fetch_all(self, group: str)` →
   `list[tuple[str, int, int]]` as `(topic, partition, offset)`.
   Keep `offset_fetch(group, topic)`.
2. **Go:** public `OffsetFetchAll(group string) ([]OffsetFetchEntry, error)`
   with topic on each row. Keep `OffsetFetch(group, topic)`.
3. **Java:** public `List<OffsetFetchEntry> offsetFetchAll(String group)`.
   `Offset` stays partition+offset so `offsetFetch(group, topic)` is
   unchanged. New `OffsetFetchEntry(topic, partition, offset)`.
4. Reuse the existing OffsetFetch send loop (v0.78 transient retry +
   v0.105 error 14 redirect). No new retry policy.
5. Empty wire entries remain “all” (current broker / protocol behavior).
6. Do **not** wrap Metadata (v0.116), CreateTopic (v0.117),
   CommitOffsets (v0.119), or ListMembers.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `fetch_offsets` | Already accepts empty entries = all |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only; not Kafka |
| Topic-filtered `offset_fetch` signature change | Frozen; still filters client-side |
| Metadata / CreateTopic / CommitOffsets / ListMembers | Sibling residuals |
| Broker / protocol / new opcodes | Frozen; empty entries already mean all |
| Phase 155 / homemade Raft | Frozen |

## API

```python
offs = c.offset_fetch("g", "t")       # [(partition, offset), ...]
all_offs = c.offset_fetch_all("g")    # [(topic, partition, offset), ...]
```

```go
offs, _ := c.OffsetFetch("g", "t")          // []Offset{Partition, Offset}
allOffs, _ := c.OffsetFetchAll("g")         // []OffsetFetchEntry{Topic, Partition, Offset}
```

```java
List<Offset> offs = c.offsetFetch("g", "t");
List<OffsetFetchEntry> all = c.offsetFetchAll("g");
```

`offset_fetch` / `OffsetFetch` / `offsetFetch` still send empty
entries and filter to the named topic. `offset_fetch_all` /
`OffsetFetchAll` / `offsetFetchAll` return every row from that same
response.

## Semantics

- Empty wire entries = all group offsets (same as today).
- Topic filter stays client-side on the existing method.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetFetch loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Error 2 / 9 / 10 / 11 and protocol are not retried.
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP. Existing OffsetFetch retry / 14 tests still pass.

| Case | Expect |
|------|--------|
| `offset_fetch_all` / `OffsetFetchAll` two topics from one all-entries response | both rows |
| Existing `offset_fetch(group, "t")` on the same response | only topic `t` |
| Transient 7 then ok | inherit existing tests |
| Error 14 still redirects | inherit existing tests |

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest discover -s tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- Topic-filtered fetch is still client-side (empty wire entries).
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Rust `volant-client` is unchanged.

## Merge notes

Sibling slice **v0.119** (CommitOffsets) also edits the three `Client`
files. When merging:

- **Keep the OffsetFetch all-group wrapper only.** Do not change the
  OffsetFetch send loop (v0.78 retry + v0.105 14).
- Do not wrap Metadata, CreateTopic, CommitOffsets, or ListMembers.
- Do not change `offset_fetch(group, topic)` / `OffsetFetch` /
  `offsetFetch` return types.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (public
  `offset_fetch_all` next to `offset_fetch`)
- Go `clients/go/client.go` (public `OffsetFetchAll` next to
  `OffsetFetch`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`offsetFetchAll` next to `offsetFetch`)

The hunk is local to OffsetFetch.

## Related

- [V24_SPEC.md](./V24_SPEC.md) — Python/Go OffsetCommit / OffsetFetch
- [V33_SPEC.md](./V33_SPEC.md) — Java OffsetCommit / OffsetFetch
- [V78_SPEC.md](./V78_SPEC.md) — language OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — language OffsetFetch 14 redirect
