# v0.148 — language OffsetFetch topic + metadata

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V118_SPEC.md](./V118_SPEC.md) /
[V122_SPEC.md](./V122_SPEC.md) / [V140_SPEC.md](./V140_SPEC.md):
`OffsetFetchAll` / `offset_fetch_all` / `fetch_offsets` already return
per-entry **metadata**. Topic-filtered `OffsetFetch` / `offset_fetch`
still return partition+offset only and drop metadata.

Add named methods that filter the group OffsetFetch to one topic and
keep metadata. Reuse existing `FetchOffsets` / `fetch_offsets` /
`fetchOffsets` (do not reimplement the RPC). Topic-filtered
`OffsetFetch` / `offset_fetch` stay partition+offset. This is **not**
Kafka OffsetFetch versions / require-stable.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Rust, or codec encode/decode.

## Goals

1. **Go:** public `func (c *Client) OffsetFetchEntries(group, topic
   string) ([]OffsetFetchEntry, error)`. Call `FetchOffsets(group,
   nil)` and keep entries whose `Topic == topic`. Public
   `OffsetFetchEntry` already has `Metadata`.
2. **Java:** public `List<OffsetFetchEntry> offsetFetchEntries(String
   group, String topic)`. Same filter on `fetchOffsets(group, empty)`.
   Overload of the existing package-private codec helper (different
   second-arg type).
3. **Python:** public `offset_fetch_entries(self, group: str, topic:
   str) -> list[OffsetFetchEntry]`. Filter `self.fetch_offsets(group)`
   by `e.topic == topic`. Keep `offset_fetch` as
   `[(partition, offset)]`.
4. Reuse the existing OffsetFetch send loop (v0.78 transient retry +
   v0.105 error 14 redirect). No new retry policy.
5. Do **not** change `OffsetFetch` / `offset_fetch` return types.
6. Do **not** change `Offset` struct / class.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `OffsetFetch` / `offset_fetch` return types | Frozen; partition+offset only |
| Change `Offset` | Frozen |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Codec encode/decode / Rust client | Frozen; metadata already on the wire |

## API

```go
offs, _ := c.OffsetFetch("g", "t")              // []Offset{Partition, Offset} unchanged
allOffs, _ := c.OffsetFetchAll("g")             // []OffsetFetchEntry{Topic, Partition, Offset, Metadata}
rows, _ := c.OffsetFetchEntries("g", "t")       // same topic filter, keep Metadata
```

```java
List<Offset> offs = c.offsetFetch("g", "t");                    // partition+offset unchanged
List<OffsetFetchEntry> all = c.offsetFetchAll("g");             // topic+partition+offset+metadata
List<OffsetFetchEntry> rows = c.offsetFetchEntries("g", "t");   // same topic filter, keep metadata
```

```python
offs = c.offset_fetch("g", "t")                 # [(partition, offset), ...] unchanged
all_offs = c.offset_fetch_all("g")              # [(topic, partition, offset), ...]
rows = c.offset_fetch_entries("g", "t")         # [OffsetFetchEntry, ...] with metadata
```

`OffsetFetch` / `offset_fetch` still send empty entries and filter to
the named topic as `[]Offset` / `List<Offset>` / `[(partition, offset)]`.

## Semantics

- Topic filter stays client-side (empty wire entries = all group
  offsets, then keep the named topic).
- New methods return public `OffsetFetchEntry` rows including
  already-decoded metadata.
- Topic-filtered `OffsetFetch` / `offset_fetch` still return
  partition+offset only.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetFetch loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP stub that injects a scripted OffsetFetch reply with two
topics; one entry has metadata `"consumer-1"`.
Existing OffsetFetch / OffsetFetchAll / FetchOffsets suites still pass.

| Case | Expect |
|------|--------|
| Scripted two-topic reply; one entry metadata `"consumer-1"` | `OffsetFetchEntries` / `offsetFetchEntries` / `offset_fetch_entries` return only the requested topic **with** metadata |
| Same reply via `OffsetFetch` / `offset_fetch` | still partition+offset only for that topic |

```bash
# from clients/python
PYTHONPATH=src python3 -m unittest tests.test_client -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- Topic-filtered fetch is still client-side (empty wire entries).
- Topic-filtered `OffsetFetch` / `offset_fetch` still drop metadata.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Rust client is unchanged.
- Codec encode/decode is unchanged (metadata already decoded).

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to OffsetFetch topic+metadata wrappers:

- **Keep the filter wrappers only.** Do not change the OffsetFetch
  send loop (v0.78 retry + v0.105 14).
- Do not change `OffsetFetch` / `offset_fetch` return types.
- Do not change `Offset`.
- Do not change Rust, codec, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`OffsetFetchEntries` next to
  `OffsetFetch` / `OffsetFetchAll`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`offsetFetchEntries(group, topic)` next to `offsetFetch` /
  `offsetFetchAll`)
- Python `clients/python/src/volant/client.py`
  (`offset_fetch_entries` next to `offset_fetch` / `offset_fetch_all`)

The hunk is local to OffsetFetch topic+metadata wrappers.

## Related

- [V24_SPEC.md](./V24_SPEC.md) — Python/Go OffsetCommit / OffsetFetch
- [V33_SPEC.md](./V33_SPEC.md) — Java OffsetCommit / OffsetFetch
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
- [V122_SPEC.md](./V122_SPEC.md) — language OffsetFetch entries
- [V140_SPEC.md](./V140_SPEC.md) — Go/Java OffsetFetch entry metadata
