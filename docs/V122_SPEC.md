# v0.122 — language OffsetFetch entries (Rust parity)

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V118_SPEC.md](./V118_SPEC.md) /
Rust `Client::fetch_offsets`: language `offset_fetch` /
`offset_fetch_all` always send **empty wire entries** (all group
offsets) then filter client-side. Rust `fetch_offsets(group, entries)`
already encodes specific topic/partition rows.

Expose the rust-shaped API. Keep `offset_fetch(group, topic)` and
`offset_fetch_all(group)`. Reuse the OffsetFetch send loop (v0.78
retry + v0.105 14). Empty entries on the wire stay “all”. This is
**not** Kafka OffsetFetch versions / require-stable.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, or Rust client (`fetch_offsets` is
already public).

## Goals

1. **Python:** public `fetch_offsets(self, group: str, entries:
   Optional[list] = None)` → list of existing `OffsetFetchEntry`
   (topic, partition, offset). Empty / `None` = all (current wire).
   `offset_fetch_all` calls it. `offset_fetch(group, topic)` still
   filters to one topic.
2. **Go:** export `func (c *Client) FetchOffsets(group string,
   entries []codec.OffsetEntry) ([]codec.OffsetFetchEntry, error)`
   (today `fetchOffsets` is unexported). Keep `OffsetFetch` /
   `OffsetFetchAll`.
3. **Java:** public `List<OffsetFetchEntry> fetchOffsets(String group,
   List<Codec.OffsetEntry> entries)`. Null / empty = all. Keep
   `offsetFetch` / `offsetFetchAll`.
4. Reuse the existing OffsetFetch send loop (v0.78 transient retry +
   v0.105 error 14 redirect). No new retry policy.
5. Empty wire entries remain “all” (current broker / protocol behavior).
6. Do **not** wrap ListMembers (v0.121), GroupConsumer (v0.123), or
   DescribeGroup (v0.124).

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `fetch_offsets` | Already public |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only; not Kafka |
| Topic-filtered `offset_fetch` signature change | Frozen; still filters client-side |
| ListMembers / GroupConsumer / DescribeGroup | Sibling residuals |
| Broker / protocol / new opcodes | Frozen; empty entries already mean all |
| Phase 155 / homemade Raft | Frozen |

## API

```python
offs = c.offset_fetch("g", "t")            # [(partition, offset), ...]
all_offs = c.offset_fetch_all("g")         # [(topic, partition, offset), ...]
rows = c.fetch_offsets("g", [("t", 0)])    # [OffsetFetchEntry, ...]
rows = c.fetch_offsets("g")                # empty / None = all
```

```go
offs, _ := c.OffsetFetch("g", "t")          // []Offset{Partition, Offset}
allOffs, _ := c.OffsetFetchAll("g")         // []OffsetFetchEntry{Topic, Partition, Offset}
rows, _ := c.FetchOffsets("g", []codec.OffsetEntry{{Topic: "t", Partition: 0}})
rows, _ = c.FetchOffsets("g", nil)          // nil / empty = all
```

```java
List<Offset> offs = c.offsetFetch("g", "t");
List<OffsetFetchEntry> all = c.offsetFetchAll("g");
List<OffsetFetchEntry> rows = c.fetchOffsets("g", List.of(new Codec.OffsetEntry("t", 0)));
rows = c.fetchOffsets("g", null);           // null / empty = all
```

`offset_fetch` / `OffsetFetch` / `offsetFetch` still send empty
entries and filter to the named topic. `offset_fetch_all` /
`OffsetFetchAll` / `offsetFetchAll` return every row from that same
all-entries request.

## Semantics

- Empty / `None` / `null` wire entries = all group offsets (same as today).
- Non-empty entries are encoded on the OffsetFetch request (Rust parity).
- Topic filter stays client-side on the existing one-topic method.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetFetch loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Error 2 / 9 / 10 / 11 and protocol are not retried.
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP stub that records decoded OffsetFetch request entries.
Existing OffsetFetch retry / 14 tests still pass.

| Case | Expect |
|------|--------|
| `fetch_offsets(group, [(topic, partition)])` / equivalent | stub decodes those entries (not empty) |
| `fetch_offsets(group, None/empty)` | still sends empty (all) |
| Existing `offset_fetch(group, topic)` | only that topic; empty wire entries |
| Existing `offset_fetch_all` | still works; empty wire entries |

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
- Language GroupConsumer still uses the existing path (v0.123).

## Merge notes

Sibling slices **v0.121** (ListMembers) and **v0.124** (DescribeGroup)
also edit the three `Client` files. When merging:

- **Keep the OffsetFetch entries wrapper only.** Do not change the
  OffsetFetch send loop (v0.78 retry + v0.105 14).
- Do not wrap ListMembers, GroupConsumer, or DescribeGroup.
- Do not change `offset_fetch(group, topic)` / `OffsetFetch` /
  `offsetFetch` or `offset_fetch_all` / `OffsetFetchAll` /
  `offsetFetchAll` return types.
- Do not change the broker, Kafka shim, or Rust client in this merge.

Expect conflicts on:

- Python `clients/python/src/volant/client.py` (public
  `fetch_offsets` next to `offset_fetch` / `offset_fetch_all`)
- Go `clients/go/client.go` (export `FetchOffsets` next to
  `OffsetFetch` / `OffsetFetchAll`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`fetchOffsets` next to `offsetFetch` / `offsetFetchAll`)

The hunk is local to OffsetFetch.

## Related

- [V24_SPEC.md](./V24_SPEC.md) — Python/Go OffsetCommit / OffsetFetch
- [V33_SPEC.md](./V33_SPEC.md) — Java OffsetCommit / OffsetFetch
- [V78_SPEC.md](./V78_SPEC.md) — language OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — language OffsetFetch 14 redirect
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
