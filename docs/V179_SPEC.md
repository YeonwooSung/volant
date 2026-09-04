# v0.179 — language single-entry FetchOffset

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V122_SPEC.md](./V122_SPEC.md) /
[V164_SPEC.md](./V164_SPEC.md): `FetchOffsets` / `fetch_offsets` /
`fetchOffsets` already take a list of entries (empty = all).
`OffsetFetchAll` / `OffsetFetchEntries` / `fetch_offsets_for_topic`
are the all-group and topic-filter helpers. There is no one-entry
helper matching `DeleteOffset` / `delete_offset` (v0.164).

Add `FetchOffset` / `fetchOffset` / `fetch_offset`. Reuse the
existing batch method (do not reimplement the RPC).
`FetchOffsets` / `fetch_offsets` / `fetchOffsets` /
`OffsetFetch` / `OffsetFetchAll` / `OffsetFetchEntries` stay
unchanged. This is **not** Kafka OffsetFetch versions.

This is residual **v0.179** (language single-entry FetchOffset). It
is **not** Phase 155. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or Rust.

## Goals

1. **Go:** public `func (c *Client) FetchOffset(group, topic string,
   partition uint32) ([]codec.OffsetFetchEntry, error)` that calls
   `FetchOffsets(group, []codec.OffsetEntry{{Topic: topic,
   Partition: partition}})`.
2. **Java:** named public `List<OffsetFetchEntry> fetchOffset(String
   group, String topic, int partition)` that calls
   `fetchOffsets(group, Collections.singletonList(new
   Codec.OffsetEntry(topic, partition)))`. Do not collide with
   `fetchOffsets(List)`.
3. **Python:** public `def fetch_offset(self, group: str, topic: str,
   partition: int) -> list[OffsetFetchEntry]` that calls
   `self.fetch_offsets(group, [(topic, partition)])`.
4. Inherit retry / error **14** from the existing batch method (v0.78
   transient retry + v0.105 error 14). No new retry policy.
5. Do **not** change `FetchOffsets` / `fetch_offsets` /
   `fetchOffsets` / `OffsetFetch` / `OffsetFetchAll` /
   `OffsetFetchEntries`.
6. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `FetchOffsets` / `fetch_offsets` / `fetchOffsets` | Frozen; list still accepts one or many |
| Change `OffsetFetch` / `OffsetFetchAll` / `OffsetFetchEntries` | Frozen; all-group / topic-filter already shipped |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Rust `fetch_offset` | Frozen; batch `fetch_offsets` already public |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) FetchOffset(group, topic string, partition uint32) ([]codec.OffsetFetchEntry, error) {
    return c.FetchOffsets(group, []codec.OffsetEntry{{Topic: topic, Partition: partition}})
}
```

```java
public List<OffsetFetchEntry> fetchOffset(String group, String topic, int partition) {
    return fetchOffsets(group, Collections.singletonList(new Codec.OffsetEntry(topic, partition)));
}
```

```python
def fetch_offset(self, group: str, topic: str, partition: int) -> list[OffsetFetchEntry]:
    return self.fetch_offsets(group, [(topic, partition)])
```

```go
rows, _ := c.FetchOffset("g", "t", 0)                            // one OffsetEntry
rows, _ = c.FetchOffsets("g", []codec.OffsetEntry{{Topic: "t", Partition: 0}})
rows, _ = c.FetchOffsets("g", nil)                               // unchanged: all
all, _ := c.OffsetFetchAll("g")                                  // unchanged: all
topic, _ := c.OffsetFetchEntries("g", "t")                       // unchanged: topic filter
```

```java
List<OffsetFetchEntry> rows = c.fetchOffset("g", "t", 0);        // one OffsetEntry
rows = c.fetchOffsets("g", Collections.singletonList(new Codec.OffsetEntry("t", 0)));
rows = c.fetchOffsets("g", Collections.emptyList());             // unchanged: all
List<OffsetFetchEntry> all = c.offsetFetchAll("g");              // unchanged: all
List<OffsetFetchEntry> topic = c.offsetFetchEntries("g", "t");   // unchanged: topic filter
```

```python
rows = c.fetch_offset("g", "t", 0)                               # one OffsetEntry
rows = c.fetch_offsets("g", [("t", 0)])
rows = c.fetch_offsets("g")                                      # unchanged: all
all_offs = c.offset_fetch_all("g")                               # unchanged: all
topic_offs = c.offset_fetch_entries("g", "t")                    # unchanged: topic filter
```

## Semantics

- One-entry helpers send wire count **1** with that topic + partition.
- They do not re-encode; they wrap the existing batch method.
- Return type matches `FetchOffsets` / `fetch_offsets` /
  `fetchOffsets` (`[]codec.OffsetFetchEntry` / `list[OffsetFetchEntry]`
  / `List<OffsetFetchEntry>`).
- `FetchOffsets` / `fetch_offsets` / `fetchOffsets` are unchanged
  (nil/empty still mean all).
- `OffsetFetch` / `OffsetFetchAll` / `OffsetFetchEntries` are
  unchanged (empty wire entries; topic filter is client-side).
- Transient 6 / 7 / 15 / 16 and transport retry via the batch method
  (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Not Kafka OffsetFetch versions / require-stable.

## Tests

Fake TCP stub that records decoded OffsetFetch entries (same helpers
as existing `TestFetchOffsetsEncodesSpecificEntries` /
`fetchOffsetsEncodesSpecificEntries` /
`test_fetch_offsets_encodes_specific_entries`). Existing empty-all
and topic-filter tests stay green.

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
(cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_fetch_offset -q)
```

Do **not** run full Python `discover`.

| Case | Expect |
|------|--------|
| `FetchOffset` / `fetchOffset` / `fetch_offset` (`"g"`, `"t"`, `0`) | wire entries count 1; topic `t`, partition 0 |
| Existing `FetchOffsets` empty / explicit / All / Entries cases | still pass |

Existing OffsetFetch retry / 14 tests must still pass
(`FetchOffsets` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `FetchOffset` wraps `FetchOffsets` with one entry |
| `clients/go/client_test.go` | one-entry wire check near `TestFetchOffsetsEncodesSpecificEntries` |
| `clients/go/README.md` | usage line + one prose sentence |
| `clients/java/src/main/java/io/volant/Client.java` | named `fetchOffset` wraps `fetchOffsets` with one entry |
| `clients/java/src/test/java/io/volant/ClientTest.java` | one-entry wire check near `fetchOffsetsEncodesSpecificEntries` |
| `clients/java/README.md` | usage line + one prose sentence |
| `clients/python/src/volant/client.py` | `fetch_offset` wraps `fetch_offsets` with one pair |
| `clients/python/tests/test_fetch_offset.py` | one-entry wire check (dedicated; reuses OffsetFetch fake-TCP) |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V179_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable.
- Empty entries still fetch **all** committed offsets for the group.
- `FetchOffsets` / `fetch_offsets` / `fetchOffsets` /
  `OffsetFetch` / `OffsetFetchAll` / `OffsetFetchEntries` are
  unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- Client version stays **0.2.0**.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the one-entry wrappers:

- **Keep the wrappers only.** Do not change `FetchOffsets` /
  `fetch_offsets` / `fetchOffsets` / `OffsetFetch` /
  `OffsetFetchAll` / `OffsetFetchEntries`.
- Do not change the OffsetFetch send loop (v0.78 retry + v0.105 14).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `FetchOffset` after
  `FetchOffsets`
- Java `clients/java/src/main/java/io/volant/Client.java` —
  named `fetchOffset` after `fetchOffsets`
- Python `clients/python/src/volant/client.py` — `fetch_offset`
  after `fetch_offsets`
- `clients/*/README.md` and the existing OffsetFetch test files
  (`client_test.go` / `ClientTest.java`)

## Related

- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetchAll
- [V122_SPEC.md](./V122_SPEC.md) — language FetchOffsets entries
- [V148_SPEC.md](./V148_SPEC.md) — language OffsetFetchEntries
- [V154_SPEC.md](./V154_SPEC.md) — Rust fetch_offsets_for_topic
- [V164_SPEC.md](./V164_SPEC.md) — language single-entry DeleteOffset
- [V78_SPEC.md](./V78_SPEC.md) — language OffsetFetch transient retry
- [V105_SPEC.md](./V105_SPEC.md) — language OffsetFetch error 14
