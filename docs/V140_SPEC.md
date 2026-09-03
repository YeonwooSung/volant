# v0.140 — Go/Java OffsetFetch entry metadata

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V118_SPEC.md](./V118_SPEC.md) /
[V122_SPEC.md](./V122_SPEC.md) / [V128_SPEC.md](./V128_SPEC.md):
native OffsetFetch response entries already carry per-entry
**metadata**. Python `OffsetFetchEntry.metadata` and Go/Java codec
types already decode it. Public Go `OffsetFetchEntry` and Java
`io.volant.OffsetFetchEntry` drop it when mapping codec rows.

Surface the already-decoded metadata on the **public** OffsetFetch
entry types. Topic-filtered `OffsetFetch` / `offsetFetch` still
returns partition+offset only. This is **not** Kafka OffsetFetch
versions / require-stable.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Rust, or codec encode/decode.

## Goals

1. **Go:** add `Metadata string` to public `OffsetFetchEntry` (after
   Topic / Partition / Offset so named-field composites stay
   compatible). `OffsetFetchAll` copies `e.Metadata`.
2. **Go:** leave `FetchOffsets` return type as
   `[]codec.OffsetFetchEntry` (already has `Metadata`).
3. **Java:** add `public final String metadata` to
   `io.volant.OffsetFetchEntry`. Keep the 3-arg constructor as
   `this(..., "")`. Add a 4-arg constructor
   `(topic, partition, offset, metadata)` with null → `""`.
4. **Java:** update `equals` / `hashCode` / `toString` to include
   metadata. `Client.fetchOffsets` passes `e.metadata` into the
   4-arg constructor. `offsetFetchAll` uses `fetchOffsets`.
5. Do **not** change topic-filtered `[]Offset` / `List<Offset>`.
6. Do **not** change codec encode/decode, broker, Rust, or Python.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `OffsetFetchEntry.metadata` | Already public |
| Go `FetchOffsets` return type | Already `[]codec.OffsetFetchEntry` |
| Topic-filtered `OffsetFetch` / `offsetFetch` | Frozen; partition+offset only |
| Kafka OffsetFetch versions / require-stable | Native opcode 7 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Codec encode/decode / Rust client | Frozen; metadata already on the wire |

## API

```go
offs, _ := c.OffsetFetch("g", "t")          // []Offset{Partition, Offset} unchanged
allOffs, _ := c.OffsetFetchAll("g")         // []OffsetFetchEntry{Topic, Partition, Offset, Metadata}
rows, _ := c.FetchOffsets("g", nil)         // []codec.OffsetFetchEntry (already has Metadata)
```

```java
List<Offset> offs = c.offsetFetch("g", "t");           // partition+offset unchanged
List<OffsetFetchEntry> all = c.offsetFetchAll("g");    // topic+partition+offset+metadata
List<OffsetFetchEntry> rows = c.fetchOffsets("g", null);
new OffsetFetchEntry("t", 0, 5);                       // metadata ""
new OffsetFetchEntry("t", 0, 5, "consumer-1");
```

`OffsetFetch` / `offsetFetch` still send empty entries and filter to
the named topic as `[]Offset` / `List<Offset>` (partition+offset).

## Semantics

- Public OffsetFetch entry types now carry the already-decoded
  per-entry metadata string.
- Empty metadata stays `""` (3-arg Java constructor; omitted Go
  named fields).
- Topic-filtered fetch is still client-side and still returns
  partition+offset only.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  OffsetFetch loop (v0.78; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.105).
- Not Kafka OffsetFetch versions / require-stable / multi-group v8+.

## Tests

Fake TCP stub that injects a scripted OffsetFetch reply.
Existing OffsetFetchAll / FetchOffsets suites still pass.

| Case | Expect |
|------|--------|
| Scripted reply with one entry metadata `"consumer-1"` | `OffsetFetchAll` / `offsetFetchAll` returns that metadata |
| Same reply via `FetchOffsets` / `fetchOffsets` | metadata still `"consumer-1"` |
| Java 3-arg `new OffsetFetchEntry("t", 0, 5)` | `metadata=""` |

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** OffsetFetch versions / require-stable / multi-group.
- Topic-filtered fetch is still client-side (empty wire entries) and
  still returns partition+offset only.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust clients are unchanged.
- Codec encode/decode is unchanged (metadata already decoded).

## Merge notes

Sibling slices that also edit Go/Java `Client` should keep this hunk
local to OffsetFetch entry mapping:

- **Keep the public metadata field only.** Do not change the
  OffsetFetch send loop (v0.78 retry + v0.105 14).
- Do not change `OffsetFetch` / `offsetFetch` return types.
- Do not change `FetchOffsets` Go return type.
- Do not change Python, Rust, codec, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`OffsetFetchEntry` + `OffsetFetchAll`)
- Java `clients/java/src/main/java/io/volant/OffsetFetchEntry.java`
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`fetchOffsets` mapping)

The hunk is local to public OffsetFetch entry types.

## Related

- [V24_SPEC.md](./V24_SPEC.md) — Python/Go OffsetCommit / OffsetFetch
- [V33_SPEC.md](./V33_SPEC.md) — Java OffsetCommit / OffsetFetch
- [V118_SPEC.md](./V118_SPEC.md) — language OffsetFetch all-group
- [V122_SPEC.md](./V122_SPEC.md) — language OffsetFetch entries
- [V128_SPEC.md](./V128_SPEC.md) — Go/Java OffsetCommit metadata
