# v0.116 — Go / Java metadata topic filter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V114_SPEC.md](./V114_SPEC.md):
Python already has `metadata(topics=…)` and Rust has
`metadata_topics`. Go `Client.Metadata()` and Java
`Client.metadata()` always send an empty topics list (all topics).

Add a thin public method that sends a topic filter. Keep
`Metadata()` / `metadata()` as “all topics” (empty list). Reuse the
existing Metadata retry loop (v0.95). No new retry policy. Empty
`topics` remains “all”. This is **not** Kafka Metadata
`allow_auto_topic_creation` / topic ids.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker / protocol / Python / Rust.

## Goals

1. Keep `Metadata()` / `metadata()` as “all topics”. Existing
   signatures stay.
2. **Go:** `func (c *Client) MetadataTopics(topics []string) (Metadata, error)`
   — `Metadata()` becomes `return c.MetadataTopics(nil)`.
3. **Java:** `public Metadata metadata(List<String> topics)` —
   `metadata()` becomes `return metadata(Collections.emptyList());`.
   Do not break existing `metadata()` callers.
4. Same decode / retry / error handling as today’s metadata. Reuse
   the v0.95 retry loop. No new retry policy.
5. Nil / empty `topics` remains “all” (current broker / protocol
   behavior).
6. Do **not** wrap CreateTopic (v0.117), OffsetFetch (v0.118), or
   CommitOffsets (v0.119).
7. Do **not** change broker / protocol. The Metadata request already
   has a `topics` field.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `allow_auto_topic_creation` / topic ids | Native opcode 4 only; not Kafka |
| Python `metadata(topics=…)` / Rust `metadata_topics` | Already shipped |
| CreateTopic wrap (v0.117) / OffsetFetch (v0.118) / CommitOffsets (v0.119) | Sibling residuals |
| New retry policy | Inherit v0.95 via the existing Metadata loop |
| Broker / protocol / new opcodes | Frozen; field already exists |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) Metadata() (Metadata, error) {
    return c.MetadataTopics(nil)
}

func (c *Client) MetadataTopics(topics []string) (Metadata, error) {
    // same decode / retry / error handling as today's Metadata()
}
```

```java
public Metadata metadata() {
    return metadata(Collections.emptyList());
}

public Metadata metadata(List<String> topics) {
    // same decode / retry / error handling as today's metadata()
}
```

Go name is `MetadataTopics` (clear, no overload). Java overloads
`metadata(List)` so existing `metadata()` callers are unchanged.

```go
c.Metadata()                              // all topics
c.MetadataTopics([]string{"events"})
c.MetadataTopics(nil)                     // same as Metadata()
c.MetadataTopics([]string{})              // same as Metadata()
```

```java
c.metadata();                             // all topics
c.metadata(List.of("events"));
c.metadata(Collections.emptyList());      // same as metadata()
```

## Semantics

- Empty / nil `topics` = all topics (same as today).
- Named list is encoded as the native Metadata `topics` field
  (`u32` count + strings). Broker already filters on that field.
- Transient 6 / 7 / 15 / 16 and TCP I/O retry via the existing
  Metadata loop (v0.95; default `max_retries=0`).
- Error 2 / 9 / 10 / 11 / 13 / 14 and protocol are not retried.
- Native Metadata still has no top-level `error_code`.
- Not Kafka Metadata versions / `allow_auto_topic_creation` / topic
  ids.

## Tests

Fake TCP stub that decodes inbound Metadata and records the
`topics` list:

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

| Case | Expect |
|------|--------|
| `Metadata()` / `metadata()` | inbound topics is empty (all) |
| `MetadataTopics([]string{"events"})` / `metadata(List.of("events"))` | inbound topics is `["events"]` |
| Empty list | same empty list as the no-arg method |
| No-arg method with queued Timeout then ok | still retries (v0.95 inherit) |

Existing Metadata retry tests in `client_test.go` /
`ClientTest.java` must still pass.

| File | What |
|------|------|
| `clients/go/client.go` | `Metadata` wraps `MetadataTopics` |
| `clients/java/src/main/java/io/volant/Client.java` | `metadata()` wraps `metadata(List)` |
| `clients/go/metadata_topics_test.go` | fake TCP stub |
| `clients/java/src/test/java/io/volant/MetadataTopicsTest.java` | fake TCP stub |
| `docs/V116_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** Metadata versions / `allow_auto_topic_creation` /
  topic ids.
- **Empty still means all** (native opcode 4).
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust are unchanged (already have the field).
- CreateTopic (v0.117), OffsetFetch (v0.118), and CommitOffsets
  (v0.119) are unchanged.
- Retry policy is unchanged (v0.95 inherit).
- Native Metadata still has no top-level `error_code`.
- Broker / protocol are frozen.

## Merge notes

Sibling slice **v0.117** also edits `client.go` / `Client.java`.
When merging:

- **Keep `Metadata()` / `metadata()` as a wrapper** around the
  filtered method.
- Keep the filtered method using the existing Metadata retry loop.
- Do **not** wrap CreateTopic (v0.117), OffsetFetch (v0.118), or
  CommitOffsets (v0.119).
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `Metadata` / new
  `MetadataTopics`
- `clients/java/src/main/java/io/volant/Client.java` — hunk is
  local to `metadata` / new `metadata(List)`

## Related

- [V114_SPEC.md](./V114_SPEC.md) — Rust `metadata_topics` this
  mirrors
- [V95_SPEC.md](./V95_SPEC.md) — language Metadata / ListMembers
  retry this inherits
- [V96_SPEC.md](./V96_SPEC.md) — Rust Metadata / ListMembers retry
- [V77_SPEC.md](./V77_SPEC.md) — Metadata `controller_id` trailer
- [PHASE2_SPEC.md](./PHASE2_SPEC.md) — native Metadata `topics`
  field
