# v0.181 — language single-topic MetadataTopic

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V114_SPEC.md](./V114_SPEC.md) /
[V116_SPEC.md](./V116_SPEC.md): Go `MetadataTopics([]string)`,
Java `metadata(List<String> topics)`, and Python
`metadata(topics: Optional[list[str]] = None)` already take a topic
list (empty = all). There is no named one-topic helper matching
`FetchOffset` / `DeleteOffset`.

Add `MetadataTopic` / `metadataTopic` / `metadata_topic`. Reuse the
existing list method (do not reimplement the RPC).
`Metadata` / `MetadataTopics` / `metadata()` / `metadata(topics)`
stay unchanged. This is **not** Kafka Metadata
`allow_auto_topic_creation` / topic ids.

This is residual **v0.181** (language single-topic MetadataTopic). It
is **not** Phase 155. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or Rust.

## Goals

1. **Go:** public `func (c *Client) MetadataTopic(topic string)
   (Metadata, error)` that calls `MetadataTopics([]string{topic})`.
2. **Java:** named public `Metadata metadataTopic(String topic)` that
   calls `metadata(Collections.singletonList(topic))`. Do not collide
   with `metadata()` / `metadata(List)`.
3. **Python:** public `def metadata_topic(self, topic: str) ->
   MetadataResponse` that calls `self.metadata([topic])`.
4. Inherit retry / error **14** from the existing list method (v0.95
   transient retry + v0.156 error 14). No new retry policy.
5. Do **not** change `Metadata` / `MetadataTopics` / `metadata()` /
   `metadata(topics)`.
6. Do **not** change broker / protocol / Rust.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `Metadata` / `MetadataTopics` / `metadata()` / `metadata(topics)` | Frozen; list still accepts one or many |
| Kafka Metadata `allow_auto_topic_creation` / topic ids | Native opcode 4 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Rust `metadata_topic` | Frozen; `metadata_topics` already public |
| Phase 155 / homemade Raft | Frozen |

## API

```go
func (c *Client) MetadataTopic(topic string) (Metadata, error) {
    return c.MetadataTopics([]string{topic})
}
```

```java
public Metadata metadataTopic(String topic) {
    return metadata(Collections.singletonList(topic));
}
```

```python
def metadata_topic(self, topic: str) -> MetadataResponse:
    return self.metadata([topic])
```

```go
meta, _ := c.MetadataTopic("events")                         // one topic
meta, _ = c.MetadataTopics([]string{"events"})
meta, _ = c.Metadata()                                       // unchanged: all
meta, _ = c.MetadataTopics(nil)                              // unchanged: all
```

```java
Metadata meta = c.metadataTopic("events");                   // one topic
meta = c.metadata(List.of("events"));
meta = c.metadata();                                         // unchanged: all
meta = c.metadata(Collections.emptyList());                  // unchanged: all
```

```python
meta = c.metadata_topic("events")                            # one topic
meta = c.metadata(["events"])
meta = c.metadata()                                          # unchanged: all
```

## Semantics

- One-topic helpers send wire topic count **1** with that name.
- They do not re-encode; they wrap the existing list method.
- Return type matches `MetadataTopics` / `metadata` / `metadata(topics)`
  (`Metadata` / `Metadata` / `MetadataResponse`).
- `Metadata` / `MetadataTopics` / `metadata()` / `metadata(topics)`
  are unchanged (nil/empty still mean all).
- Transient 6 / 7 / 15 / 16 and transport retry via the list method
  (v0.95; default `max_retries=0`).
- Error 14 follows `max_redirects` (v0.156).
- Not Kafka Metadata `allow_auto_topic_creation` / topic ids.

## Tests

Fake TCP stub that records decoded Metadata topics (same helpers
as existing `TestMetadataTopicsEncodesNamedFilter` /
`metadataListEncodesNamedFilter`). Existing empty-all and retry
tests stay green.

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
(cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_metadata_topic -q)
```

Do **not** run full Python `discover`.

| Case | Expect |
|------|--------|
| `MetadataTopic` / `metadataTopic` / `metadata_topic` (`"events"`) | wire topics count 1; name `events` |
| Existing `Metadata` / `MetadataTopics` empty / named / retry cases | still pass |

Existing Metadata retry / 14 tests must still pass
(`MetadataTopics` / `metadata` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `MetadataTopic` wraps `MetadataTopics` with one topic |
| `clients/go/metadata_topics_test.go` | one-topic wire check near `TestMetadataTopicsEncodesNamedFilter` |
| `clients/go/README.md` | usage line + one prose sentence |
| `clients/java/src/main/java/io/volant/Client.java` | named `metadataTopic` wraps `metadata(List)` with one topic |
| `clients/java/src/test/java/io/volant/MetadataTopicsTest.java` | one-topic wire check near `metadataListEncodesNamedFilter` |
| `clients/java/README.md` | usage line + one prose sentence |
| `clients/python/src/volant/client.py` | `metadata_topic` wraps `metadata` with one name |
| `clients/python/tests/test_metadata_topic.py` | one-topic wire check (dedicated; reuses Metadata fake-TCP) |
| `clients/python/README.md` | usage line + one prose sentence |
| `docs/V181_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** Metadata `allow_auto_topic_creation` / topic ids.
- Empty topics still fetch **all** topics.
- `Metadata` / `MetadataTopics` / `metadata()` / `metadata(topics)`
  are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- Client version stays **0.2.0**.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the one-topic wrappers:

- **Keep the wrappers only.** Do not change `Metadata` /
  `MetadataTopics` / `metadata()` / `metadata(topics)`.
- Do not change the Metadata send loop (v0.95 retry + v0.156 14).
- Do not change Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `MetadataTopic` after
  `MetadataTopics`
- Java `clients/java/src/main/java/io/volant/Client.java` —
  named `metadataTopic` after `metadata(List)`
- Python `clients/python/src/volant/client.py` — `metadata_topic`
  after `metadata`
- `clients/*/README.md` and the existing Metadata test files
  (`metadata_topics_test.go` / `MetadataTopicsTest.java`)

## Related

- [V114_SPEC.md](./V114_SPEC.md) — Rust metadata_topics
- [V116_SPEC.md](./V116_SPEC.md) — language MetadataTopics
- [V95_SPEC.md](./V95_SPEC.md) — language Metadata transient retry
- [V156_SPEC.md](./V156_SPEC.md) — language Metadata error 14
- [V179_SPEC.md](./V179_SPEC.md) — language single-entry FetchOffset
