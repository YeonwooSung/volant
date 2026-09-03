# v0.117 — Go/Java CreateTopic configs

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from Python `create_topic(..., configs=)` /
Rust `create_topic_with_configs`: Go `CreateTopic(name, partitions)`
always sent empty configs and discarded the topic id; Java
`createTopic(name, partitions)` always sent empty configs (it already
returns topic id).

Add an overload / new method. Keep the existing
`CreateTopic(name, partitions)` / `createTopic(name, partitions)`
signatures. Reuse `adminRoundTrip` so error **14** and transient retry
are inherited. This is **not** Kafka CreateTopics configs /
IncrementalAlterConfigs.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, or Rust.

## Goals

1. **Go:** `CreateTopicWithConfigs(name, partitions, configs [][2]string) (uint32, error)`
   via existing `adminRoundTrip`. Type-assert
   `codec.CreateTopicResponse` and return `TopicID`.
2. **Go:** `CreateTopic` calls it with nil/empty configs and discards
   the id (keep `error` return).
3. **Java:** `createTopic(name, partitions, List<String[]> configs)`
   matching AlterConfigs style. Still returns topic id.
4. **Java:** `createTopic(name, partitions)` calls it with
   `Collections.emptyList()`.
5. Configs are native CreateTopic pairs (same as Python/Rust). Empty
   value is allowed if the codec already allows it.
6. Do **not** wrap Metadata (sibling v0.116), OffsetFetch, or
   CommitOffsets.
7. Do **not** change broker / protocol. CreateTopic request already
   has a `configs` list.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `create_topic(..., configs=)` | Already shipped |
| Rust `create_topic_with_configs` | Already shipped |
| Kafka CreateTopics configs / IncrementalAlterConfigs | Native opcode 3 only |
| Metadata wrap (v0.116) | Sibling residual |
| OffsetFetch / CommitOffsets wrap | Sibling / already shipped |
| Broker / protocol / new opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |
| Change existing `CreateTopic(name, n)` / `createTopic(name, n)` signatures | Compatibility |

## API

```go
func (c *Client) CreateTopic(name string, partitions int) error {
    _, err := c.CreateTopicWithConfigs(name, partitions, nil)
    return err
}

func (c *Client) CreateTopicWithConfigs(name string, partitions int, configs [][2]string) (uint32, error)
```

```java
public int createTopic(String name, int partitions) {
    return createTopic(name, partitions, Collections.emptyList());
}

public int createTopic(String name, int partitions, List<String[]> configs)
```

```go
c.CreateTopic("events", 1)
id, _ := c.CreateTopicWithConfigs("events", 1, [][2]string{{"retention.ms", "1000"}})
```

```java
c.createTopic("events", 1);
int id = c.createTopic("events", 1, Collections.singletonList(new String[] {"retention.ms", "1000"}));
```

## Semantics

- Empty / nil configs encode as a zero-length Phase 13 trailer (same
  as today’s `CreateTopic(name, n)`).
- Named pairs are native CreateTopic `(key, value)` strings, not
  Kafka CreateTopics `CreatableTopicConfig` / IncrementalAlterConfigs.
- Error 14 (`NotController`) follows `maxRedirects` via
  `adminRoundTrip`. Transient 6 / 7 / 15 / 16 and TCP/IO follow
  `maxRetries` (default 0). 14 is not a retry.
- Go new method returns `CreateTopicResponse.TopicID`. Existing
  `CreateTopic` still returns only `error`.
- Java both overloads return topic id.

## Tests

Fake TCP. Stub records decoded CreateTopic configs. Existing
CreateTopic 14 / retry tests must still pass.

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| `CreateTopic(name, n)` / `createTopic(name, n)` | stub sees empty configs |
| With configs `[("retention.ms","1000")]` | stub sees those pairs |
| New method | returns the topic id from the response |
| Error 14 | still redirects (inherit `adminRoundTrip`) |

## Honesty leftovers

- **Not Kafka** CreateTopics configs / IncrementalAlterConfigs.
- Go `CreateTopic(name, n)` still discards topic id (signature
  unchanged).
- Empty value is allowed on the wire; broker validation is unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust are unchanged (already have configs).
- Metadata (v0.116), OffsetFetch, and CommitOffsets are unchanged.
- Broker / protocol are frozen.

## Merge notes

Sibling slice **v0.116** (Metadata wrap) also edits the Go/Java
`Client` files. When merging:

- **Keep the CreateTopic overload only.** Do not change Metadata,
  OffsetFetch, or CommitOffsets.
- Keep `CreateTopic` / `createTopic(name, n)` as wrappers that send
  empty configs.
- Keep `adminRoundTrip` as the only transport (14 + transient retry
  inherit).
- Do not change the broker, Kafka shim, Python, or Rust.

Expect conflicts on:

- Go `clients/go/client.go` — hunk is local to `CreateTopic` / new
  `CreateTopicWithConfigs`
- Java `clients/java/src/main/java/io/volant/Client.java` — hunk is
  local to `createTopic`
- Go / Java CreateTopic test stubs (record decoded configs + optional
  topic id)

The hunk is local to CreateTopic.

## Related

- [V13_SPEC.md](./V13_SPEC.md) — Phase 13 topic configs trailer
- [V72_SPEC.md](./V72_SPEC.md) — admin NotController 14 redirect
- [V93_SPEC.md](./V93_SPEC.md) — Describe/AlterConfigs 14
- Python `create_topic(..., configs=)` / Rust `create_topic_with_configs`
