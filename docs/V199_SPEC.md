# v0.199 — CreateTopic default partitions=1 helpers

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from Python `create_topic(name)`
(already `partitions=1`): Go `CreateTopic(name, partitions)` and
Java `createTopic(name, partitions)` both require an explicit
partition count.

Add named / overload helpers that default partitions to **1**. Do
**not** change Go `CreateTopic` return type (still error-only;
`CreateTopicID` already returns the id).

This is residual **v0.199**. It is **not** Phase 155. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change
homemade Raft, or change the broker / protocol / Python / Rust.

## Goals

1. **Go:** public `func (c *Client) CreateTopicDefault(name string) error`
   that calls `c.CreateTopic(name, 1)`. Returns error only (same as
   `CreateTopic`). Place it immediately after `CreateTopic` (before
   `CreateTopicID`).
2. **Java:** public `int createTopic(String name)` that calls
   `createTopic(name, 1)`. Returns the topic id (same as the 2-arg
   overload). Named overload — do **not** invent
   `createTopicDefault`.
3. Python already has `partitions=1`. Do **not** change Python.
4. Rust still requires partitions. Do **not** change Rust in this
   slice.
5. Do **not** change `CreateTopic` / `CreateTopicID` /
   `CreateTopicWithConfigs` / 2-arg / 3-arg `createTopic`
   signatures.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Go `CreateTopic` to return topic id | API break; use `CreateTopicID` |
| Python `create_topic(name)` default | Already `partitions=1` |
| Rust `create_topic` default partitions | Still requires explicit count |
| Kafka CreateTopics default partitions / replication | Native opcode 3 only |
| Change `CreateTopic` / 2-arg / 3-arg signatures | Frozen; helpers only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// CreateTopicDefault creates a topic with 1 partition.
// Same as CreateTopic(name, 1). Returns error only (CreateTopicID
// still returns the topic id).
func (c *Client) CreateTopicDefault(name string) error {
    return c.CreateTopic(name, 1)
}
```

```java
/** Create a topic with 1 partition. Same as {@link #createTopic(String, int)}. */
public int createTopic(String name) {
    return createTopic(name, 1);
}
```

```go
if err := c.CreateTopicDefault("events"); err != nil {
    log.Fatal(err)
}
if err := c.CreateTopic("events", 3); err != nil { // unchanged: explicit
    log.Fatal(err)
}
id, err := c.CreateTopicID("events", 1) // still the id path
```

```java
int id = c.createTopic("events");      // partitions=1; returns topic id
int id2 = c.createTopic("events", 3);  // unchanged: explicit
```

Existing `CreateTopic` / `CreateTopicID` / `CreateTopicWithConfigs`
/ 2-arg / 3-arg `createTopic` signatures are unchanged.

## Semantics

- Partitions is **1**, same as Python `create_topic(name)` default.
- Inherit error 14 / transient retry from existing `CreateTopic` /
  `createTopic` / `adminRoundTrip`. No new retry policy.
- Go helper is error-only (do **not** change `CreateTopic` to return
  the id — that is an API break; use `CreateTopicID(name, 1)` if the
  caller wants the id).
- Java overload returns the topic id like the existing overloads.
- Not Kafka CreateTopics default partitions / replication.

## Tests

Fake TCP stub (same `adminBroker` / `AdminBroker` as
`TestCreateTopicSendsEmptyConfigs` / `createTopicSendsEmptyConfigs`):

| Case | Expect |
|------|--------|
| Go `CreateTopicDefault("events")` | encodes partitions=1 |
| Java `createTopic("events")` | encodes partitions=1; returns topic id |
| Existing `CreateTopic` / 2-arg `createTopic` tests | still pass |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** change broker / protocol / Rust / Python. Do **not** run
Python discover. Do **not** append codec tests. Do **not** run cargo
workspace.

## Honesty leftovers

- Go `CreateTopic` still discards the topic id (use `CreateTopicID`).
- Rust still requires an explicit partition count.
- Python already defaulted partitions=1.
- Not Kafka CreateTopics.
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Sibling v0.200 also edits Go `client.go` / Java `Client.java` / both
READMEs. Keep this hunk local to the CreateTopic helper / overload.
Do not change Dial / Auth / getters.

Expect conflicts on:

- `clients/go/client.go`
- `clients/go/README.md`
- `clients/java/src/main/java/io/volant/Client.java`
- `clients/java/README.md`

Keep both sides on conflict (orchestrator will merge).

The hunk is local to the CreateTopic helper / overload + fake-TCP
tests + README usage lines.

## Related

- [V117_SPEC.md](./V117_SPEC.md) — Go/Java CreateTopic configs
- [V126_SPEC.md](./V126_SPEC.md) — Go CreateTopicID returns topic id
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V72_SPEC.md](./V72_SPEC.md) — language admin error 14
