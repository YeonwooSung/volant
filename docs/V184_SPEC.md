# v0.184 — Go/Java GroupConsumer assignor getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V41_SPEC.md](./V41_SPEC.md) /
[V69_SPEC.md](./V69_SPEC.md) / [V73_SPEC.md](./V73_SPEC.md): Rust
already has `GroupConsumer::assignor()` and Python already has
`GroupConsumer.assignor`. Go and Java store the join-time assignor
(`"broker"` / `"range"`) but have no public getter.

Expose the stored assignor. Do **not** change join / range assignor
behavior.

This is residual **v0.184**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
or Python.

## Goals

1. **Go:** public `Assignor() string`. Return the stored field
   (`g.assignor`). Nil receiver is `""`.
2. **Java:** public `String assignor()`. Return the stored field
   (`assignor`).
3. Python / Rust already covered. Do not change Python or Rust.
4. Do **not** change join / range assignor behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change join / range assignor behavior | Frozen; getter only |
| SyncGroup / sticky / cooperative assignor | Frozen; still no SyncGroup |
| Python / Rust getters | Already shipped |
| Kafka consumer assignor API | Native JoinGroup / DescribeGroup only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// Assignor is the join-time assignor ("broker" or "range").
func (g *GroupConsumer) Assignor() string {
    if g == nil {
        return ""
    }
    return g.assignor
}
```

```java
/** Join-time assignor ({@code broker} or {@code range}). */
public String assignor() {
    return assignor;
}
```

```go
g, _ := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithAssignor("range"))
_ = g.Assignor() // "range"
g, _ = volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
_ = g.Assignor() // "broker"
```

```java
GroupConsumer g = GroupConsumer.joinWithAssignor(backend, "g", List.of("t"), 10_000, "range");
g.assignor(); // "range"
g = GroupConsumer.join(backend, "g", List.of("t"), 10_000);
g.assignor(); // "broker"
```

Existing join / `WithAssignor` / `joinWithAssignor` signatures are
unchanged.

## Semantics

- Getters read stored fields only. They do **not** send JoinGroup or
  DescribeGroup.
- Default join (omitted / empty / `"broker"`) returns **`"broker"`**.
- After `WithAssignor("range")` / `joinWithAssignor(..., "range")`,
  the getter returns **`"range"`**.
- Join / range override / DescribeGroup fallback stay unchanged.
- Not a Kafka consumer assignor.

## Tests

Existing assignor fakes (same scripted brokers / FakeBackend as
v0.69):

| Case | Expect |
|------|--------|
| `WithAssignor("range")` / `joinWithAssignor(..., "range")` | getter is `"range"` |
| Default join | getter is `"broker"` |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** change Python / Rust / broker / protocol. Do **not** run
full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** Local `"range"` still uses DescribeGroup
  members.
- Getter only. Join / range behavior is unchanged.
- Python `assignor` and Rust `assignor()` already exist.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language `GroupConsumer` should keep
this hunk local to the getters:

- **Keep getters as a read of stored fields.** Do not change join.
- Do not change range / DescribeGroup fallback.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/group.go` (`AutoOffsetReset`)
- Java `clients/java/src/main/java/io/volant/GroupConsumer.java`
  (`autoOffsetReset`)
- `clients/go/assignor_test.go`
- `clients/java/src/test/java/io/volant/RangeAssignorTest.java`

The hunk is local to the getters + existing assignor tests.

## Related

- [V41_SPEC.md](./V41_SPEC.md) — client-side range assignor
- [V69_SPEC.md](./V69_SPEC.md) — language multi-member range via
  DescribeGroup
- [V73_SPEC.md](./V73_SPEC.md) — Rust GroupConsumer range +
  `assignor()`
- [V160_SPEC.md](./V160_SPEC.md) — leftover getter pattern
