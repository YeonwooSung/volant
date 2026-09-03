# v0.127 — Go/Java public JoinGroup with instance id

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V28_SPEC.md](./V28_SPEC.md) /
[V33_SPEC.md](./V33_SPEC.md) / [V36_SPEC.md](./V36_SPEC.md): Go public
`JoinGroup` and Java public `joinGroup(group, topics, timeout)` always
send empty `group_instance_id`. Python already has
`join_group(..., group_instance_id=)`. Rust already has
`join_group_with_instance`. GroupConsumer static membership already
uses the unexported / package-private path.

Expose instance id on the thin Client without breaking existing public
signatures. Empty instance id stays “dynamic membership”. This is
**not** Kafka JoinGroup `group.instance.id`.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, or Rust.

## Goals

1. **Go:** public `func (c *Client) JoinGroupWithInstance(group string,
   topics []string, sessionTimeoutMs int, instanceID string)
   (JoinGroupResult, error)` calling existing
   `joinGroup(group, "", topics, sessionTimeoutMs, instanceID)`.
   Keep `JoinGroup`.
2. **Java:** public `JoinGroupResult joinGroupWithInstance(String group,
   List<String> topics, int sessionTimeoutMs, String groupInstanceId)`
   calling the existing 5-arg package-private `joinGroup`. Keep
   `joinGroup(group, topics, timeout)`. Do **not** add a new
   `joinGroup(..., String)` that collides with instanceId vs assignor
   vs memberId (historical `joinWithAssignor` collision).
3. Empty instance id stays dynamic membership (current public API).
4. Do **not** add JoinGroup retry (not idempotent).
5. Do **not** wrap CreateTopic, OffsetCommit, Produce, or acks.
6. Do **not** change Python, Rust, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `join_group(..., group_instance_id=)` | Already public |
| Rust `join_group_with_instance` | Already public |
| GroupConsumer static membership | Already uses the private path (v0.36) |
| JoinGroup retry | Not idempotent |
| Kafka JoinGroup `group.instance.id` | Native opcode 8 trailer only |
| CreateTopic / OffsetCommit / Produce / acks | Out of scope |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; trailer already on the wire |
| Phase 155 / homemade Raft | Frozen |

## API

```go
j, _ := c.JoinGroup("g", []string{"t"}, 10000)                    // empty instance
j, _ = c.JoinGroupWithInstance("g", []string{"t"}, 10000, "inst-1")
j, _ = c.JoinGroupWithInstance("g", []string{"t"}, 10000, "")     // same as JoinGroup
```

```java
JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000); // empty instance
j = c.joinGroupWithInstance("g", List.of("t"), 10000, "inst-1");
j = c.joinGroupWithInstance("g", List.of("t"), 10000, "");  // same as joinGroup
```

Existing public signatures stay. Java does **not** add a 4-arg
`joinGroup(..., String)` (memberId / assignor / instanceId collision).

## Semantics

- Empty instance id = dynamic membership (same as today).
- Non-empty instance id is encoded on the JoinGroup request (Phase 12
  trailer). Broker already treats it as static membership.
- First join still sends empty `member_id` (broker assigns one).
- `sessionTimeoutMs` 0 still defaults to 10000.
- JoinGroup is **not** retried.
- Not Kafka JoinGroup versions / `group.instance.id`.

## Tests

Fake TCP stub that records decoded JoinGroup `group_instance_id`.

| Case | Expect |
|------|--------|
| Existing `JoinGroup` / `joinGroup(g, topics, timeout)` | stub decodes empty instance id |
| New method with a non-empty id | stub decodes that instance id |
| New method with empty instance | matches the old public API (empty) |

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** JoinGroup versions / `group.instance.id`.
- Native opcode **8** trailer only. Broker / protocol unchanged.
- Empty instance id stays dynamic.
- JoinGroup is still not retried (not idempotent).
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust are unchanged.
- GroupConsumer static membership still uses the private path.

## Merge notes

Sibling slices that also edit Go/Java `Client` should keep this wrap
local to JoinGroup:

- **Keep the public JoinGroupWithInstance /
  joinGroupWithInstance wrapper only.** Do not change the unexported
  / package-private `joinGroup` send path.
- Do **not** add a Java `joinGroup(..., String)` overload.
- Do not wrap CreateTopic, OffsetCommit, Produce, or acks.
- Do not add JoinGroup retry.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (public `JoinGroupWithInstance` next to
  `JoinGroup`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`joinGroupWithInstance` next to `joinGroup`)

The hunk is local to JoinGroup.

## Related

- [V28_SPEC.md](./V28_SPEC.md) — Python/Go/Java thin JoinGroup
- [V33_SPEC.md](./V33_SPEC.md) — Java join still sent empty instance
- [V36_SPEC.md](./V36_SPEC.md) — GroupConsumer static membership
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — `group_instance_id` trailer
