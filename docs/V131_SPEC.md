# v0.131 — Go/Java public JoinGroup rejoin (member_id)

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V28_SPEC.md](./V28_SPEC.md) /
[V127_SPEC.md](./V127_SPEC.md): Go public `JoinGroup` /
`JoinGroupWithInstance` and Java public `joinGroup` /
`joinGroupWithInstance` always send empty `member_id`. Python already
has `join_group(..., member_id=)`. Rust already has `join_group` take
`member_id`. GroupConsumer already uses the unexported /
package-private path.

Expose rejoin `member_id` on the thin Client without breaking existing
public signatures and **without** a new `joinGroup(..., String)` that
collides with instanceId vs memberId vs assignor. Empty `member_id`
stays “first join”. This is **not** Kafka JoinGroup rejoin.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, or Rust.

## Goals

1. **Go:** public `func (c *Client) JoinGroupMember(group, memberID
   string, topics []string, sessionTimeoutMs int) (JoinGroupResult,
   error)` calling existing `joinGroup(group, memberID, topics,
   sessionTimeoutMs, "")`. Optional thin
   `JoinGroupMemberInstance(..., instanceID)`. Keep `JoinGroup` /
   `JoinGroupWithInstance`.
2. **Java:** public `JoinGroupResult joinGroupMember(String group,
   String memberId, List<String> topics, int sessionTimeoutMs)`
   calling the existing 5-arg package-private `joinGroup(..., "")`.
   Keep `joinGroup(group, topics, timeout)` /
   `joinGroupWithInstance`. Do **not** add a new
   `joinGroup(..., String)` that collides with memberId vs assignor
   vs instanceId.
3. Empty `member_id` stays first join (current public API).
4. Do **not** add JoinGroup retry (not idempotent).
5. Do **not** wrap Produce, Heartbeat, or timestamp/headers
   (siblings v0.132–v0.135).
6. Do **not** change Python, Rust, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Python `join_group(..., member_id=)` | Already public |
| Rust `join_group(..., member_id)` | Already public |
| GroupConsumer rejoin | Already uses the private path |
| JoinGroup retry | Not idempotent |
| Kafka JoinGroup rejoin / member.assignment | Native opcode 8 only |
| Produce / Heartbeat / timestamp / headers | Sibling residuals |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `member_id` already on the wire |
| Phase 155 / homemade Raft | Frozen |

## API

```go
j, _ := c.JoinGroup("g", []string{"t"}, 10000)                    // empty member_id
j, _ = c.JoinGroupMember("g", "m-1", []string{"t"}, 10000)
j, _ = c.JoinGroupMember("g", "", []string{"t"}, 10000)           // same as JoinGroup
j, _ = c.JoinGroupMemberInstance("g", "m-1", []string{"t"}, 10000, "inst-1")
```

```java
JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000); // empty memberId
j = c.joinGroupMember("g", "m-1", List.of("t"), 10000);
j = c.joinGroupMember("g", "", List.of("t"), 10000);       // same as joinGroup
```

Existing public signatures stay. Java does **not** add a 4-arg
`joinGroup(..., String)` (memberId / assignor / instanceId collision).

## Semantics

- Empty `member_id` = first join (broker assigns one; same as today).
- Non-empty `member_id` is encoded on the JoinGroup request (rejoin).
- `JoinGroup` / `JoinGroupWithInstance` still send empty `member_id`.
- `sessionTimeoutMs` 0 still defaults to 10000.
- JoinGroup is **not** retried.
- Not Kafka JoinGroup versions / `member.id` rejoin.

## Tests

Fake TCP stub that records decoded JoinGroup `member_id`.

| Case | Expect |
|------|--------|
| Existing `JoinGroup` / `joinGroup(g, topics, timeout)` | stub decodes empty member_id |
| New method with a non-empty id | stub decodes that member_id |
| New method with empty member_id | matches the old public API (empty) |

```bash
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** JoinGroup versions / rejoin.
- Native opcode **8** only. Broker / protocol unchanged.
- Empty `member_id` stays first join.
- JoinGroup is still not retried (not idempotent).
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Python and Rust are unchanged.
- GroupConsumer still uses the private path.

## Merge notes

Sibling slices that also edit Go/Java `Client` should keep this wrap
local to JoinGroup:

- **Keep the public JoinGroupMember / joinGroupMember wrapper only.**
  Do not change the unexported / package-private `joinGroup` send path.
- Do **not** add a Java `joinGroup(..., String)` overload.
- Do not wrap Produce, Heartbeat, or timestamp/headers.
- Do not add JoinGroup retry.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (public `JoinGroupMember` next to
  `JoinGroup` / `JoinGroupWithInstance`)
- Java `clients/java/src/main/java/io/volant/Client.java`
  (`joinGroupMember` next to `joinGroup` / `joinGroupWithInstance`)

The hunk is local to JoinGroup.

## Related

- [V28_SPEC.md](./V28_SPEC.md) — Python/Go/Java thin JoinGroup
- [V127_SPEC.md](./V127_SPEC.md) — Go/Java public instance id
- [V36_SPEC.md](./V36_SPEC.md) — GroupConsumer static membership
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — `group_instance_id` trailer
