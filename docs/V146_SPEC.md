# v0.146 — Java public JoinGroup member + instance

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V131_SPEC.md](./V131_SPEC.md) /
[V127_SPEC.md](./V127_SPEC.md): Go already has public
`JoinGroupMemberInstance(group, memberID, topics, timeout, instanceID)`.
Python `join_group(..., member_id=, group_instance_id=)` already sends
both. Java has `joinGroupWithInstance` (empty member) and
`joinGroupMember` (empty instance) but no named method that sends
both. The private 5-arg `joinGroup` already exists.

Add a public named wrapper only. This is **not** Kafka JoinGroup
rejoin / static membership.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Python, Go, or Rust.

## Goals

1. **Java:** public `JoinGroupResult joinGroupMemberWithInstance(String
   group, String memberId, List<String> topics, int sessionTimeoutMs,
   String groupInstanceId)` calling the existing 5-arg
   package-private `joinGroup(group, memberId, topics,
   sessionTimeoutMs, groupInstanceId)`.
2. Keep `joinGroup(group, topics, timeout)` /
   `joinGroupWithInstance` / `joinGroupMember` unchanged.
3. Do **not** add a new `joinGroup(..., String)` that collides with
   memberId vs assignor vs instanceId.
4. Empty `memberId` stays first join. Empty `groupInstanceId` stays
   dynamic membership.
5. Do **not** add JoinGroup retry (not idempotent).
6. Do **not** change Go, Python, Rust, broker, or protocol.

## Non-goals

| Deferred | Why |
|----------|-----|
| Go `JoinGroupMemberInstance` | Already public (v0.131) |
| Python `join_group(..., member_id=, group_instance_id=)` | Already public |
| Rust `join_group` member + instance | Already public |
| GroupConsumer rejoin / static | Already uses the private path |
| JoinGroup retry | Not idempotent |
| Kafka JoinGroup rejoin / member.assignment | Native opcode 8 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; both fields already on the wire |
| Phase 155 / homemade Raft | Frozen |

## API

```java
JoinGroupResult j = c.joinGroup("g", List.of("t"), 10000); // empty member + instance
j = c.joinGroupWithInstance("g", List.of("t"), 10000, "inst-1"); // empty member
j = c.joinGroupMember("g", "m-1", List.of("t"), 10000);          // empty instance
j = c.joinGroupMemberWithInstance("g", "m-1", List.of("t"), 10000, "inst-1");
j = c.joinGroupMemberWithInstance("g", "", List.of("t"), 10000, ""); // first join, dynamic
```

Existing public signatures stay. Java does **not** add a 4-arg
`joinGroup(..., String)` (memberId / assignor / instanceId collision).

## Semantics

- Empty `memberId` = first join (broker assigns one; same as today).
- Empty `groupInstanceId` = dynamic membership (same as `joinGroupMember`).
- Non-empty ids are encoded on the JoinGroup request.
- `joinGroup` / `joinGroupWithInstance` still send empty `memberId`.
- `joinGroupMember` still sends empty `groupInstanceId`.
- `sessionTimeoutMs` 0 still defaults to 10000.
- JoinGroup is **not** retried.
- Not Kafka JoinGroup versions / `member.id` rejoin / static membership.

## Tests

Fake TCP stub that records decoded JoinGroup `member_id` and
`group_instance_id`. Existing v0.131 member-only cases stay.

| Case | Expect |
|------|--------|
| Existing `joinGroupMember` | stub decodes that member_id; empty instance |
| New method with member `m-1` and instance `inst-1` | stub decodes both |
| New method with empty instance | stub decodes empty instance (same as `joinGroupMember`) |

```bash
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- **Not Kafka** JoinGroup versions / rejoin / static membership.
- Native opcode **8** only. Broker / protocol unchanged.
- Empty `memberId` stays first join. Empty `groupInstanceId` stays dynamic.
- JoinGroup is still not retried (not idempotent).
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Go, Python, and Rust are unchanged.
- GroupConsumer still uses the private path.

## Merge notes

Sibling slices that also edit Java `Client` should keep this wrap
local to JoinGroup:

- **Keep the public `joinGroupMemberWithInstance` wrapper only.**
  Do not change the package-private `joinGroup` send path.
- Do **not** add a Java `joinGroup(..., String)` overload.
- Do not add JoinGroup retry.
- Do not change Go, Python, Rust, broker, or protocol.

Expect conflicts on:

- Java `clients/java/src/main/java/io/volant/Client.java`
  (`joinGroupMemberWithInstance` next to `joinGroupMember`)
- Java `clients/java/src/test/java/io/volant/JoinGroupMemberTest.java`

The hunk is local to JoinGroup.

## Related

- [V28_SPEC.md](./V28_SPEC.md) — Python/Go/Java thin JoinGroup
- [V127_SPEC.md](./V127_SPEC.md) — Go/Java public instance id
- [V131_SPEC.md](./V131_SPEC.md) — Go/Java public member rejoin
- [V36_SPEC.md](./V36_SPEC.md) — GroupConsumer static membership
- [PHASE12_SPEC.md](./PHASE12_SPEC.md) — `group_instance_id` trailer
