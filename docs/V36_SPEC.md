# v0.36 — static membership on GroupConsumers

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “high-level assignor / **static membership**
on clients.” Rust already has `GroupConsumer::join_static(...,
group_instance_id)`. Python’s thin `join_group(...,
group_instance_id=)` and `GroupConsumer.join(...,
group_instance_id=)` already existed; Go/Java constructors did not
expose it. This slice surfaces the Phase 12 trailer on all three
high-level consumers.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker.

## Goals

1. **Python** `GroupConsumer.join(..., group_instance_id=)` already
   stored and resent the id; add unit coverage that the join request
   includes it and a rejoin after error 9 keeps it.
2. **Go** `JoinGroupConsumerStatic(c, group, topics, timeout, instanceID)`
   (empty id = same as `JoinGroupConsumer`).
3. **Java** `GroupConsumer.joinStatic(c, group, topics, timeout, instanceId)`
   (empty id = same as `join`).
4. Re-join after heartbeat error **9** (and 10 / 11) resends the same
   instance id. Rust already does this.
5. Unit / fake tests: join request includes `group_instance_id`; rejoin
   keeps it. Optional live e2e (`VOLANT_E2E=1`) asserts broker
   `member_id = static:{id}`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Client-side assignor | Broker still assigns; clients honor it |
| Thin `Client.JoinGroup` defaulting to static | High-level constructors only; thin RPC stays empty unless the caller passes an id |
| Kafka JoinGroup `group.instance.id` | Native opcode 8 trailer only |
| OffsetCommit / LeaveGroup instance trailer on clients | Join is the Phase 12 member-id source |
| Broker / protocol / Rust client changes | Wire and `join_static` already exist |
| Required CI language job | Existing optional smoke scripts only |

## API

```python
GroupConsumer.join(c, "g", ["t"], 10_000, group_instance_id="inst-1")
```

```go
JoinGroupConsumerStatic(c, "g", []string{"t"}, 10000, "inst-1")
```

```java
GroupConsumer.joinStatic(c, "g", List.of("t"), 10_000, "inst-1");
```

Empty instance id is dynamic membership (today’s default). The stored
id is sent on every JoinGroup, including the first join (empty
`member_id`) and a rejoin after error 9/10/11 (assigned `member_id`
kept). Broker Phase 12 still derives `member_id = static:{id}` when
the instance is non-empty and `member_id` is empty.

Accessors: Python `g.group_instance_id`, Go `g.GroupInstanceID()`,
Java `g.groupInstanceId()`.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Fake Client: join includes instance id; default empty; rejoin on 9 keeps it |
| `clients/go/group_test.go` | Fake TCP: `JoinGroupConsumerStatic` sends instance; default empty; rejoin keeps it |
| `clients/java/src/test/java/io/volant/GroupConsumerTest.java` | Mock backend: `joinStatic` sends instance; default empty; rejoin keeps it |
| each language e2e | Optional `VOLANT_E2E=1`: live join with `inst-1` → `member_id = static:inst-1` |

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- No client-side assignor. The broker’s assignment is still
  authoritative.
- Thin `Client.join_group` / `JoinGroup` / `joinGroup` still default to
  empty `group_instance_id`. Only `GroupConsumer` constructors (and
  Python’s existing thin kwarg) expose static membership.
- OffsetCommit / LeaveGroup on these clients still do not send an
  instance trailer. Member identity on rejoin is the stored
  `member_id` plus the same `group_instance_id` on JoinGroup.
- Not Kafka KIP-345 (`group.instance.id` on the Kafka shim). Native
  opcode 8 only.
- `poll` is still one heartbeat + one fetch pass. Sync only; one TCP
  connection; not concurrent-safe.
- Still no Kafka-wire SDK, SCRAM / shared-token auth, or leader
  redirect on these clients.
- Broker and Rust `volant-client` are unchanged.

See [clients/python/README.md](../clients/python/README.md),
[clients/go/README.md](../clients/go/README.md),
[clients/java/README.md](../clients/java/README.md),
[PHASE12_SPEC.md](./PHASE12_SPEC.md),
[V31_SPEC.md](./V31_SPEC.md),
[V32_SPEC.md](./V32_SPEC.md), and
[V33_SPEC.md](./V33_SPEC.md).
