# v0.189 — Go/Java GroupConsumer sessionTimeoutMs getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V32_SPEC.md](./V32_SPEC.md) /
[V33_SPEC.md](./V33_SPEC.md) / [V31_SPEC.md](./V31_SPEC.md): Python
already has `GroupConsumer.session_timeout_ms`. Go and Java store the
join-time session timeout (0 defaults to 10000) but have no public
getter.

Expose the stored timeout. Do **not** change join / heartbeat
interval.

This is residual **v0.189**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
or Python.

## Goals

1. **Go:** public `SessionTimeoutMs() uint32`. Return the stored field
   (`g.sessionTimeoutMs`). Nil receiver is `0`. The field is already
   the defaulted value after join (0 → 10000).
2. **Java:** public `int sessionTimeoutMs()`. Return the stored field
   (`sessionTimeoutMs`). Join already writes the defaulted timeout
   (0 → 10000) into the field.
3. Python already covered. Do not change Python or Rust.
4. Do **not** change join / heartbeat interval.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change join / heartbeat interval | Frozen; getter only |
| SyncGroup / sticky / cooperative assignor | Frozen; still no SyncGroup |
| Python / Rust getters | Python already shipped; Rust not in this slice |
| Kafka `session.timeout.ms` consumer config | Native JoinGroup only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// SessionTimeoutMs is the join-time session timeout in milliseconds
// (0 was defaulted to 10000 at join).
func (g *GroupConsumer) SessionTimeoutMs() uint32 {
    if g == nil {
        return 0
    }
    return g.sessionTimeoutMs
}
```

```java
/** Join-time session timeout in milliseconds (0 was defaulted to 10000). */
public int sessionTimeoutMs() {
    return sessionTimeoutMs;
}
```

```go
g, _ := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
_ = g.SessionTimeoutMs() // 10000
g, _ = volant.JoinGroupConsumer(c, "g", []string{"t"}, 0)
_ = g.SessionTimeoutMs() // 10000
```

```java
GroupConsumer g = GroupConsumer.join(backend, "g", List.of("t"), 10_000);
g.sessionTimeoutMs(); // 10000
g = GroupConsumer.join(backend, "g", List.of("t"), 0);
g.sessionTimeoutMs(); // 10000
```

Existing join signatures are unchanged.

## Semantics

- Getters read stored fields only. They do **not** send JoinGroup or
  Heartbeat.
- After join with `10_000`, the getter returns **10000**.
- After join with `0`, the getter returns **10000** (join already
  defaulted the stored field).
- Join / heartbeat interval (`sessionTimeoutMs / 3`, clamped
  100–3000 ms) stay unchanged.
- Not a Kafka `session.timeout.ms` consumer config.

## Tests

Existing join fakes (same scripted brokers / FakeBackend as
v0.32 / v0.33):

| Case | Expect |
|------|--------|
| Join with `10_000` | getter is `10000` |
| Join with `0` | getter is `10000` |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** change Python / Rust / broker / protocol. Do **not** run
full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** Local `"range"` still uses DescribeGroup
  members.
- Getter only. Join / heartbeat interval are unchanged.
- Python `session_timeout_ms` already exists.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language `GroupConsumer` should keep
this hunk local to the getters:

- **Keep getters as a read of stored fields.** Do not change join.
- Do not change heartbeat interval (`sessionTimeoutMs / 3`).
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/group.go` (`Assignor`)
- Java `clients/java/src/main/java/io/volant/GroupConsumer.java`
  (`assignor`)
- `clients/go/group_test.go`
- `clients/java/src/test/java/io/volant/GroupConsumerTest.java`

The hunk is local to the getters + existing session-timeout tests.

## Related

- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
- [V37_SPEC.md](./V37_SPEC.md) — background heartbeat interval
- [V184_SPEC.md](./V184_SPEC.md) — leftover getter pattern (assignor)
