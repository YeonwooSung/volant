# v0.186 — Go GroupConsumer HeartbeatCount

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V44_SPEC.md](./V44_SPEC.md) /
[V37_SPEC.md](./V37_SPEC.md): Rust already has
`GroupConsumer::heartbeat_count()` (Heartbeat RPCs issued by poll +
background, not JoinGroup). Go has no public counter.

Expose a stored attempt count. Do **not** change Poll / heartbeat /
JoinGroup behavior.

This is residual **v0.186**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java.

## Goals

1. **Go:** public `HeartbeatCount() uint64`. Return Heartbeat RPCs
   issued by `Poll` + background (`heartbeatOnce`). Nil receiver
   is `0`.
2. Increment **once per Heartbeat attempt** immediately before
   `g.client.Heartbeat(...)`, matching Rust `fetch_add` before
   `heartbeat()`.
3. Do **not** increment on JoinGroup.
4. Do **not** change Poll / heartbeat semantics except the counter.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Poll / heartbeat / JoinGroup | Frozen; counter only |
| Count JoinGroup / LeaveGroup | Frozen; Heartbeat attempts only |
| SyncGroup / JoinGroup retry | Frozen; still no SyncGroup |
| Python / Java / Rust counters | Rust already shipped; others out of scope |
| Kafka consumer heartbeat API | Native Heartbeat only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// HeartbeatCount is Heartbeat RPCs issued by Poll + background
// (not JoinGroup).
func (g *GroupConsumer) HeartbeatCount() uint64 {
    if g == nil {
        return 0
    }
    g.mu.Lock()
    defer g.mu.Unlock()
    return g.heartbeatCount
}
```

```go
g, _ := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000, volant.WithBackgroundHeartbeat(false))
_ = g.HeartbeatCount() // 0 after join
_, _ = g.Poll(0)
_ = g.HeartbeatCount() // 1 after one Poll
```

Existing join / `WithBackgroundHeartbeat` signatures are unchanged.

## Semantics

- Counter is an attempt count: increment immediately before the
  Heartbeat RPC, even if the RPC later fails.
- `Poll` increments once at the start of the call (after the closed
  check), then heartbeats as today.
- `heartbeatOnce` increments once when not closed, then heartbeats
  as today.
- JoinGroup / rejoin / LeaveGroup do **not** increment.
- Getter reads the stored field. It does **not** send Heartbeat.
- Nil receiver returns `0`.
- Not a Kafka `heartbeat.interval.ms` / metrics API.

## Tests

Existing group fakes (`fakeGroupBroker` in `group_test.go`):

| Case | Expect |
|------|--------|
| Join with `WithBackgroundHeartbeat(false)` | `HeartbeatCount() == 0` before Poll |
| After one `Poll` | `HeartbeatCount() == 1` |
| Background on + short session timeout | count becomes `>= 1` without Poll |
| Nil `*GroupConsumer` | `HeartbeatCount()` is `0` |

```bash
cd clients/go && go test ./...
```

Do **not** change Python / Rust / Java / broker / protocol. Do **not**
run full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** JoinGroup is still not retried.
- Counter only. Poll / heartbeat / join behavior is unchanged.
- Rust `heartbeat_count()` already exists.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `GroupConsumer` should keep this
hunk local to the counter:

- **Keep increments immediately before `Heartbeat`.** Do not count
  JoinGroup.
- Do not change Poll / heartbeat / rejoin.
- Do not change Python, Rust, Java, broker, or protocol.

Expect conflicts on:

- Go `clients/go/group.go` (`Poll` / `heartbeatOnce` / getters)
- `clients/go/group_test.go`
- `clients/go/README.md`

The hunk is local to the counter + existing group tests.

## Related

- [V37_SPEC.md](./V37_SPEC.md) — language background heartbeat
- [V44_SPEC.md](./V44_SPEC.md) — Rust GroupConsumer background
  heartbeat + `heartbeat_count()`
- [V184_SPEC.md](./V184_SPEC.md) — leftover getter pattern
