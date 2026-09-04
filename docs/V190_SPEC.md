# v0.190 — Go/Java GroupConsumer Leave alias

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from Rust `GroupConsumer::leave` and
Python `GroupConsumer.leave()` (alias for `close`): Go only has
`Close()`, Java only has `close()`.

Add `Leave` / `leave` as aliases. Do **not** change Close / close
behavior. Idempotent leave still goes through Close.

This is residual **v0.190**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
or Python.

## Goals

1. **Go:** public `Leave() error`. Delegate to `Close()`.
2. **Java:** public `void leave()`. Delegate to `close()`.
3. Python / Rust already covered. Do not change Python or Rust.
4. Do **not** change Close / close behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Close / close behavior | Frozen; alias only |
| SyncGroup / JoinGroup retry | Frozen; still no SyncGroup |
| Python `leave()` / Rust `leave()` | Already shipped |
| Kafka LeaveGroup (API key 13) | Native LeaveGroup opcode only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// Leave is an alias for Close (Rust GroupConsumer::leave).
func (g *GroupConsumer) Leave() error {
    return g.Close()
}
```

```java
/** Alias for {@link #close()} (Rust {@code GroupConsumer::leave}). */
public void leave() {
    close();
}
```

```go
g, _ := volant.JoinGroupConsumer(c, "g", []string{"t"}, 10_000)
_ = g.Leave()
_ = g.Leave() // idempotent; same as second Close
```

```java
GroupConsumer g = GroupConsumer.join(backend, "g", List.of("t"), 10_000);
g.leave();
g.leave(); // idempotent; same as second close()
```

Existing `Close()` / `close()` signatures are unchanged.

## Semantics

- `Leave` / `leave` delegate to `Close` / `close`. They do **not**
  send a second LeaveGroup path.
- After join, first `Leave` / `leave` leaves the group (same as
  Close / close).
- Second `Leave` / `leave` / `Close` / `close` is idempotent.
- Auto-commit-on-close, heartbeat stop, and `ErrGroupClosed` /
  closed-poll behavior stay on Close / close.
- Not a Kafka consumer `unsubscribe` / LeaveGroup API.

## Tests

Existing group fakes (same scripted brokers / FakeBackend as
`TestCloseLeavesGroup` / `pollAfterCloseThrows`):

| Case | Expect |
|------|--------|
| After join, `Leave()` / `leave()` | succeeds; one LeaveGroup |
| Second `Leave()` / `leave()` then `Close()` / `close()` | idempotent; still one LeaveGroup |
| `Poll` / `poll` after `Leave` / `leave` | `ErrGroupClosed` / closed |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** change Python / Rust / broker / protocol. Do **not** run
full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** Alias only.
- Close / close behavior is unchanged. Leave still goes through
  Close.
- Python `leave()` and Rust `leave()` already exist.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language `GroupConsumer` should keep
this hunk local to the aliases:

- **Keep Leave / leave as a one-line delegate to Close / close.**
  Do not change Close.
- Do not change join / heartbeat / auto-commit.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/group.go` (`Close`)
- Java `clients/java/src/main/java/io/volant/GroupConsumer.java`
  (`close`)
- `clients/go/group_test.go`
- `clients/java/src/test/java/io/volant/GroupConsumerTest.java`

The hunk is local to the aliases + existing close tests.

## Related

- [V184_SPEC.md](./V184_SPEC.md) — leftover getter pattern
- [V73_SPEC.md](./V73_SPEC.md) — Rust GroupConsumer range + leave
- [V44_SPEC.md](./V44_SPEC.md) — GroupConsumer heartbeat / leave
