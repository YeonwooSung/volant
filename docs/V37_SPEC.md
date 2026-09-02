# v0.37 — GroupConsumer background heartbeat

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “silent consumer expires.” Python / Go / Java
`GroupConsumer` only heartbeated inside `poll`. An app that processed a
batch longer than `session_timeout_ms` was expired (error 10) even though
it was still alive. This slice starts a background heartbeat after a
successful join.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change the broker. Rust
`volant-client` is unchanged (v0.44).

## Goals

1. After a successful join, start a background loop that sends Heartbeat
   every `session_timeout_ms / 3`, clamped to **[100 ms, 3000 ms]**.
2. Default **on**. Escape keeps the v0.31/v0.32/v0.33 poll-only path:
   - Python: `GroupConsumer.join(..., heartbeat=False)`
   - Go: `JoinGroupConsumer(..., WithBackgroundHeartbeat(false))`
   - Java: `GroupConsumer.join(..., false)` / `join(c, group, topics, timeout, heartbeat)`
3. On heartbeat error **9 / 10 / 11**, the loop re-joins (same as `poll`).
   Other broker errors are ignored until the next tick.
4. `close` / `Close` stops the loop (join with a short timeout), then
   LeaveGroup. Idempotent. Does not close the `Client`.
5. `poll` still heartbeats once at the start of the call.

## Non-goals

| Deferred | Why |
|----------|-----|
| Rust `volant-client` heartbeat task | Sibling residual (v0.44) |
| Fully concurrent `poll` / `commit` | One lock serializes the loop vs poll; not a multi-thread API |
| Sharing one `Client` with other RPCs while open | Sync clients; one TCP connection |
| Auto-commit | Separate leftover |
| Changing session timeout / broker expiry | Coordinator unchanged |

## Interval

```
interval = clamp(session_timeout_ms / 3, 100, 3000)  # milliseconds
```

| session_timeout_ms | interval |
|-------------------:|---------:|
| 0 / 150 / 300 | 100 ms |
| 900 | 300 ms |
| 10_000 (default) | 3000 ms |

## Concurrency honesty

The background thread/goroutine/executor and `poll` / `commit` share a
mutex around join state and GroupConsumer RPCs. Callers must still treat
the consumer as **single-threaded**: do not call `poll` from two threads,
and do not use the same `Client` for other RPCs while the consumer is
open.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Clamp; bg heartbeat without poll; rejoin on 9; `heartbeat=False` is silent |
| `clients/go/group_test.go` | Same (fake TCP) |
| `clients/java/.../GroupConsumerTest.java` | Same (mock backend) |

Poll-only unit tests pass `heartbeat=False` so they stay deterministic.

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

## Honesty leftovers

- Rust `GroupConsumer` still poll-only (v0.44).
- Not a fully concurrent consumer. One TCP connection. Sync only.
- No auto-commit. No client-side assignor. No shared-token Auth / leader
  redirect on these clients (sibling residuals).
- Broker coordinator and session timeout are unchanged.

See [clients/python/README.md](../clients/python/README.md),
[clients/go/README.md](../clients/go/README.md),
[clients/java/README.md](../clients/java/README.md),
[V31_SPEC.md](./V31_SPEC.md),
[V32_SPEC.md](./V32_SPEC.md), and
[V33_SPEC.md](./V33_SPEC.md).
