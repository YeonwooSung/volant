# v0.75 — GroupConsumer poll fetch knobs on Python / Go / Java

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V64_SPEC.md](./V64_SPEC.md): Client
`fetch` / `FetchOpts` already expose `max_messages` / `max_bytes` /
`max_wait_ms`, but GroupConsumer `poll` hardcodes
`_POLL_MAX_MESSAGES = 100` (Python) and does not pass `max_bytes`.
Operators cannot tune a coordinated poll without dropping to raw fetch.

This is a residual slice under “Multi-language clients”. It does
**not** open Phase 155, add Kafka API keys, add new native opcodes, or
change the broker / `volant-protocol` / Rust client.

## Goals

1. Keep today’s **default** poll fetch size so existing tests stay
   valid:
   - `max_messages` default **100** (current `_POLL_MAX_MESSAGES`,
     **not** Client fetch’s 128)
   - `max_bytes` default **4 MiB** (same as Client fetch)
   - `max_wait_ms` on poll stays the existing poll argument
     (Python `poll(max_wait_ms=500)`, etc.)
2. Additive knobs, stored on the GroupConsumer (join-time and/or
   setters). Existing `poll(...)` signatures stay valid.
3. `_fetch_assigned` / equivalent must pass the knobs into
   `client.fetch` / `FetchOpts` / 6-arg `fetch`.
4. Values `<= 0` clamp to the defaults (100 / 4MiB) so tests can be
   sloppy.
5. Do **not** change assignor, auto_offset_reset, or `_apply_reset`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka `max.poll.records` | Native Fetch opcode 2 only; one fetch per assigned partition |
| Change Client fetch default 128 | Unrelated; poll stays 100 |
| Rust `GroupConsumer` poll knobs | Still hardcoded |
| New native opcodes / Kafka API keys | Reuse Fetch (2) |
| Broker / protocol / Rust client changes | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

Keep existing `poll(...)` signatures. Additive knobs only.

```python
GroupConsumer.join(..., fetch_max_messages=100, fetch_max_bytes=4*1024*1024)
g.fetch_max_messages = 10
g.fetch_max_bytes = 4096
g.poll(max_wait_ms=500)  # unchanged signature
```

```go
JoinGroupConsumer(..., WithFetchMaxMessages(10), WithFetchMaxBytes(4096))
// default 100 / 4MiB
g.Poll(500 * time.Millisecond) // still only a max-wait timeout
```

```java
g.setFetchMaxMessages(10);
g.setFetchMaxBytes(4096);
g.poll(500); // still only a max-wait timeout
// named setters so they do not collide with join(..., int)
```

`<= 0` clamps to 100 / 4MiB at join, setter, and fetch time.

## Tests

No broker; fake Client / fake TCP / mock backend:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q   # from clients/python
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Default poll | fetch request max_messages=100, max_bytes=4MiB |
| join/setter max_messages=10 | fetch request max_messages=10 |
| join/setter max_bytes=4096 | fetch request max_bytes=4096 |
| Existing poll tests | still pass (default 100) |
| `<= 0` | clamps to 100 / 4MiB |

| File | What |
|------|------|
| `clients/python/tests/test_group.py` | Fake Client records fetch knobs |
| `clients/go/group_test.go` | Fake TCP sees FetchRequest knobs |
| `clients/java/.../GroupConsumerTest.java` | Mock backend records fetch knobs |

## Files

| Path | Role |
|------|------|
| `clients/python/src/volant/group.py` | join knobs + attributes; `_fetch_assigned` passes them |
| `clients/go/group.go` | `WithFetchMaxMessages` / `WithFetchMaxBytes`; `Poll` uses `FetchOpts` |
| `clients/java/src/main/java/io/volant/GroupConsumer.java` | setters; Backend 6-arg fetch |
| `clients/{python,go,java}/README.md` | Honesty: poll fetch size is tunable |
| `docs/V75_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka `max.poll.records`.** One native Fetch (opcode 2) per
  assigned partition; knobs are `max_messages` / `max_bytes` on that
  request. Default 100 is the historical poll cap, not Client fetch’s
  128.
- **Rust `GroupConsumer` still hardcodes poll fetch size.**
- No Kafka API keys / opcodes / Phase 155.

## Merge notes

Siblings **v0.69** (`_local_range_assignment`) and **v0.70**
(`_apply_reset`) also edit `group.py` / `group.go` /
`GroupConsumer.java`. Keep this hunk local to poll / fetch-assigned +
join options. Do not change who `range_assign_multi` receives or how
reset picks a position.

Do not drop auto_commit + heartbeat + assignor + instance id + reset
knob + these fetch knobs to resolve a conflict.

## Related

- [V64_SPEC.md](./V64_SPEC.md) — Go/Java Fetch knobs (Client)
- [V31_SPEC.md](./V31_SPEC.md) — Python GroupConsumer
- [V32_SPEC.md](./V32_SPEC.md) — Go GroupConsumer
- [V33_SPEC.md](./V33_SPEC.md) — Java GroupConsumer
- [V70_SPEC.md](./V70_SPEC.md) — earliest via ListOffsets
