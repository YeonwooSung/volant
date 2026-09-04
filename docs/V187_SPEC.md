# v0.187 — Java GroupConsumer heartbeatCount

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V44_SPEC.md](./V44_SPEC.md) /
[V37_SPEC.md](./V37_SPEC.md): Rust already has
`GroupConsumer::heartbeat_count()` (poll + background Heartbeat
RPCs; JoinGroup is not counted). Java `GroupConsumer` already
heartbeats on `poll` and in the v0.37 background loop, but has no
public counter.

Count Heartbeat attempts. Do **not** change poll / heartbeat
semantics except the counter.

This is residual **v0.187**. It is **not** Phase 155. It does
**not** open Phase 155, add Kafka API keys, add native opcodes, or
change the broker, protocol, Rust, Python, or Go.

## Goals

1. **Java:** `long heartbeatCount` field (already under `lock` for
   `poll` / `heartbeatOnce`).
2. Increment **once per Heartbeat attempt** (before the RPC),
   matching Rust:
   - `poll`: increment immediately before `backend.heartbeat(...)`
   - `heartbeatOnce`: increment immediately before
     `backend.heartbeat(...)` when not closed
3. Do **not** increment on join.
4. Public `heartbeatCount()` next to `fetchMaxBytes()`.
5. Do **not** change poll / heartbeat / join / leave behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change poll / heartbeat semantics | Frozen; counter only |
| Count JoinGroup | Frozen; Rust does not count join |
| SyncGroup / JoinGroup retry | Frozen |
| Kafka heartbeat.interval.ms | Native Heartbeat only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Rust / Python / Go | Rust already shipped; others out of scope |
| Phase 155 / homemade Raft | Frozen |

## API

```java
/** Heartbeat RPCs issued by {@link #poll} + background (not JoinGroup). */
public long heartbeatCount() {
    lock.lock();
    try {
        return heartbeatCount;
    } finally {
        lock.unlock();
    }
}
```

```java
GroupConsumer g = GroupConsumer.join(backend, "g", List.of("t"), 10_000, false);
g.heartbeatCount(); // 0 (join is not counted)
g.poll(0);
g.heartbeatCount(); // 1
```

Existing join / `poll` / background heartbeat signatures are
unchanged.

## Semantics

- Increment once per Heartbeat **attempt**, immediately before the
  RPC (failed Heartbeat still counts).
- `poll` always increments once (poll-only and background-on).
- Background `heartbeatOnce` increments when the consumer is not
  closed.
- JoinGroup is **not** counted.
- Getter reads the stored counter under `lock`. It does **not**
  send Heartbeat or JoinGroup.
- Not Kafka `heartbeat.interval.ms`.

## Tests

Existing heartbeat fakes (`GroupConsumerTest` / `FakeBackend`):

| Case | Expect |
|------|--------|
| Join with `heartbeat=false` | `heartbeatCount() == 0` before poll |
| After one `poll` | `heartbeatCount() == 1` |
| Background `heartbeat=true` + short session timeout | `heartbeatCount() >= 1` without poll |

```bash
cd clients/java && mvn -q test
```

Do **not** change Python / Rust / Go / broker / protocol. Do **not**
run full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- **Still no SyncGroup.** Heartbeat + JoinGroup only.
- Counter only. Poll / heartbeat / join behavior is unchanged.
- Rust `heartbeat_count()` already exists (v0.44).
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Java `GroupConsumer` should keep
this hunk local to the counter:

- **Keep increments immediately before `backend.heartbeat`.**
  Do not increment on join.
- Do not change poll / heartbeat / rejoin behavior.
- Do not change Rust, Python, Go, broker, or protocol.

Expect conflicts on:

- Java `clients/java/src/main/java/io/volant/GroupConsumer.java`
  (`poll` / `heartbeatOnce` / getters)
- `clients/java/src/test/java/io/volant/GroupConsumerTest.java`
- `clients/java/README.md`

The hunk is local to the counter + existing heartbeat tests.

## Related

- [V37_SPEC.md](./V37_SPEC.md) — language GroupConsumer background
  heartbeat
- [V44_SPEC.md](./V44_SPEC.md) — Rust GroupConsumer background
  heartbeat + `heartbeat_count()`
- [V184_SPEC.md](./V184_SPEC.md) — leftover getter pattern
