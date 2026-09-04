# v0.201 — Java GroupConsumer heartbeatIntervalMs public

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V37_SPEC.md](./V37_SPEC.md) /
[V44_SPEC.md](./V44_SPEC.md): Go already has public
`HeartbeatInterval(sessionTimeoutMs)` and Python already has public
`heartbeat_interval_ms(session_timeout_ms)`. Java
`GroupConsumer.heartbeatIntervalMs(int)` already implements the same
clamp (`sessionTimeoutMs / 3`, 100–3000 ms) but was package-private
(`static long`). Tests in the same package already call it.

Make the helper **public**. Do **not** change the clamp, the formula,
or who calls it.

This is residual **v0.201**. It is **not** Phase 155. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, or change
the broker, protocol, Rust, Python, or Go.

## Goals

1. **Java:** `public static long heartbeatIntervalMs(int sessionTimeoutMs)`.
   Same body as today (`sessionTimeoutMs / 3`, clamp 100–3000 inclusive).
2. Keep the existing javadoc. Do **not** rename. Do **not** add an
   instance getter.
3. Do **not** change `HB_INTERVAL_MIN_MS` / `HB_INTERVAL_MAX_MS`.
4. Background heartbeat / join still call the same method.
5. Go / Python already public. Do **not** change them.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change formula / clamp | Frozen; visibility only |
| Instance getter | Frozen; static helper only |
| Change join / poll / heartbeat | Frozen; same callers |
| Kafka `heartbeat.interval.ms` | Native Heartbeat period only |
| Go `HeartbeatInterval` / Python `heartbeat_interval_ms` | Already public |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```java
/** Background heartbeat period: {@code sessionTimeoutMs / 3}, clamped to 100–3000 ms. */
public static long heartbeatIntervalMs(int sessionTimeoutMs) {
    long interval = sessionTimeoutMs / 3L;
    if (interval < HB_INTERVAL_MIN_MS) {
        return HB_INTERVAL_MIN_MS;
    }
    if (interval > HB_INTERVAL_MAX_MS) {
        return HB_INTERVAL_MAX_MS;
    }
    return interval;
}
```

```java
GroupConsumer.heartbeatIntervalMs(10_000); // 3000
GroupConsumer.heartbeatIntervalMs(900);    // 300
GroupConsumer.heartbeatIntervalMs(0);      // 100
```

Existing join / `poll` / background heartbeat signatures are
unchanged. Only the visibility changes: `static long` →
`public static long`.

## Semantics

- Formula stays `sessionTimeoutMs / 3`, clamp 100–3000 inclusive.
- `heartbeatIntervalMs(0)` / negative / 150 → 100; 300 → 100;
  900 → 300; 10_000 → 3000.
- Background heartbeat / join still call the same method.
- Not Kafka `heartbeat.interval.ms`.

## Tests

Existing `heartbeatIntervalClamped` in `GroupConsumerTest` already
covers the math (same-package callers still compile after the
visibility change):

| Case | Expect |
|------|--------|
| `heartbeatIntervalMs(0)` | `100` |
| `heartbeatIntervalMs(150)` | `100` |
| `heartbeatIntervalMs(300)` | `100` |
| `heartbeatIntervalMs(900)` | `300` |
| `heartbeatIntervalMs(10_000)` | `3000` |

```bash
cd clients/java && mvn -q test
```

Do **not** change Python / Rust / Go / broker / protocol. Do **not**
run full Python discover. Do **not** run cargo workspace.

## Honesty leftovers

- Formula / clamp unchanged.
- Go / Python already public.
- Not Kafka `heartbeat.interval.ms`.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling v0.202 also edits Java README (and Java Client, not
GroupConsumer). Keep this hunk local to `heartbeatIntervalMs`
visibility + README sentence.

- **Keep visibility as `public static long`.** Do not change the
  body or callers.
- Do not change `HB_INTERVAL_MIN_MS` / `HB_INTERVAL_MAX_MS`.
- Do not change join / poll / heartbeat.
- Do not change Go, Python, Rust, broker, or protocol.

Expect conflicts on:

- `clients/java/README.md` — keep both sides
- Java `clients/java/src/main/java/io/volant/GroupConsumer.java`
  (visibility only; sibling should not touch this method)
- `clients/java/src/test/java/io/volant/GroupConsumerTest.java`

The hunk is local to `heartbeatIntervalMs` visibility + README
sentence. Existing `heartbeatIntervalClamped` stays.

## Related

- [V37_SPEC.md](./V37_SPEC.md) — language GroupConsumer background
  heartbeat (`sessionTimeoutMs / 3`, clamp 100–3000)
- [V44_SPEC.md](./V44_SPEC.md) — Rust `heartbeat_interval`
- [V187_SPEC.md](./V187_SPEC.md) — Java `heartbeatCount()` leftover
- [V189_SPEC.md](./V189_SPEC.md) — Java `sessionTimeoutMs()` leftover
