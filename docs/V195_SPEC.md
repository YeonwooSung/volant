# v0.195 — language Client timeout getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V183_SPEC.md](./V183_SPEC.md) /
language Dial / connect: Java stores `timeoutMs` with no getter, Go
stores `timeout` with no getter, and Python already keeps the value as
private `_timeout`. Expose the stored dial / RPC timeout without
changing Dial / connect / timeout application.

This is residual **v0.195**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or Rust.

## Goals

1. **Go:** public `Timeout() time.Duration`. Return stored
   `c.timeout`. Nil receiver returns `0`. Do **not** dial.
2. **Java:** public `int timeoutMs()`. Return stored `timeoutMs`.
   Named `timeoutMs()` so it does not collide with `connect`
   overloads.
3. **Python:** public `@property timeout`. Return stored `_timeout`.
   Do **not** rename `_timeout` usages. No setter.
4. Do **not** change Dial / connect / timeout application.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Dial / connect / timeout application | Frozen; getter only |
| Rename Python `_timeout` | Frozen; storage stays private |
| Java `timeout()` / connect overload collision | Frozen; use `timeoutMs()` |
| Kafka request.timeout.ms / socket.timeout.ms | Native dial / RPC field only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// Timeout returns the dial / RPC timeout (Dial default 10s).
func (c *Client) Timeout() time.Duration {
    if c == nil {
        return 0
    }
    return c.timeout
}
```

```java
/** Dial / RPC timeout in milliseconds (connect default 10000). */
public int timeoutMs() {
    return timeoutMs;
}
```

```python
@property
def timeout(self) -> float:
    """Dial / RPC timeout in seconds (constructor default 10.0)."""
    return self._timeout
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", 5*time.Second)
_ = c.Timeout() // 5s
c, _ = volant.Dial("127.0.0.1:9092")
_ = c.Timeout() // 10s
```

```java
try (Client c = Client.connect("127.0.0.1", 9092, 2500)) {
  c.timeoutMs(); // 2500
}
```

```python
c = Client("127.0.0.1:9092", timeout=2.5)
_ = c.timeout  # 2.5
```

Existing Dial / connect / constructor signatures are unchanged.

## Semantics

- Getters read the stored field only. They do **not** send RPCs.
- After `DialTimeout` / `connect(..., timeoutMs)` /
  `Client(..., timeout=)`, the getter is the constructor value.
- After `Dial` / `connect(host, port)` / `Client(addr)`, the getter
  is the language default (Go 10s, Java 10000 ms, Python 10.0 s).
- Go nil receiver returns `0` (same nil-guard style as `Addr()`).
- Python storage stays `_timeout`. No setter.
- Dial / connect / SoTimeout / `settimeout` application is unchanged.
- Not a Kafka `request.timeout.ms` / `socket.timeout.ms` API.

## Tests

Fake TCP stub (same `serveAuth` / `OneShotAuthServer` / accept-once
listener as v0.115 / v0.183):

| Case | Expect |
|------|--------|
| Go `DialTimeout(addr, 5s)` then `Timeout()` | `5s` |
| Go `Dial(addr)` then `Timeout()` | `10s` (Dial default) |
| Go nil `*Client` | `Timeout()` is `0` |
| Java `Client.connect(host, port, 2500)` | `timeoutMs()==2500` |
| Python `Client(..., timeout=2.5)` | `.timeout == 2.5` |
| Python `Client(addr)` | `.timeout == 10.0` |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
cd clients/python && PYTHONPATH=src python3 -m unittest tests.test_timeout -q
```

Do **not** change broker / protocol / Rust. Do **not** run full
Python discover. Do **not** run `tests.test_client`. Do **not** run
cargo workspace.

## Honesty leftovers

- **Not Kafka** `request.timeout.ms` / `socket.timeout.ms`. Native
  client field only.
- Getter never dials or reconnects. It returns the stored value.
- Dial / connect / timeout application are unchanged.
- Python `_timeout` storage is unchanged. No setter.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit language `Client` should keep this hunk
local to the getters:

- **Keep Timeout / timeoutMs / timeout as a read of the stored
  field.** Do not change Dial / connect.
- Do not rename Python `_timeout`.
- Do not change broker, protocol, or Rust.

Expect conflicts on:

- Go `clients/go/client.go` (`Addr()`)
- Java `clients/java/src/main/java/io/volant/Client.java` (`addr()`)
- Python `clients/python/src/volant/client.py` (`reconnect`)
- Go `clients/go/reconnect_test.go`
- Java `clients/java/src/test/java/io/volant/AuthTest.java`

The hunk is local to the getters + fake-TCP tests.

## Related

- [V183_SPEC.md](./V183_SPEC.md) — Go Addr getter (same leftover pattern)
- [V115_SPEC.md](./V115_SPEC.md) — language public Reconnect
- [V160_SPEC.md](./V160_SPEC.md) — producer id getters (same pattern)
