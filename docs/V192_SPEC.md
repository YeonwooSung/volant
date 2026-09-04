# v0.192 — Go MaxRetries getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
[V66_SPEC.md](./V66_SPEC.md): Java already has `maxRetries()`.
Python has public `.max_retries`. Rust has
`ClientConfig.max_retries`. Go `Client` stores `maxRetries`
privately and `SetMaxRetries` writes it, but has no named getter.

Expose the stored retry budget without changing SetMaxRetries /
retry logic. Do **not** change how `maxRetries` is written.

This is residual **v0.192**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java (already have getters).

## Goals

1. **Go:** public `MaxRetries() int`. Return stored `c.maxRetries`.
   Nil receiver returns `0`. Do **not** retry, sleep, or send RPCs.
2. Java / Python / Rust already covered. Do not change them.
3. Do **not** change SetMaxRetries or retry logic.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change SetMaxRetries / retry logic | Frozen; getter only |
| Change default / clamp | Frozen; setter still treats negative as 0 |
| Java `maxRetries()` / Python `.max_retries` / Rust `ClientConfig.max_retries` | Already shipped |
| Kafka `retries` | Frozen; native Produce/Fetch only |
| Kafka API keys / new native opcodes | Frozen; `SUPPORTED_APIS` stays 38 |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// MaxRetries returns extra Produce/Fetch/Heartbeat/SCRAM attempts
// after the first (default 0).
func (c *Client) MaxRetries() int {
    if c == nil {
        return 0
    }
    return c.maxRetries
}
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", timeout)
_ = c.MaxRetries()           // 0
c.SetMaxRetries(2)
_ = c.MaxRetries()           // 2
c.SetMaxRetries(-1)
_ = c.MaxRetries()           // 0
```

Existing `SetMaxRetries` signature is unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send RPCs.
- After `Dial` / `DialTimeout` / `DialTLS` / …, `MaxRetries()` is
  **0**.
- After `SetMaxRetries(2)`, `MaxRetries()` is **2**.
- After `SetMaxRetries(-1)`, the setter still clamps to 0, so
  `MaxRetries()` is **0**.
- Nil receiver returns `0` (same nil-guard style as `TLS()` /
  `Addr()`).
- Produce / Fetch / Heartbeat / SCRAM retry still uses the private
  field; this slice does not change that path.
- Not a Kafka `retries` config.

## Tests

Fake TCP stub (same scripted brokers as v0.61):

| Case | Expect |
|------|--------|
| `DialTimeout(addr)` then `c.MaxRetries()` | `0` |
| After `SetMaxRetries(2)` | getter is `2` |
| After `SetMaxRetries(-1)` | getter is `0` |

```bash
cd clients/go && go test ./...
```

Do **not** change Java / Python / Rust. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** `retries`. Native client field only.
- Getter never retries or sleeps. It returns the stored int.
- SetMaxRetries / retry logic are unchanged.
- Java `maxRetries()`, Python `.max_retries`, and Rust
  `ClientConfig.max_retries` already exist.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the getter:

- **Keep `MaxRetries` as a read of `c.maxRetries`.** Do not change
  SetMaxRetries.
- Do not change Java, Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`SetMaxRetries`)
- Go `clients/go/client_test.go`

The hunk is local to the getter + scripted-broker tests.

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry / `SetMaxRetries`
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry (same knob)
- [V183_SPEC.md](./V183_SPEC.md) — leftover getter pattern (`Addr`)
