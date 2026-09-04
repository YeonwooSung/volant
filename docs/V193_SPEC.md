# v0.193 — Go RetryBackoff getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V61_SPEC.md](./V61_SPEC.md) /
[V66_SPEC.md](./V66_SPEC.md): Java already has `retryBackoffMs()`.
Go `Client` stores `retryBackoff` privately (Dial default 50ms) and
exposes `SetRetryBackoff`, but has no named getter.

Expose the stored produce/fetch retry sleep without changing
`SetRetryBackoff` or retry sleep logic. Do **not** change how
`retryBackoff` is written.

This is residual **v0.193**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java (already has a getter).

## Goals

1. **Go:** public `RetryBackoff() time.Duration`. Return stored
   `c.retryBackoff`. Nil receiver returns `0`. Do **not** sleep,
   retry, or clamp (setter already clamps negatives to 0).
2. Java already covered (`retryBackoffMs()`). Do not change Java /
   Python / Rust.
3. Do **not** change `SetRetryBackoff` or retry sleep behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `SetRetryBackoff` / retry sleep | Frozen; getter only |
| Change Dial default (50ms) | Frozen; getter returns the stored value |
| Java `retryBackoffMs()` | Already shipped |
| Kafka retry / backoff API | Native produce/fetch retry only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// RetryBackoff returns the sleep between produce/fetch retries
// (default 50ms).
func (c *Client) RetryBackoff() time.Duration {
    if c == nil {
        return 0
    }
    return c.retryBackoff
}
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", timeout)
_ = c.RetryBackoff()                 // 50ms
c.SetRetryBackoff(0)
_ = c.RetryBackoff()                 // 0
c.SetRetryBackoff(-time.Second)
_ = c.RetryBackoff()                 // 0 (setter clamps)
```

Existing `SetRetryBackoff` signature is unchanged.

## Semantics

- Getter reads the stored field only. It does **not** sleep or retry.
- After `Dial` / `DialTimeout` / `DialTLS` / …, `RetryBackoff()` is
  **50ms**.
- After `SetRetryBackoff(d)` with `d >= 0`, getter is `d`.
- After `SetRetryBackoff` with a negative duration, getter is `0`
  (setter already clamps).
- Nil receiver returns `0` (same nil-guard style as `Addr()`).
- Produce / Fetch retry still sleeps `c.retryBackoff` when `> 0`.
  This slice does not change that path.
- Not a Kafka producer `retry.backoff.ms` API.

## Tests

Fake TCP stub (same `startScripted` as other client getter tests):

| Case | Expect |
|------|--------|
| `DialTimeout` then `c.RetryBackoff()` | `50 * time.Millisecond` |
| After `SetRetryBackoff(0)` | getter is `0` |
| After `SetRetryBackoff(-time.Second)` | getter is `0` (setter clamps) |

```bash
cd clients/go && go test ./...
```

Do **not** change Java / Python / Rust. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** `retry.backoff.ms`. Native client field only.
- Getter never sleeps or retries. It returns the stored duration.
- `SetRetryBackoff` and retry sleep logic are unchanged.
- Java `retryBackoffMs()` already exists.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the getter:

- **Keep `RetryBackoff` as a read of `c.retryBackoff`.** Do not
  change `SetRetryBackoff` or sleep.
- Do not change Java, Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`SetRetryBackoff`)
- Go `clients/go/client_test.go`

The hunk is local to the getter + DialTimeout tests.

## Related

- [V61_SPEC.md](./V61_SPEC.md) — produce retry / `SetRetryBackoff`
- [V66_SPEC.md](./V66_SPEC.md) — fetch retry reuses the same backoff
- [V183_SPEC.md](./V183_SPEC.md) — Go Addr getter (same pattern)
