# v0.191 — Go MaxRedirects getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V43_SPEC.md](./V43_SPEC.md):
Java already has `maxRedirects()`, Python has public
`.max_redirects`, Rust has `ClientConfig.max_redirects`. Go
`Client` stores `maxRedirects` privately and updates it on
`SetMaxRedirects`, but has no named getter.

Expose the stored NotLeader/NotController redirect budget without
changing Dial / SetMaxRedirects / redirect. Do **not** change how
`maxRedirects` is written.

This is residual **v0.191**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java (already have getters / public fields).

## Goals

1. **Go:** public `MaxRedirects() int`. Return stored
   `c.maxRedirects`. Nil receiver returns `0`. Do **not** redirect
   or mutate the budget.
2. Java / Python / Rust already covered. Do not change them.
3. Do **not** change Dial / SetMaxRedirects / redirect behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change SetMaxRedirects / redirect | Frozen; getter only |
| Change Dial default (1) | Frozen; connect default stays 1 |
| Java `maxRedirects()` / Python `.max_redirects` / Rust `max_redirects` | Already shipped |
| Kafka API keys / new native opcodes | Frozen; `SUPPORTED_APIS` stays 38 |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// MaxRedirects returns the NotLeader/NotController redirect budget
// (default 1; 0 = no redirect).
func (c *Client) MaxRedirects() int {
    if c == nil {
        return 0
    }
    return c.maxRedirects
}
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", timeout)
_ = c.MaxRedirects()          // 1 (Dial default)
c.SetMaxRedirects(0)
_ = c.MaxRedirects()          // 0
c.SetMaxRedirects(3)
_ = c.MaxRedirects()          // 3
c.SetMaxRedirects(-1)
_ = c.MaxRedirects()          // 0 (setter already clamps)
```

Existing `SetMaxRedirects` signature and clamp are unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send RPCs.
- After `Dial` / `DialTimeout` / `DialTLS` / …, `MaxRedirects()` is
  **1** (connect default).
- After `SetMaxRedirects(n)` with `n >= 0`, `MaxRedirects()` is `n`.
- After `SetMaxRedirects(-1)`, `MaxRedirects()` is **0** (setter
  already clamps negatives).
- Nil receiver returns `0` (same nil-guard style as `Addr()`).
- Redirect still uses the private `maxRedirects` budget; this slice
  does not change that path.
- Not a Kafka `retries` / advertised-listener API.

## Tests

Fake TCP stub (same `serveAuth` as v0.183):

| Case | Expect |
|------|--------|
| `DialTimeout` then `c.MaxRedirects()` | `1` |
| After `SetMaxRedirects(0)` | getter is `0` |
| After `SetMaxRedirects(3)` | getter is `3` |
| After `SetMaxRedirects(-1)` | getter is `0` (setter clamps) |

```bash
cd clients/go && go test ./...
```

Do **not** change Java / Python / Rust. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** `retries` / advertised listeners. Native client
  field only.
- Getter never redirects. It returns the stored budget.
- SetMaxRedirects / redirect are unchanged.
- Java `maxRedirects()`, Python `.max_redirects`, and Rust
  `ClientConfig.max_redirects` already exist.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the getter:

- **Keep `MaxRedirects` as a read of `c.maxRedirects`.** Do not
  change SetMaxRedirects / redirect.
- Do not change Java, Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`SetMaxRedirects`)
- Go `clients/go/reconnect_test.go`

The hunk is local to the getter + fake-TCP tests.

## Related

- [V43_SPEC.md](./V43_SPEC.md) — language leader redirect /
  `SetMaxRedirects`
- [V183_SPEC.md](./V183_SPEC.md) — Go Addr getter (same pattern)
- [V160_SPEC.md](./V160_SPEC.md) — producer id getters (same pattern)
