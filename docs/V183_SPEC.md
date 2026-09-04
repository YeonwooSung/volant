# v0.183 — Go Addr getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V115_SPEC.md](./V115_SPEC.md):
Java already has `addr()`, Python has public `.addr`, Rust has
`current_addr()`. Go `Client` stores `addr` privately and updates it
on `Reconnect`, but has no named getter.

Expose the stored broker address (`host:port`) without changing Dial /
Reconnect / redirect. Do **not** change how `addr` is written.

This is residual **v0.183**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java (already have getters).

## Goals

1. **Go:** public `Addr() string`. Return stored `c.addr`. Nil
   receiver returns `""`. Do **not** dial, reconnect, or parse.
2. Java / Python / Rust already covered. Do not change them.
3. Do **not** change Dial / Reconnect / redirect behavior.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Dial / Reconnect / redirect | Frozen; getter only |
| Parse / split host:port | Frozen; return the stored string |
| Java `addr()` / Python `.addr` / Rust `current_addr()` | Already shipped |
| Kafka API keys / new native opcodes | Frozen; `SUPPORTED_APIS` stays 38 |
| Broker / protocol | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// Addr returns the current broker address (host:port).
// Updated by Reconnect.
func (c *Client) Addr() string {
    if c == nil {
        return ""
    }
    return c.addr
}
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", timeout)
_ = c.Addr()                 // "127.0.0.1:9092"
_ = c.Reconnect("127.0.0.1:9093")
_ = c.Addr()                 // "127.0.0.1:9093"
```

Existing Dial / Reconnect signatures are unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send RPCs.
- After `Dial` / `DialTimeout` / `DialTLS` / …, `Addr()` is the
  dial string.
- After successful `Reconnect(other)`, `Addr()` is `other`.
- Nil receiver returns `""` (same nil-guard style as `TLS()`).
- Redirect still uses the private reconnect path; this slice does
  not change that path.
- Not a Kafka bootstrap / advertised-listener API.

## Tests

Fake TCP stub (same `serveAuth` / scripted brokers as v0.115):

| Case | Expect |
|------|--------|
| `DialTimeout(addr)` then `c.Addr()` | equals `addr` |
| After `Reconnect(other)` | `Addr()` equals `other` |
| Nil `*Client` | `Addr()` is `""` |

```bash
cd clients/go && go test ./...
```

Do **not** change Java / Python / Rust. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** bootstrap / advertised listeners. Native client
  field only.
- Getter never dials or reconnects. It returns the stored string.
- Dial / Reconnect / redirect are unchanged.
- Java `addr()`, Python `.addr`, and Rust `current_addr()` already
  exist.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the getter:

- **Keep `Addr` as a read of `c.addr`.** Do not change Dial /
  Reconnect.
- Do not change Java, Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`TLS()`)
- Go `clients/go/reconnect_test.go`

The hunk is local to the getter + fake-TCP tests.

## Related

- [V115_SPEC.md](./V115_SPEC.md) — language public Reconnect
- [V160_SPEC.md](./V160_SPEC.md) — producer id getters (same pattern)
- [V27_SPEC.md](./V27_SPEC.md) — Go TLS (`TLS()`)
