# v0.200 — language Client auth token getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V42_SPEC.md](./V42_SPEC.md) /
language Dial / connect Auth: Python already exposes public
`Client.auth_token` (and tests assert it) and Rust
`ClientConfig.auth_token` is public. Go stores `authToken`
privately with no getter. Java stores `private final String
authToken` with no getter. Expose the stored shared-token Auth
value without changing Dial / connect / Auth / SCRAM.

This is residual **v0.200**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Python,
or Rust.

## Goals

1. **Go:** public `func (c *Client) AuthToken() string`. Return
   stored `c.authToken`. Nil receiver returns `""`. Place it near
   the other getters (`TLS` / `Addr` / `Timeout`).
2. **Java:** public `String authToken()`. Return stored
   `authToken` (may be null). Place it near `addr()` /
   `timeoutMs()`.
3. Python / Rust already public. Do **not** change them.
4. Do **not** add a SCRAM password getter. Optional username
   getter is **out of scope** (next leftover).
5. Do **not** change Dial / DialAuth / connect / authenticate.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Dial / DialAuth / connect / authenticate | Frozen; getter only |
| SCRAM password getter (`scramPass` / `scramPassword`) | Frozen; stays private |
| Optional username getter | Next leftover; out of scope |
| Python `.auth_token` / Rust `ClientConfig.auth_token` | Already public |
| Kafka SASL / JAAS | Native opcode 30 field only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// AuthToken returns the shared-token used for opcode 30, or "" if none.
func (c *Client) AuthToken() string {
    if c == nil {
        return ""
    }
    return c.authToken
}
```

```java
/** Shared-token used for opcode 30, or {@code null} if none. */
public String authToken() {
    return authToken;
}
```

```go
c, _ := volant.DialAuth("127.0.0.1:9092", "s3cret")
_ = c.AuthToken() // "s3cret"
c, _ = volant.Dial("127.0.0.1:9092")
_ = c.AuthToken() // ""
```

```java
try (Client c = Client.connect("127.0.0.1", 9092, "s3cret")) {
  c.authToken(); // "s3cret"
}
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.authToken(); // null
}
```

Existing Dial / DialAuth / connect signatures are unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send Auth
  (opcode 30) or SCRAM.
- After `Dial` / `connect` without a token: Go `""`, Java `null`.
- After `DialAuth(addr, "s3cret")` / `connect(..., "s3cret")`:
  `"s3cret"`.
- Empty token on DialAuth / connect still skips Auth (unchanged);
  getter returns `""` / empty-or-null as stored today. Do **not**
  change how empty tokens are stored.
- Token Auth still wins over SCRAM (unchanged).
- Go nil receiver returns `""` (same nil-guard style as `Addr()`).
- Not a Kafka SASL / JAAS API.

## Tests

Fake TCP stub (same `serveAuth` / `OneShotAuthServer` as v0.115 /
v0.183 / v0.195):

| Case | Expect |
|------|--------|
| Go `DialAuth(addr, "s3cret")` then `AuthToken()` | `"s3cret"` |
| Go `Dial(addr)` then `AuthToken()` | `""` |
| Go nil `*Client` | `AuthToken()` is `""` |
| Java `Client.connect(..., "s3cret")` | `authToken()=="s3cret"` |
| Java `Client.connect(host, port)` | `authToken()==null` |

```bash
cd clients/go && go test ./...
cd clients/java && mvn -q test
```

Do **not** change broker / protocol / Python / Rust. Do **not** run
Python discover. Do **not** append codec tests. Do **not** run cargo
workspace.

## Honesty leftovers

- **SCRAM password stays private.** No `scramPass` /
  `scramPassword` getter.
- Python `.auth_token` / Rust `ClientConfig.auth_token` already
  public.
- Getter does not send Auth.
- Token Auth still wins over SCRAM.
- **Not Kafka SASL.** Native opcode 30 field only.
- Optional username getter is the next leftover.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling v0.199 also edits Go `client.go` / Java `Client.java` /
both READMEs / possibly `reconnect_test.go`. Keep this hunk local
to the AuthToken / authToken getter + dedicated tests.

- **Keep AuthToken / authToken as a read of the stored field.**
  Do not change Dial / DialAuth / connect / authenticate.
- Do not add a SCRAM password getter.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go`
- `clients/go/reconnect_test.go` (keep TestMaxRedirectsGetter AND
  TestTimeout* AND the new AuthToken tests as separate funcs)
- `clients/go/README.md`
- `clients/java/src/main/java/io/volant/Client.java`
- `clients/java/README.md`

Keep both sides on conflict (orchestrator will merge). The hunk is
local to the getters + fake-TCP tests.

## Related

- [V42_SPEC.md](./V42_SPEC.md) — language shared-token Auth
- [V195_SPEC.md](./V195_SPEC.md) — language timeout getter (same leftover pattern)
- [V183_SPEC.md](./V183_SPEC.md) — Go Addr getter (same leftover pattern)
- [V191_SPEC.md](./V191_SPEC.md) — Go MaxRedirects getter
- [V115_SPEC.md](./V115_SPEC.md) — language public Reconnect
