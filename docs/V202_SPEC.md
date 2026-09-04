# v0.202 — language Client SCRAM username getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V200_SPEC.md](./V200_SPEC.md) /
language DialScram / connectScram: Python already exposes public
`Client.scram_username` and Rust `ClientConfig.scram_username` is
public. Go stores `scramUser` privately with no getter. Java stores
`private final String scramUsername` with no getter. Expose the
stored SCRAM username without changing DialScram / connectScram /
handshake. Do **not** expose the SCRAM password (`scramPass` /
`scramPassword`).

This is residual **v0.202**. It is **not** Phase 155. It does **not**
open Phase 155, add Kafka API keys, add native opcodes, change
homemade Raft, or change the broker / protocol / Python / Rust.

## Goals

1. **Go:** public `func (c *Client) ScramUser() string`. Return
   stored `c.scramUser`. Nil receiver returns `""`. Place it
   immediately after `AuthToken()` in `clients/go/client.go`.
2. **Java:** public `String scramUsername()`. Return stored
   `scramUsername` (may be null). Place it immediately after
   `authToken()` in
   `clients/java/src/main/java/io/volant/Client.java`.
3. Python / Rust already public. Do **not** change them.
4. Do **not** add a password getter. Do **not** change Dial /
   DialScram / connect / connectScram / authenticate.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change Dial / DialScram / connect / connectScram / handshake | Frozen; getter only |
| SCRAM password getter (`scramPass` / `scramPassword`) | Frozen; stays private |
| Python `.scram_username` / Rust `ClientConfig.scram_username` | Already public |
| Kafka SASL / JAAS | Native SCRAM-SHA-256 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// ScramUser returns the SCRAM-SHA-256 username, or "" if none.
func (c *Client) ScramUser() string {
    if c == nil {
        return ""
    }
    return c.scramUser
}
```

```java
/** SCRAM-SHA-256 username, or {@code null} if none. */
public String scramUsername() {
    return scramUsername;
}
```

```go
c, _ := volant.DialScram("127.0.0.1:9092", "alice", "s3cret")
_ = c.ScramUser() // "alice"
c, _ = volant.Dial("127.0.0.1:9092")
_ = c.ScramUser() // ""
```

```java
try (Client c = Client.connectScram("127.0.0.1", 9092, "alice", "s3cret")) {
  c.scramUsername(); // "alice"
}
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.scramUsername(); // null
}
```

Existing Dial / DialScram / connect / connectScram signatures are
unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send SCRAM
  (60–63) or Auth (30).
- After `Dial` / `connect` without SCRAM: Go `""`, Java `null`.
- After `DialScram(addr, "alice", ...)` / `connectScram(..., "alice", ...)`:
  `"alice"`.
- After token-only `DialAuth` / `connect(..., token)`: username still
  empty/null (token Auth wins; username was not set).
- Password field stays private.
- Go nil receiver returns `""` (same nil-guard style as `AuthToken()`).
- Not Kafka SASL / JAAS.

## Tests

Fake TCP stub (same `serveAuth` / `serveScram` / `ScramServer` as
v0.115 / v0.200 / existing SCRAM tests):

| Case | Expect |
|------|--------|
| Go `DialScram(addr, "alice", ...)` then `ScramUser()` | `"alice"` |
| Go `Dial(addr)` then `ScramUser()` | `""` |
| Go nil `*Client` | `ScramUser()` is `""` |
| Java `Client.connectScram(..., "alice", ...)` | `scramUsername()=="alice"` |
| Java `Client.connect(host, port)` | `scramUsername()==null` |

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
- Python `.scram_username` / Rust `ClientConfig.scram_username`
  already public.
- Getter does not send SCRAM.
- Language SCRAM is SHA-256 only.
- **Not Kafka SASL.** Native SCRAM-SHA-256 handshake only.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling v0.201 edits Java `GroupConsumer.java` + Java README. This
slice edits Go `client.go` / Java `Client.java` / both READMEs /
`reconnect_test.go`. Keep hunks local to the username getter +
dedicated tests.

- **Keep ScramUser / scramUsername as a read of the stored field.**
  Do not change Dial / DialScram / connect / connectScram /
  authenticate.
- Do not add a SCRAM password getter.
- Do not change Python, Rust, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` (after AuthToken)
- `clients/go/reconnect_test.go` (keep AuthToken tests AND new
  ScramUser tests as separate funcs)
- `clients/go/README.md`
- `clients/java/src/main/java/io/volant/Client.java`
- `clients/java/README.md`

Keep both sides on conflict (orchestrator will merge). The hunk is
local to the getters + fake-TCP tests.

## Related

- [V200_SPEC.md](./V200_SPEC.md) — language auth token getter
- [V46_SPEC.md](./V46_SPEC.md) — language SCRAM-SHA-256
- [V42_SPEC.md](./V42_SPEC.md) — language shared-token Auth
- [V195_SPEC.md](./V195_SPEC.md) — language timeout getter (same leftover pattern)
- [V183_SPEC.md](./V183_SPEC.md) — Go Addr getter (same leftover pattern)
- [V115_SPEC.md](./V115_SPEC.md) — language public Reconnect
