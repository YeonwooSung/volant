# v0.173 — Go CreateScramUserDefault

**Status:** Shipped (bounded MVP; not Phase 155)
**Crate:** 0.2.0 (unchanged)
**Theme:** Close the leftover from [V55_SPEC.md](./V55_SPEC.md): Java
already has `createScramUser(user, pass)` (iterations 0). Python
`create_scram_user(user, pass, iterations=0)` already defaults to 0.
Go only has `CreateScramUser(username, password, iterations)` — `0`
already means the broker default (4096), but there is no named
default-iterations helper matching Java.

Add `Client.CreateScramUserDefault`. Reuse `CreateScramUser` (do not
reimplement the RPC). `CreateScramUser(username, password, iterations)`
stays unchanged. This is **not** Kafka AlterUserScramCredentials.

This is residual **v0.173** (Go CreateScramUserDefault). It is **not**
Phase 155. It does **not** open Phase 155, add Kafka API keys, add
native opcodes, or change the broker, protocol, Rust, Python, or Java.

## Goals

1. Add public `func (c *Client) CreateScramUserDefault(username,
   password string) error` that calls
   `CreateScramUser(username, password, 0)`.
2. Inherit retry / error **14** from `CreateScramUser` (v0.72
   error 14 + v0.103 transient retry via `adminRoundTrip`). No new
   retry policy.
3. Do **not** change `CreateScramUser(username, password, iterations)`.
4. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `CreateScramUser(username, password, iterations)` | Frozen; 0 already means broker default |
| Kafka AlterUserScramCredentials (API key 51) | Native opcode 64/65 only |
| SCRAM-SHA-512 / channel binding | Language SCRAM remains SHA-256 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java | Already have user+pass overloads (v0.55) |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// CreateScramUserDefault creates a SCRAM user with broker-default
// iterations (0 → 4096). Same as CreateScramUser(username, password, 0).
func (c *Client) CreateScramUserDefault(username, password string) error {
    return c.CreateScramUser(username, password, 0)
}
```

```go
_ = c.CreateScramUserDefault("alice", "s3cret")           // broker default
_ = c.CreateScramUser("alice", "s3cret", 0)               // unchanged: same wire
_ = c.CreateScramUser("alice", "s3cret", 4096)
```

## Semantics

- Wire iterations `0` = broker default (4096) (same as today).
- `CreateScramUserDefault` is a named wrapper; it does not re-encode.
- `CreateScramUser(username, password, iterations)` is unchanged
  (`0` still means broker default).
- Password is sent in the clear (use TLS).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `CreateScramUser` / `adminRoundTrip` (v0.103; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.72).
- Not Kafka AlterUserScramCredentials (no mechanism list, no
  deletions array, no SHA-512).
- Language SCRAM remains SHA-256 only.

## Tests

Fake TCP stub that records decoded CreateScramUser iterations (same
helper as existing `scram_admin_test.go`).

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `CreateScramUserDefault("alice", "s3cret")` | wire iterations == 0; username/password match |
| Existing `CreateScramUser` explicit 4096 / error cases | still pass |

Existing CreateScramUser retry / 14 tests must still pass
(`CreateScramUser` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `CreateScramUserDefault` wraps `CreateScramUser(username, password, 0)` |
| `clients/go/scram_admin_test.go` | zero-iterations wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V173_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** AlterUserScramCredentials (API key 51). Native
  opcode **64/65** only. No mechanism list, deletions array, or
  SHA-512.
- Iterations `0` still means the broker default (**4096**).
- Password is still sent in the clear (use TLS).
- `CreateScramUser(username, password, iterations)` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the CreateScramUserDefault wrapper:

- **Keep the wrapper only.** Do not change `CreateScramUser`.
- Do not change the CreateScramUser send loop (v0.72 14 + v0.103
  transient retry).
- Do not change Python, Java, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to `CreateScramUserDefault`
  after `CreateScramUser`
- `clients/go/scram_admin_test.go`
- `clients/go/README.md`

## Related

- [V55_SPEC.md](./V55_SPEC.md) — language Create/Delete/ListScramUsers
- [V72_SPEC.md](./V72_SPEC.md) — language admin error 14
- [V103_SPEC.md](./V103_SPEC.md) — language admin_round_trip transient retry
- [V167_SPEC.md](./V167_SPEC.md) — Go ReassignAllPartitions (same wrapper pattern)
- [PHASE22_SPEC.md](./PHASE22_SPEC.md) — native 64–69
