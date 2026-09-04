# v0.194 — Go TransactionalID getter

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V57_SPEC.md](./V57_SPEC.md):
Java already has `transactionalId()`. Go `Client` stores
`transactionalID` privately and writes it via `SetTransactionalID`,
but has no named getter.

Expose the stored native transactional_id without changing
`SetTransactionalID` / `BeginTransaction`. Do **not** change how
the field is written.

This is residual **v0.194**. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, Rust,
Python, or Java (already has a getter).

## Goals

1. **Go:** public `TransactionalID() string`. Return stored
   `c.transactionalID`. Nil receiver returns `""`. Empty means none.
2. Java already covered. Do not change Java / Python / Rust.
3. Do **not** change `SetTransactionalID` / `BeginTransaction`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `SetTransactionalID` / `BeginTransaction` | Frozen; getter only |
| Kafka InitProducerId / txn API keys 22/24/25/26/28 | Native opcode 32 / 50–53 only |
| Java `transactionalId()` | Already shipped |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// TransactionalID returns the native transactional_id (empty = none).
func (c *Client) TransactionalID() string {
    if c == nil {
        return ""
    }
    return c.transactionalID
}
```

```go
c, _ := volant.DialTimeout("127.0.0.1:9092", timeout)
_ = c.TransactionalID()          // ""
c.SetTransactionalID("txn-1")
_ = c.TransactionalID()          // "txn-1"
c.SetTransactionalID("")
_ = c.TransactionalID()          // ""
```

Existing `SetTransactionalID` / `BeginTransaction` signatures are
unchanged.

## Semantics

- Getter reads the stored field only. It does **not** send RPCs
  (no InitProducerId / BeginTxn).
- After `Dial` / `DialTimeout` / …, `TransactionalID()` is `""`
  (unset / none).
- After `SetTransactionalID("txn-1")`, the getter is `"txn-1"`.
- After `SetTransactionalID("")`, the getter is empty again.
- Nil receiver returns `""` (same nil-guard style as `Addr()` /
  `TLS()`).
- Not a Kafka transactional.id / InitProducerId API.

## Tests

Fake TCP stub (same `serveTxn` as v0.57):

| Case | Expect |
|------|--------|
| `DialTimeout` then `c.TransactionalID()` | `""` |
| After `SetTransactionalID("txn-1")` | `"txn-1"` |
| After `SetTransactionalID("")` | `""` |

```bash
cd clients/go && go test ./...
```

Do **not** change Java / Python / Rust. Do **not** append codec tests.

## Honesty leftovers

- **Not Kafka** transactional.id / InitProducerId (API key 22) /
  txn API keys 22/24/25/26/28. Native field + opcode **32** /
  **50–53** only.
- Getter never inits or begins a txn. It returns the stored string.
- `SetTransactionalID` / `BeginTransaction` are unchanged.
- Java `transactionalId()` already exists.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / codec encode/decode are unchanged.

## Merge notes

Sibling slices that also edit Go `Client` should keep this hunk
local to the getter:

- **Keep `TransactionalID` as a read of `c.transactionalID`.**
  Do not change `SetTransactionalID` / `BeginTransaction`.
- Do not change Java, Python, Rust, broker, or protocol.

Expect conflicts on:

- Go `clients/go/client.go` (`SetTransactionalID`)
- Go `clients/go/txn_test.go`

The hunk is local to the getter + fake-TCP tests.

## Related

- [V57_SPEC.md](./V57_SPEC.md) — native BeginTxn / SetTransactionalID
- [V183_SPEC.md](./V183_SPEC.md) — leftover getter pattern
- [V185_SPEC.md](./V185_SPEC.md) — Idempotence getter
- [V160_SPEC.md](./V160_SPEC.md) — producer id getters
