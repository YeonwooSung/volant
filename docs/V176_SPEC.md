# v0.176 — Go CommitTransactionEmpty

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V57_SPEC.md](./V57_SPEC.md) /
[V100_SPEC.md](./V100_SPEC.md): Java already has no-arg
`commitTransaction()` → empty list. Python already has
`commit_transaction(offsets=None)`. Rust already has
`commit_transaction_empty` (v0.175). Go `CommitTransaction(offsets)`
already accepts nil (documented). There is no named empty-offset
helper.

Add `Client.CommitTransactionEmpty`. Reuse `CommitTransaction` (do
not reimplement the RPC). `CommitTransaction` / `AbortTransaction` /
`BeginTransaction` stay unchanged. This is **not** Kafka txn API keys.

This is residual **v0.176** (Go CommitTransactionEmpty). It is **not**
Phase 176 work. It does **not** open Phase 155, add Kafka API keys,
add native opcodes, or change the broker, protocol, Rust, Python, or
Java.

## Goals

1. Add public `func (c *Client) CommitTransactionEmpty()
   ([]codec.TxnProduceResult, error)` that calls
   `CommitTransaction(nil)` (nil offsets = no deferred offset
   commits).
2. Return `[]codec.TxnProduceResult`.
3. Inherit retry from `CommitTransaction` / `endTransaction`
   (v0.99 / v0.100 transient retry). No new retry policy.
4. Do **not** change `CommitTransaction` / `AbortTransaction` /
   `BeginTransaction`.
5. Do **not** change broker / protocol / Rust / Python / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `CommitTransaction` / `AbortTransaction` / `BeginTransaction` | Frozen; nil already means no deferred offsets |
| Kafka transactions (API keys 22/24/25/26/28) | Native opcodes 50–53 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java | Already have empty-list / None overloads |
| Rust `commit_transaction_empty` | Sibling **v0.175** |
| Phase 155 / homemade Raft | Frozen |

## API

```go
// CommitTransactionEmpty commits the open transaction with no deferred
// offsets. Same as CommitTransaction(nil).
func (c *Client) CommitTransactionEmpty() ([]codec.TxnProduceResult, error) {
    return c.CommitTransaction(nil)
}
```

```go
_, _ = c.CommitTransactionEmpty()                       // no deferred offsets
_, _ = c.CommitTransaction(nil)                         // unchanged: same wire
_, _ = c.CommitTransaction([]codec.TxnOffsetCommit{...}) // deferred TxnOffsetCommit list
```

## Semantics

- Nil / empty wire offsets = no deferred offset commits (same as today).
- `CommitTransactionEmpty` is a named wrapper; it does not re-encode.
- Encodes EndTxn with **committed=1** and **empty offsets**.
- Producer id must already be initialized (`InitProducerID` or
  implicit Init on produce / BeginTxn); same as `CommitTransaction`.
- `CommitTransaction(offsets)` is unchanged (nil still means no
  deferred commits).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `CommitTransaction` / `endTransaction` (v0.99; default
  `max_retries=0`).
- Not Kafka transactions (API keys 22/24/25/26/28).

## Tests

Fake TCP stub that records decoded EndTxn `committed` + offsets
(same helper as existing `txn_test.go`). After a successful
BeginTxn, `CommitTransactionEmpty()` must encode EndTxn with
**committed=1** and **empty / nil offsets**.

```bash
(cd clients/go && go test ./...)
```

| Case | Expect |
|------|--------|
| `CommitTransactionEmpty()` after BeginTxn | EndTxn committed=1, empty/nil offsets |
| Existing `CommitTransaction(nil)` retry / timeout cases | still pass |

Existing `TestEndTxnRetriesTimeoutThenOk` must still pass
(`CommitTransaction` unchanged).

| File | What |
|------|------|
| `clients/go/client.go` | `CommitTransactionEmpty` wraps `CommitTransaction(nil)` |
| `clients/go/txn_test.go` | empty-offset EndTxn wire check |
| `clients/go/README.md` | usage line + one prose sentence |
| `docs/V176_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** transactions (API keys 22/24/25/26/28).
- Nil / empty offsets still mean **no** deferred offset commits.
- `CommitTransaction` / `AbortTransaction` / `BeginTransaction`
  are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Rust / Python / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.go` should keep this hunk local
to the EndTxn empty helper:

- **Keep the named wrapper only.** Do not change
  `CommitTransaction` / `AbortTransaction` / `BeginTransaction`.
- Do not change the EndTxn send loop (v0.99 retry).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `clients/go/client.go` — hunk is local to
  `CommitTransactionEmpty` after `CommitTransaction`
- `clients/go/txn_test.go`
- `clients/go/README.md`

## Related

- [V57_SPEC.md](./V57_SPEC.md) — language BeginTxn / EndTxn
- [V99_SPEC.md](./V99_SPEC.md) — language BeginTxn / EndTxn retry
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [V151_SPEC.md](./V151_SPEC.md) — Rust public InitProducerId
- [V175_SPEC.md](./V175_SPEC.md) — Rust commit_transaction_empty
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — native BeginTxn / EndTxn 50–53
