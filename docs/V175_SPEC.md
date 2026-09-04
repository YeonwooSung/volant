# v0.175 — Rust commit_transaction_empty

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V57_SPEC.md](./V57_SPEC.md) /
[V100_SPEC.md](./V100_SPEC.md): Java already has no-arg
`commitTransaction()` → empty list. Python already has
`commit_transaction(offsets=None)`. Go `CommitTransaction(offsets)`
already accepts nil (documented). Rust only has
`Client::commit_transaction(offsets: Vec<TxnOffsetCommit>)` where
empty already means no deferred offset commits. There is no named
empty-offset helper.

Add `Client::commit_transaction_empty`. Reuse `commit_transaction`
(do not reimplement the RPC). `commit_transaction` /
`abort_transaction` / `begin_transaction` stay unchanged. This is
**not** Kafka txn API keys.

This is residual **v0.175** (Rust commit_transaction_empty). It is
**not** Phase 175 work. It does **not** open Phase 155, add Kafka
API keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::commit_transaction_empty()` that calls
   `commit_transaction(Vec::new())` (empty wire offsets = no
   deferred offset commits).
2. Return `Vec<TxnProduceResult>`.
3. Inherit retry from `commit_transaction` / `end_transaction`
   (v0.100 transient retry). No new retry policy.
4. Do **not** change `commit_transaction` / `abort_transaction` /
   `begin_transaction`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `commit_transaction` / `abort_transaction` / `begin_transaction` | Frozen; empty vec already means no deferred offsets |
| Kafka transactions (API keys 22/24/25/26/28) | Native opcodes 50–53 only |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Go / Java | Already have empty-list / nil overloads |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Commit the open transaction with no deferred offsets.
///
/// Same as `commit_transaction(vec![])`.
pub async fn commit_transaction_empty(&self) -> Result<Vec<TxnProduceResult>> {
    self.commit_transaction(Vec::new()).await
}
```

```rust
let _ = client.commit_transaction_empty().await?;        // no deferred offsets
let _ = client.commit_transaction(vec![]).await?;        // unchanged: same wire
let _ = client.commit_transaction(offsets).await?;       // deferred TxnOffsetCommit list
```

## Semantics

- Empty wire offsets = no deferred offset commits (same as today).
- `commit_transaction_empty` is a named wrapper; it does not re-encode.
- Encodes EndTxn with **committed=1** and **empty offsets**.
- Producer id must already be initialized (`init_producer_id` or
  implicit Init on produce / BeginTxn); same as `commit_transaction`.
- `commit_transaction(offsets)` is unchanged (empty still means no
  deferred commits).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `commit_transaction` / `end_transaction` (v0.100; default
  `max_retries=0`).
- Not Kafka transactions (API keys 22/24/25/26/28).

## Tests

Fake TCP stub that records decoded EndTxn `committed` + offsets.
After a successful InitProducerId, `commit_transaction_empty()`
must encode EndTxn with **committed=1** and **empty offsets**.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `commit_transaction_empty()` after InitProducerId | EndTxn committed=1, empty offsets |

Existing `v100_begin_end_txn_retry.rs` must still pass
(`commit_transaction` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `commit_transaction_empty` wraps `commit_transaction` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v175_commit_transaction_empty.rs` | fake TCP empty-offset EndTxn wire check |
| `docs/V175_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** transactions (API keys 22/24/25/26/28).
- Empty offsets still mean **no** deferred offset commits.
- `commit_transaction` / `abort_transaction` / `begin_transaction`
  are unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the EndTxn empty helper:

- **Keep the named wrapper only.** Do not change
  `commit_transaction` / `abort_transaction` / `begin_transaction`.
- Do not change the EndTxn send loop (v0.100 retry).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `commit_transaction_empty` after `commit_transaction`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V57_SPEC.md](./V57_SPEC.md) — language BeginTxn / EndTxn
- [V99_SPEC.md](./V99_SPEC.md) — language BeginTxn / EndTxn retry
- [V100_SPEC.md](./V100_SPEC.md) — Rust BeginTxn / EndTxn retry
- [V151_SPEC.md](./V151_SPEC.md) — Rust public InitProducerId
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — native BeginTxn / EndTxn 50–53
