# v0.151 — Rust public InitProducerId

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V150_SPEC.md](./V150_SPEC.md) /
[V102_SPEC.md](./V102_SPEC.md): language clients already have public
InitProducerId (`init_producer_id` / `InitProducerID` /
`initProducerId`). Rust `ensure_producer_id` is still **private**.
Produce / BeginTxn still init implicitly.

Expose a public no-arg wrapper. If already initialized, it is a
no-op (same as the helper). Returns the stored pid/epoch. Do **not**
reimplement the retry loop; wrap `ensure_producer_id`. Do **not**
change implicit produce / BeginTxn Init.

This is residual **v0.151**, not Phase 155. It does **not** open
Phase 155, add Kafka API keys, add native opcodes, or change the
broker, protocol, or Python/Go/Java.

## Goals

1. Public `Client::init_producer_id(&self) -> Result<(u64, u16)>`.
   Call `ensure_producer_id()` then return
   `(state.producer_id, state.epoch)`.
2. Keep `ensure_producer_id` private. Do not change its retry /
   error-21-not-retried contract (v0.102).
3. Second call is a no-op (already initialized), same as the helper.
4. Produce / BeginTxn still init implicitly. No extra Init from this
   slice.
5. Do **not** reimplement the retry loop. Wrap the existing helper
   so v0.102 transient retries still apply.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change implicit produce / BeginTxn Init | Frozen |
| Reimplement Init retry | Wrap `ensure_producer_id` (v0.102) |
| Kafka InitProducerId (API key 22) | Native opcode 32 only |
| Broker / protocol / Python / Go / Java | Frozen (language public API is v0.150) |
| New opcodes / Kafka API keys | Frozen |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Ensure InitProducerId has run (native opcode 32).
/// Returns the stored producer id and epoch. A second call is a no-op
/// (already initialized). Produce / BeginTxn still init implicitly.
pub async fn init_producer_id(&self) -> Result<(u64, u16)> {
    self.ensure_producer_id().await?;
    let state = self.idempotent.lock().await;
    Ok((state.producer_id, state.epoch))
}
```

```rust
let (pid, epoch) = c.init_producer_id().await?;  // opcode 32
let (pid2, epoch2) = c.init_producer_id().await?; // no-op; same values
```

Existing produce / BeginTxn signatures are unchanged. They still
call the private helper on first use.

## Semantics

- First public call sends native opcode **32** and stores pid/epoch.
- Second call does **not** send another Init (already initialized).
- Produce with `enable_idempotence` still inits once (no extra Init).
- BeginTxn still inits implicitly when the pid is not ready.
- Transient 6 / 7 / 15 / 16 and transport retry via the existing
  helper (v0.102; default `max_retries=0`).
- Error 21 on Init itself is still raised immediately (not retried).
- Not Kafka InitProducerId versions.

## Tests

```bash
cargo test -p volant-client -- --test-threads=1
```

Fake TCP stub (v0.102 style) that records Init opcode / count:

| Case | Expect |
|------|--------|
| First `init_producer_id` | opcode 32; returns stored pid/epoch |
| Second call | no second Init; same pid/epoch |
| Existing produce-with-idempotence still inits once | no extra Init from this slice |

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | public `init_producer_id` wrap |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v151_init_producer_id.rs` | first / second / produce-once |
| `docs/V151_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** InitProducerId (API key 22). Native opcode **32**
  only.
- `ensure_producer_id` stays private.
- Implicit produce / BeginTxn Init is unchanged.
- Default `max_retries=0` (v0.102) is unchanged.
- Error 21 on Init itself is still not retried.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` should keep this hunk
local to the public wrapper:

- **Keep the public method as a wrap of `ensure_producer_id`.** Do
  not copy the retry loop.
- Do not change produce / BeginTxn Init.
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `ensure_producer_id` / `init_producer_id`
- `crates/volant-client/src/lib.rs` (crate-doc)

The hunk is local to the public wrapper + one fake-TCP test file.

## Related

- [V150_SPEC.md](./V150_SPEC.md) — language public InitProducerId
- [V102_SPEC.md](./V102_SPEC.md) — Rust InitProducerId retry
- [V101_SPEC.md](./V101_SPEC.md) — language InitProducerId retry
- [V47_SPEC.md](./V47_SPEC.md) — idempotent produce / InitProducerId
- [PHASE10_SPEC.md](./PHASE10_SPEC.md) — Rust idempotent produce
- [PHASE18_SPEC.md](./PHASE18_SPEC.md) — native InitProducerId / txn
