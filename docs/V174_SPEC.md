# v0.174 — Rust create_scram_user_default

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover from [V55_SPEC.md](./V55_SPEC.md):
Java/Python have a 2-arg / default-0 overload for CreateScramUser.
Rust `create_scram_user(username, password, iterations)` requires
iterations. `0` already means the broker default (4096). There is no
named default-iterations helper. Go `CreateScramUserDefault` is
sibling **v0.173**.

Add `Client::create_scram_user_default`. Reuse `create_scram_user`
(do not reimplement the RPC). `create_scram_user` stays unchanged.
This is **not** Kafka AlterUserScramCredentials.

This is residual **v0.174** (Rust create_scram_user_default). It is
**not** Phase 174 work. It does **not** open Phase 155, add Kafka API
keys, add native opcodes, or change the broker, protocol, or
Python/Go/Java.

## Goals

1. Add public `Client::create_scram_user_default(username, password)`
   that calls `create_scram_user(username, password, 0)` (wire
   iterations **0** = broker default 4096).
2. Return `()` (same as `create_scram_user`).
3. Inherit retry / error **14** from `create_scram_user`
   (`admin_round_trip`: v0.104 transient retry + v0.88 error 14).
   No new retry policy.
4. Do **not** change `create_scram_user`.
5. Do **not** change broker / protocol / Python / Go / Java.

## Non-goals

| Deferred | Why |
|----------|-----|
| Change `create_scram_user` | Frozen; `0` already means broker default |
| Kafka AlterUserScramCredentials (API key 51) | Native opcodes 64/65 only |
| SCRAM-SHA-512 | Language/Rust SCRAM remains SHA-256 only |
| Hash the password on the client | Password still sent in the clear (use TLS) |
| Broker / protocol / new opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Python / Java | Already have 2-arg / default-0 (v0.55) |
| Go `CreateScramUserDefault` | Sibling **v0.173** |
| Phase 155 / homemade Raft | Frozen |

## API

```rust
/// Create or replace a SCRAM user with broker-default iterations.
///
/// Same as `create_scram_user(username, password, 0)`.
pub async fn create_scram_user_default(
    &self,
    username: &str,
    password: &str,
) -> Result<()> {
    self.create_scram_user(username, password, 0).await
}
```

```rust
client.create_scram_user_default("alice", "s3cret").await?; // iterations 0
client.create_scram_user("alice", "s3cret", 0).await?;      // unchanged
client.create_scram_user("alice", "s3cret", 4096).await?;   // explicit
```

## Semantics

- Wire iterations `0` still means the broker default (4096).
- `create_scram_user_default` is a named wrapper; it does not
  re-encode.
- `create_scram_user(username, password, iterations)` is unchanged
  (`0` still means broker default).
- Transient 6 / 7 / 15 / 16 and transport retry via
  `create_scram_user` / `admin_round_trip` (v0.104; default
  `max_retries=0`).
- Error 14 follows `max_redirects` (v0.88).
- Password is still sent in the clear (use TLS).
- SHA-256 only. Not Kafka AlterUserScramCredentials.

## Tests

Fake TCP stub that records decoded CreateScramUser username /
password / iterations.

```bash
cargo test -p volant-client -- --test-threads=1
```

| Case | Expect |
|------|--------|
| `create_scram_user_default("alice", "s3cret")` | CreateScramUser with iterations **0** |

Existing SCRAM-admin / admin-14 / admin-retry tests must still pass
(`create_scram_user` unchanged).

| File | What |
|------|------|
| `crates/volant-client/src/client.rs` | `create_scram_user_default` wraps `create_scram_user` |
| `crates/volant-client/src/lib.rs` | crate-doc note |
| `crates/volant-client/tests/v174_create_scram_user_default.rs` | fake TCP iterations-0 wire check |
| `docs/V174_SPEC.md` | This spec |

## Honesty leftovers

- **Not Kafka** AlterUserScramCredentials.
- Iterations `0` still means the broker default (4096).
- Password is still sent in the clear (use TLS).
- Language/Rust SCRAM remains SHA-256 only.
- `create_scram_user` is unchanged.
- Default `max_retries` / `max_redirects` unchanged.
- **No Kafka API keys / opcodes / Phase 155.**
- Broker / protocol / Python / Go / Java are unchanged.

## Merge notes

Sibling slices that also edit `client.rs` / crate-doc should keep
this hunk local to the CreateScramUser named helper:

- **Keep the named wrapper only.** Do not change `create_scram_user`.
- Do not change the CreateScramUser send loop (v0.104 retry +
  v0.88 14).
- Do not change language clients, broker, or protocol.

Expect conflicts on:

- `crates/volant-client/src/client.rs` — hunk is local to
  `create_scram_user_default` after `create_scram_user`
- `crates/volant-client/src/lib.rs` (crate-doc)

## Related

- [V55_SPEC.md](./V55_SPEC.md) — language Create/Delete/ListScramUsers
- [V88_SPEC.md](./V88_SPEC.md) — Rust SCRAM-admin error 14
- [V104_SPEC.md](./V104_SPEC.md) — Rust admin_round_trip transient retry
- [V79_SPEC.md](./V79_SPEC.md) — Rust admin error 14
- [PHASE22_SPEC.md](./PHASE22_SPEC.md) — native CreateScramUser 64/65
