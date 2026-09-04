# v0.233 — Kafka Describe/AlterUserScramCredentials keys 50/51 v0

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** Advertise Kafka **DescribeUserScramCredentials** (API key
**50**) and **AlterUserScramCredentials** (API key **51**), version
**0** only (always flexible). Wrap the existing `ScramStore` (native
opcodes **64–69**). Unfreezes `SUPPORTED_APIS` from 40 → **42**.

This is residual **v0.233**. It is **not** Kafka SASL Alter as the
native password API. Native create (opcodes 64/65) still takes
plaintext. Do **not** touch `group.rs`. Do **not** change Join / Fetch
/ txn.

## Goals

1. Advertise `(ApiKey::DescribeUserScramCredentials, 0, 0)` and
   `(ApiKey::AlterUserScramCredentials, 0, 0)` in `SUPPORTED_APIS`.
2. Dispatch keys 50 / 51 v0 (flexible request header + compact body).
3. Describe: empty users array = all users. Per stored mechanism emit
   Kafka `ScramMechanism` **1** = SCRAM-SHA-256, **2** = SCRAM-SHA-512.
4. Unknown user → that result **91** `RESOURCE_NOT_FOUND` (Kafka
   official; not 68 `NON_EMPTY_GROUP`).
5. Alter deletions remove that mechanism; last mechanism deletes the
   user. Unknown user/mechanism → **91** on that user.
6. Alter upsert takes `saltedPassword = Hi(password, salt, i)` (RFC
   5802). Derive ClientKey / StoredKey / ServerKey. No plaintext.
   iterations 0 or empty salt/password → **42** `INVALID_REQUEST`.
7. ACL: Cluster **DESCRIBE** (50) / Cluster **ALTER** (51). Disabled
   ACLs allow.
8. v1+ → **35** `UNSUPPORTED_VERSION`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Native create plaintext change | Opcodes 64/65 stay password-in-clear |
| Kafka SASL Alter as native password API | Different wire; native stays 64–69 |
| OAUTH / GSSAPI | Orthogonal SASL leftovers |
| Quota keys 48 / 49 | Out of scope |
| Versions 1+ | Kafka keys 50/51 are v0 only |
| Join / Fetch / txn / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
DescribeUserScramCredentials v0
  │
  ├─ Cluster DESCRIBE fail → top-level 31, empty results
  ├─ users = [] (or null) → every stored user
  └─ named user
          ├─ unknown → that result 91, empty credentialInfos
          └─ else → one info per stored mechanism (iterations)

AlterUserScramCredentials v0
  │
  ├─ Cluster ALTER fail → 31 on each unique user
  └─ per unique user (deletions then upsertions)
          ├─ delete unknown user/mechanism → 91
          ├─ iterations 0 / empty salt or saltedPassword → 42
          ├─ delete mechanism; last one drops the user
          └─ upsert: ScramStore::upsert_from_salted (one hash)
```

- Mechanism **1** = SHA-256, **2** = SHA-512.
- Native `upsert_user(password)` still writes **both** hashes.
- Kafka Alter upsert writes **only** the named mechanism.
- Response throttle is always 0.

## Tests

```bash
cargo test -p volant-broker --lib kafka -- --test-threads=1
cargo test -p volant-broker --lib scram -- --test-threads=1
cargo test -p volant-broker --test v233_scram_admin -- --test-threads=1
cargo test -p volant-broker --test v228_list_partition_reassignments -- --test-threads=1
```

| Case | Expect |
|------|--------|
| ApiVersions | keys **50** and **51** min=max=0; `SUPPORTED_APIS.len()==42` |
| Describe empty users after native create | SHA-256 + SHA-512 infos, iterations default 4096 |
| Alter upsert SHA-256 salted then Describe | that mechanism listed |
| Alter delete that mechanism | gone (user gone if last) |
| v1 | **35** |

| File | What |
|------|------|
| `crates/volant-broker/src/kafka/mod.rs` | ApiKey 50/51 + error 91 + `SUPPORTED_APIS` 42 |
| `crates/volant-broker/src/kafka/handler.rs` | dispatch + flexible header |
| `crates/volant-broker/src/kafka/admin_api.rs` | encode |
| `crates/volant-broker/src/scram.rs` | describe + salted upsert + per-mech delete |
| `crates/volant-broker/tests/v233_scram_admin.rs` | boot_kafka |
| `docs/KAFKA_COMPAT.md` | keys 50/51 v0 |
| `docs/V233_SPEC.md` | This spec |

## Honesty leftovers

- **Not** OAUTH / GSSAPI.
- Native create (opcodes 64/65) still sends the password in the clear
  (use TLS). Kafka key 51 is **not** that API.
- **Not** quota keys 48 / 49.
- Unknown user uses Kafka **91** `RESOURCE_NOT_FOUND` (the official
  code). 68 remains `NON_EMPTY_GROUP`.
- After any SCRAM user exists, the Kafka port still requires SASL
  before non-auth APIs (existing Phase 30 gate).
- `group.rs` `stays_40` assertion is intentionally untouched.

## Related

- [V174_SPEC.md](./V174_SPEC.md) — native SCRAM admin (opcodes 64–69)
- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — advertised keys
