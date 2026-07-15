# Phase 34 — SCRAM-SHA-512 (Kafka SASL + store)

## Goals

1. Advertise and accept **SCRAM-SHA-512** on the Kafka shim (`SaslHandshake` /
   `SaslAuthenticate`)
2. Store **both** SHA-256 and SHA-512 credentials when a user is created/updated
   from a plaintext password (same password works for either mechanism)
3. Keep **SCRAM-SHA-256** and **PLAIN** unchanged
4. Backward-compatible `users.json` load (legacy single-credential = SHA-256)
5. Tests + docs honesty

## Non-goals

- GSSAPI / OAUTHBEARER
- Channel binding
- Changing Volant-native SCRAM wire (stays SHA-256)
- Re-deriving SHA-512 keys for legacy users without re-upsert (impossible without password)

## Storage

`data_dir/__scram/users.json`:

**Legacy (Phase 22)** — still loadable:

```json
{ "users": { "alice": { "salt_b64": "...", "stored_key_b64": "...", "server_key_b64": "...", "iterations": 4096 } } }
```

**Phase 34 multi-mechanism** (written on new upserts):

```json
{
  "users": {
    "alice": {
      "sha256": { "salt_b64": "...", "stored_key_b64": "...", "server_key_b64": "...", "iterations": 4096 },
      "sha512": { "salt_b64": "...", "stored_key_b64": "...", "server_key_b64": "...", "iterations": 4096 }
    }
  }
}
```

- Independent salt per mechanism
- `upsert_user` / `upsert_scram_user` always writes both when password is known
- Legacy-only users: SHA-256 works; SHA-512 fails until password re-set

## Crypto

RFC 5802 / 7677 with hash H:

| Mechanism | H | Digest len | PBKDF2 / HMAC |
|-----------|---|------------|---------------|
| SCRAM-SHA-256 | SHA-256 | 32 | existing |
| SCRAM-SHA-512 | SHA-512 | 64 | new |

`ScramChallenge` carries `hash` so `finish` uses the matching primitive and
proof length.

## Kafka SASL

- `MECHANISMS`: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`
- Handshake selects mechanism; SCRAM flow identical to Phase 30 with algorithm-specific proofs

## Native Volant

`ScramFirst` / `ScramFinal` remain SHA-256 only (no wire change).

## Exit criteria

1. SaslHandshake lists SCRAM-SHA-512
2. Full SCRAM-SHA-512 round-trip on Kafka port
3. Wrong proof fails; unknown user fails
4. Same user works with both SCRAM-SHA-256 and SCRAM-SHA-512 after upsert
5. Legacy users.json (flat credential) still authenticates via SHA-256
6. PLAIN still works
7. `cargo test --workspace` green

## Honest limitations

- Legacy users need password re-upsert for SHA-512
- Native protocol is still SHA-256 only
- No channel binding / SCRAM-SHA-1
