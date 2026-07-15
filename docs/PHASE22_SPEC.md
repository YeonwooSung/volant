# Phase 22 — SCRAM-SHA-256 authentication (binding)

## Goals

1. **SCRAM-SHA-256** user/password auth (RFC 5802 crypto, Volant binary wire)
2. **Durable credentials** under `{data_dir}/__scram/users.json` (salt + StoredKey + ServerKey)
3. **Principal** = username after successful SCRAM (feeds Phase 20 ACLs)
4. **Coexist** with shared-token Auth and mTLS (any one may authenticate)
5. Client config + CLI + bootstrap user management
6. Tests + docs honesty

## Non-goals

- Full Kafka SASL/PLAIN or GSSAPI
- SCRAM-SHA-512
- Channel binding (tls-unique)
- Password change over the wire
- Account lockout / rate limiting

## When auth is required

```
auth_required = shared_token.is_some()
             || scram_user_count > 0
             || mtls_enabled
```

Unauthenticated connections may only send: Auth, ScramFirst, ScramFinal,
and (bootstrap only) CreateScramUser when the SCRAM store is empty.

## Wire protocol

| Dir | Opcode | Name |
|-----|--------|------|
| Req/Resp | 60/61 | ScramFirst |
| Req/Resp | 62/63 | ScramFinal |
| Req/Resp | 64/65 | CreateScramUser |
| Req/Resp | 66/67 | DeleteScramUser |
| Req/Resp | 68/69 | ListScramUsers |

### ScramFirst

Request:
```
username: string
client_nonce: string   # printable nonce from client
```

Response:
```
error_code: u16
combined_nonce: string
salt: bytes
iterations: u32
```

Unknown user → same shape as success with random salt (anti-enumeration) but
Final will fail. Or return AuthenticationFailed on Final only.

### ScramFinal

Request:
```
username: string
combined_nonce: string
client_proof: bytes    # 32 bytes for SHA-256
```

Response:
```
error_code: u16
server_signature: bytes
```

On success: connection `authenticated = true`, `principal = username`.

### AuthMessage construction (fixed Volant form)

```
client_first_bare = "n=" + username + ",r=" + client_nonce
server_first      = "r=" + combined_nonce + ",s=" + b64(salt) + ",i=" + iterations
client_final_wo_proof = "c=biws,r=" + combined_nonce   # biws = base64("n,,")
auth_message = client_first_bare + "," + server_first + "," + client_final_wo_proof
```

Standard SCRAM-SHA-256 formulas for Hi / ClientKey / StoredKey / proof / ServerSignature.

Default iterations: **4096**.

### CreateScramUser

```
username: string
password: string   # plaintext once; never stored
iterations: u32    # 0 = default 4096
```

- Bootstrap: if store has **zero** users, allowed without auth.
- Otherwise: requires Cluster Alter (or super-user / ACLs off + authenticated).
- Upserts credential.

### DeleteScramUser / ListScramUsers

Admin ops; same auth as Create when users exist. List returns usernames only.

## Durable store

```
{data_dir}/__scram/users.json
```

```json
{
  "users": {
    "alice": {
      "salt_b64": "...",
      "stored_key_b64": "...",
      "server_key_b64": "...",
      "iterations": 4096
    }
  }
}
```

## Server flags

| Flag | Meaning |
|------|---------|
| `--scram-user <user:pass>` | Upsert user at startup (repeatable) |

## Client

```rust
ClientConfig {
  scram_username: Some("alice".into()),
  scram_password: Some("s3cret".into()),
  ..
}
```

On connect: ScramFirst → ScramFinal before other RPCs (after optional TLS).

CLI:
```
volant --scram-user alice --scram-password s3cret topic list
volant user create --username bob --password x
volant user list
volant user delete --username bob
```

## Exit criteria

1. Wrong password → AuthenticationFailed; no principal
2. Correct SCRAM → produce/fetch works; principal = username for ACLs
3. Users survive broker restart
4. Bootstrap CreateScramUser when empty store
5. Shared-token Auth still works when configured
6. `cargo test --workspace` green

## Honest limitations

- No channel binding
- No SCRAM-SHA-512
- In-memory SCRAM challenge state is per TCP connection only
- CreateScramUser sends password in clear on the wire (use TLS)
- Not Kafka SASL handshake bytes
