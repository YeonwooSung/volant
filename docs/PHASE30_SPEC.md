# Phase 30 — Kafka SASL (PLAIN + SCRAM-SHA-256)

## Goals

1. **SaslHandshake** (API 17) and **SaslAuthenticate** (API 36) on `--kafka-listen`
2. Mechanisms: **PLAIN** and **SCRAM-SHA-256** against Volant Phase 22 SCRAM store
3. Connection **principal** = username after success; ACL checks use it
4. When SCRAM users exist, require SASL before non-auth APIs
5. Tests + docs honesty

## Non-goals

- SCRAM-SHA-512 / GSSAPI / OAUTHBEARER
- Channel binding (`tls-unique`)
- Legacy raw SASL frames (pre–SaslAuthenticate API)
- Shared-token Auth on the Kafka port (Volant-native only)
- Kafka SASL over TLS packaging (TLS is separate; PLAINTEXT SASL works)

## When auth is required (Kafka port)

```
kafka_auth_required = broker.scram().has_users()
```

Shared-token alone does **not** gate the Kafka port (clients cannot send
Volant Auth frames there). Optional SASL still upgrades the principal when
users exist but the gate is off only if the store is empty.

Allow without auth: `ApiVersions`, `SaslHandshake`, `SaslAuthenticate`.

## Wire

### SaslHandshake (17) v0–1

Request: `mechanism: STRING`  
Response: `error_code: INT16`, `enabled_mechanisms: [STRING]`

Supported: `PLAIN`, `SCRAM-SHA-256`. Unknown → error **33**
(`UNSUPPORTED_SASL_MECHANISM`) + list of enabled mechanisms.

### SaslAuthenticate (36) v0–1

Request: `auth_bytes: BYTES`  
Response: `error_code`, `error_message` (nullable), `auth_bytes`; v1 adds
`session_lifetime_ms = 0`.

#### PLAIN (`auth_bytes`)

RFC 4616: `\0username\0password` (optional authzid prefix also accepted as
`authzid\0username\0password`).

Verify via `ScramStore::verify_password`. Success → empty `auth_bytes`,
principal set. Failure → error **58** (`SASL_AUTHENTICATION_FAILED`).

#### SCRAM-SHA-256

Standard SASL SCRAM exchange (two `SaslAuthenticate` round-trips):

1. Client: `n,,n=user,r=clientNonce` → server: `r=…,s=…,i=…`
2. Client: `c=biws,r=…,p=…` → server: `v=…` (or `e=…` on failure)

Reuses Phase 22 crypto (`begin` / `finish` / `build_auth_message`).

## Principal & ACLs

- Before auth: principal for ACL checks remains `kafka-anonymous`
- After auth: principal = SCRAM username
- Existing ACL entries for real users apply on the Kafka path

## Exit criteria

1. ApiVersions advertises 17 and 36
2. PLAIN success/fail against SCRAM store
3. SCRAM-SHA-256 full round-trip; wrong proof fails
4. With users registered, Produce without SASL is rejected
5. Authenticated Produce + ACL principal works
6. `cargo test --workspace` green

## Honest limitations

- No SCRAM-SHA-512 / GSSAPI
- No channel binding
- Shared-token does not apply to Kafka
- Pre-1.0 clients that speak raw SASL without API 36 are unsupported
