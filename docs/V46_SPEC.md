# v0.46 — SCRAM-SHA-256 on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python / Go / Java clients have no
**SCRAM**” by sending native opcodes **60–63** after connect when the
caller sets a username and password, matching Rust
`ClientConfig.scram_username` / `scram_password`.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka SASL / SCRAM-SHA-512, add native opcodes, or
change the broker SCRAM handler.

## Goals

1. **Codec** encode/decode for ScramFirst (60/61) and ScramFinal
   (62/63) in Python, Go, and Java. Reuse each language’s existing
   `put_string` / `put_bytes`.
2. **Crypto** matches `crates/volant-client/src/scram.rs`: PBKDF2-HMAC-SHA256,
   HMAC-SHA256 Client/Server Key, `c=biws` only (no channel binding).
3. **Send SCRAM once** immediately after the socket is connected (and
   TLS handshake done, if any), before any other RPC. Failure fails the
   constructor / Dial, not the first produce.
4. **Auth vs SCRAM:** if `auth_token` is set (v0.42) → send Auth only.
   Else if username **and** password are both set → SCRAM. Else skip.
   Username without password (or vice versa) is a constructor error.
5. **Reconnect** (v0.43 leader redirect) re-runs the same path (token
   or SCRAM).
6. **Unit tests** without a broker: codec round-trip 60–63, one pinned
   crypto vector, fake server (first+final, bad password, signature
   mismatch, no creds, token wins).

## Non-goals

| Deferred | Why |
|----------|-----|
| SCRAM-SHA-512 | Rust language clients are SHA-256 only |
| Kafka SASL / `--kafka-listen` | Native clients only |
| Channel binding | Broker AuthMessage is `c=biws` only |
| Create/Delete/ListScramUsers on these clients | Admin path stays on Rust / CLI |
| New native opcodes | 60–63 already exist |
| Broker SCRAM store / handler | Already shipped (Phase 22) |
| Kafka API keys | Frozen at 38 |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`. Strings are
little-endian **u16** length + UTF-8 (`put_string`). Bytes are
little-endian **u32** length (`put_bytes`; `0xFFFFFFFF` = null, not
used on these fields).

| Direction | Opcode | Body |
|-----------|--------|------|
| Request `ScramFirst` | **60** | `username: string`, `client_nonce: string` |
| Response `ScramFirst` | **61** | `error_code: u16`, `combined_nonce: string`, `salt: bytes`, `iterations: u32` |
| Request `ScramFinal` | **62** | `username: string`, `combined_nonce: string`, `client_proof: bytes` |
| Response `ScramFinal` | **63** | `error_code: u16`, `server_signature: bytes` |

Non-zero `error_code` is `AuthenticationFailed` (**17**) in the usual
cases. Server signature mismatch is a client-side protocol error
(does not send another RPC).

## Crypto

Client nonce: 16 random bytes, standard Base64, `,` replaced with `A`.

```
Hi          = PBKDF2-HMAC-SHA256(password, salt, iterations) → 32 bytes
ClientKey   = HMAC-SHA256(Hi, "Client Key")
StoredKey   = SHA256(ClientKey)
ServerKey   = HMAC-SHA256(Hi, "Server Key")
auth_message = n={user},r={client_nonce},r={combined_nonce},s={b64(salt)},i={iterations},c=biws,r={combined_nonce}
ClientSignature = HMAC-SHA256(StoredKey, auth_message)
ClientProof     = ClientKey XOR ClientSignature
ServerSignature = HMAC-SHA256(ServerKey, auth_message)   # verified by client
```

Pinned test vector (same in Python / Go / Java):

| Field | Value |
|-------|-------|
| user / pass | `alice` / `s3cret` |
| client_nonce | `rOprNGfwEbeRWgbNEkqO` |
| combined_nonce | `rOprNGfwEbeRWgbNEkqOserver` |
| salt | `saltSALTsaltSALT` (16 bytes) |
| iterations | 4096 |
| client_proof | `82aa6ee69043dd3c43785fba02fe220ea4a74a44b12d31b3a3a3ad17c1e0b5f3` |
| server_signature | `d3068040897e7eaaa647e45356dab05074e5d48f6a283ec72a5181421768783d` |

## API

Existing constructors stay. Token, max_redirects, and SCRAM all coexist
on the same `Client` (merge note for v0.42 / v0.43).

```python
Client("127.0.0.1:9092", scram_username="alice", scram_password="s3cret")
Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem",
       scram_username="alice", scram_password="s3cret")
# auth_token set → Auth only, even if scram_* are also set
# scram_username without scram_password (or vice versa) → ValueError
```

```go
DialScram("127.0.0.1:9092", "alice", "s3cret")
DialTLSScram("127.0.0.1:9092", TLSConfig{CAFile: "ca.pem"}, "alice", "s3cret")
// Dial / DialAuth / DialTLS / DialTLSAuth unchanged.
// Empty user or password → error before connect.
```

```java
Client.connectScram("127.0.0.1", 9092, "alice", "s3cret");
Client.connectTlsScram("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "alice", "s3cret");
// connect / connectTls overloads unchanged.
// Null or empty user or password → IllegalArgumentException.
```

Rejected first/final (`error_code != 0`) raises `BrokerError` /
`BrokerException` with `op="scram first"` or `op="scram final"` and
closes the socket. A wrong server signature raises `ProtocolError`.

TLS knobs from v0.27 and `max_redirects` from v0.43 are unchanged.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode 60–63 | fixtures in `test_codec` / `codec_test` / `CodecTest` |
| Pinned crypto vector | proof + server signature hex above |
| Connect with user+pass | first frames 60 then 62 |
| Bad password | constructor/Dial fails with code 17 |
| Bad server signature | constructor/Dial fails (protocol) |
| No creds | neither Auth nor SCRAM |
| `auth_token` + SCRAM | Auth only |
| Username xor password | constructor error, no socket |

## Honesty leftovers

- **SCRAM-SHA-512** is still not implemented on these clients.
- Not Kafka SASL. Native port only.
- Does not change broker SCRAM, ACLs, or mTLS principal mapping.
- Language clients do not expose Create/Delete/ListScramUsers.
- Shared-token Auth (v0.42) still wins when a token is set.

See [ops.md](./ops.md) (`## SCRAM-SHA-256`), [V42_SPEC.md](./V42_SPEC.md)
(shared-token Auth), and [PHASE22_SPEC.md](./PHASE22_SPEC.md) (broker).
