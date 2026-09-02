# v0.42 — shared-token Auth on Python / Go / Java clients

**Status:** Shipped (bounded MVP; not Phase 155)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the leftover “Python / Go / Java clients have no
shared-token Auth” by sending native opcode **30** after connect when
the caller sets a token, matching Rust `ClientConfig.auth_token`.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka SASL / SCRAM, add native opcodes, or change
the broker Auth handler.

## Goals

1. **Codec** encode/decode for Auth request (opcode 30) and Auth
   response (opcode 31) in Python, Go, and Java. Reuse each language’s
   existing `put_string` / `get_string`.
2. **Send Auth once** immediately after the socket is connected (and
   TLS handshake done, if any), before any other RPC. A rejected token
   fails the constructor / Dial, not the first produce.
3. **Empty / unset token** skips Auth (today’s behavior).
4. **Unit tests** without a broker: payload fixtures (`token = "s3cret"`;
   response `error_code` 0 and 17) plus a fake server that records the
   first opcode.
5. Optional `VOLANT_E2E=1` live broker with `VOLANT_AUTH_TOKEN` is
   nice-to-have only; not required.

## Non-goals

| Deferred | Why |
|----------|-----|
| SCRAM (opcodes 60–69) | Explicitly out of this slice |
| Kafka SASL / `--kafka-listen` | Native clients only |
| Broker Auth handler / dual-token rotation | Already shipped (Phase 7) |
| New native opcodes | Do not add |
| Reconnect / leader redirect | Language clients have no reconnect helper; if one is added later it must re-Auth |
| Kafka API keys | Frozen |

## Wire recap (unchanged)

Matches `crates/volant-protocol/src/payload.rs`:

| Direction | Opcode | Body |
|-----------|--------|------|
| Request `Auth` | **30** | one `put_string` token (little-endian **u16** length + UTF-8; same helper as the rest of the native protocol) |
| Response `Auth` | **31** | `error_code: u16` LE. **0** = ok; **17** = `AuthenticationFailed` |

If the broker requires auth and the client skips it, later RPCs get
**18** `AuthenticationRequired` (broker-side; clients surface it as
the usual non-zero `error_code` / Error opcode).

Do not confuse `put_string` (u16) with `put_bytes` / optional bytes
(u32 length, `0xFFFFFFFF` = null). Auth uses the string helper.

## API

```python
Client("127.0.0.1:9092", auth_token="s3cret")
Client("127.0.0.1:9092", tls=True, tls_ca="ca.pem", auth_token="s3cret")
# auth_token=None or "" → no Auth RPC
```

```go
DialAuth("127.0.0.1:9092", "s3cret")
DialTLSAuth("127.0.0.1:9092", TLSConfig{CAFile: "ca.pem"}, "s3cret")
// Dial / DialTLS unchanged. Empty token skips Auth.
```

```java
Client.connect("127.0.0.1", 9092, "s3cret");
Client.connect("127.0.0.1", 9092, 5_000, "s3cret");
Client.connectTls("127.0.0.1", 9092, TlsOptions.ca("ca.pem"), "s3cret");
// connect / connectTls overloads unchanged. null or "" skips Auth.
```

Rejected Auth (`error_code != 0`) raises `BrokerError(17, op="auth")`
(Python), `BrokerError{Code: 17, Op: "auth"}` (Go), or
`BrokerException(17, "", "auth")` (Java) and closes the socket.

TLS knobs from v0.27 are unchanged and compose with `auth_token`.

## Tests

```bash
PYTHONPATH=clients/python/src python3 -m unittest discover -s clients/python/tests -q
(cd clients/go && go test ./...)
(cd clients/java && mvn -q test)
```

| Case | Expect |
|------|--------|
| Encode/decode token `"s3cret"` | bytes `06 00 73 33 63 72 65 74` |
| Response 0 / 17 | bytes `00 00` / `11 00`; `decode_response(31, …)` |
| Connect with token | first frame opcode 30, payload token |
| Connect with rejected token | constructor/Dial fails with code 17 |
| Connect with no / empty token | first frame is not Auth |

## Honesty leftovers

- **SCRAM** is still not implemented on these clients (Rust
  `volant-client` can).
- Not Kafka SASL. Native port only.
- Does not change broker Auth, ACLs, or mTLS principal mapping.
- No dual-token window; rotation still means reconnect (see
  [ops.md](./ops.md)).

See [ops.md](./ops.md) (`## Shared-token auth`) and
[V27_SPEC.md](./V27_SPEC.md) (TLS, which this slice does not replace).
