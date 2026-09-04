# v0.238 — Native SCRAM-SHA-512 handshake trailer

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Native SCRAM handshake can use **SHA-512**. The broker
already stores both hashes; Kafka SASL already has SHA-512. Language
clients were SHA-256 only (v0.46).

This is residual **v0.238**. It is **not** a new Kafka API key. Native
opcodes **60–63** only. Do **not** touch DescribeLogDirs / ElectLeaders
/ DescribeTopicPartitions / ListOffsets. Do **not** change `group.rs`.

## Goals

1. `Request::ScramFirst` optional `u8 hash` trailer after existing
   fields.
2. Broker `handle_scram` uses `begin_with_hash(..., Sha512)` when the
   trailer is **2**. `finish` honors `chal.hash`.
3. All four clients send trailer **2** when configured for SHA-512.
   Default and legacy stay SHA-256.

## Non-goals

| Deferred | Why |
|----------|-----|
| New Kafka API keys | Frozen |
| Kafka SASL SHA-512 | Already shipped (Phase 34) |
| DescribeLogDirs / ElectLeaders / DTP / ListOffsets | Sibling leftovers |
| `group.rs` | Orthogonal |
| Changing DialScram / connect default | Stay SHA-256 |
| Crate 0.3.0 | Stays 0.2.0 |

## Wire (native ScramFirst)

```
string username
string client_nonce
u8     hash     // v0.238; omit on legacy
```

| `hash` | Algorithm |
|-------:|-----------|
| missing / 0 / 1 | SHA-256 (today) |
| 2 | SHA-512 |
| other | `InvalidArg` on the broker |

`ScramFinal.client_proof` is already `Bytes` (32 or 64). No opcode
change. Legacy payloads without the trailer stay SHA-256.

## Clients

- Rust `ClientConfig::scram_hash` (default **0** = SHA-256). Set **2**
  for SHA-512. `authenticate_scram` default remains 256. Dial /
  `connect` helpers that do not set the field stay SHA-256.
- Proof helpers next to existing SHA-256 (`dklen` 64, SHA-512 /
  HMAC-SHA-512).
- Python / Go / Java: same trailer and proof helpers. Default
  Dial / connect unchanged.

## Tests

```bash
cargo test -p volant-protocol --lib -- --test-threads=1
cargo test -p volant-broker --lib scram -- --test-threads=1
cargo test -p volant-client --lib -- --test-threads=1
```

- Protocol: trailer 2 roundtrip; legacy no-trailer = 256.
- Broker: create user, native ScramFirst `hash=2` + Final 64-byte
  proof succeeds.
- Language unit tests for SHA-512 proof helpers.
