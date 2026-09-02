# v0.23 — Java native client (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** A sync Java client that speaks the **native** Volant protocol
(not Kafka, not `kafka-clients`) for create / produce / fetch / metadata.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change broker behavior.

## Goals

1. **Package** `clients/java/` (`io.volant:volant-client`, package
   `io.volant`).
2. **Frame** encode/decode matching `crates/volant-protocol/src/codec.rs`,
   `clients/python/src/volant/frame.py`, and `clients/go/frame/`: magic
   `V` (0x56), version 1, big-endian 16-byte header, CRC32 of payload only
   (`java.util.zip.CRC32` ≡ `zlib.crc32` ≡ `hash/crc32` IEEE ≡
   `crc32fast`).
3. **Payloads** matching `crates/volant-protocol/src/payload.rs`,
   `clients/python/src/volant/codec.py`, and `clients/go/codec/` for
   Produce / Fetch / CreateTopic / Metadata / DeleteTopic (little-endian
   bodies, Phase 10 produce trailer, Phase 13 create-topic configs).
4. **Sync TCP `Client.connect`:** incrementing correlation ids; error on
   magic / version / checksum mismatch; `error_code != 0` is
   `BrokerException`.
5. **Tests** that run without a broker (`FrameTest`, `CodecTest`).
   Optional e2e gated by `VOLANT_E2E=1`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Other language SDKs | This slice is Java only (Python v0.14, Go v0.19) |
| `kafka-clients` / Kafka wire | Native protocol; shim is `--kafka-listen` |
| Consumer groups / offset commit | Out of MVP |
| TLS / SCRAM / shared-token Auth | Sync plaintext only |
| Async / NIO / reactive API | Sync `Socket` is enough for the advertised API |
| Idempotent produce / txn | Trailer written as `(0, 0, -1)` |
| Leader redirect / reconnect | Single connection |
| New Rust crate / workspace member | Java lives under `clients/` |
| Required Java job on default CI | Optional `scripts/java_client_smoke.sh` only |

## Wire (recap)

Header (big-endian):

```
magic u8 | version u8 | opcode u16 | correlation_id u32 | payload_len u32 | crc32 u32 | payload
```

Payloads are little-endian. Strings are `u16_le` length + UTF-8; bytes are
`u32_le` length + data; optional bytes use `u32::MAX` for null.

| Opcode | Request | Response |
|--------|---------|----------|
| 1 | Produce | Produce |
| 2 | Fetch | Fetch |
| 3 | CreateTopic | CreateTopic |
| 4 | Metadata | Metadata |
| 5 | DeleteTopic | DeleteTopic |
| 0xFFFF | — | Error (`u16_le` code + string) |

Produce request (current encoder): topic, `i32` partition, `u8` acks,
messages (`optional key`, `value`, `i64` timestamp, headers), then
`producer_id u64` + `epoch u16` + `base_sequence i32`. Fetch / metadata /
create-topic layouts follow `payload.rs` exactly.

## API

```java
try (Client c = Client.connect("127.0.0.1", 9092)) {
  c.createTopic("t", 1);
  long off = c.produce("t", 0, null, "hello".getBytes(UTF_8));
  List<Record> recs = c.fetch("t", 0, 0);
  Metadata meta = c.metadata();
}
```

- `produce` third argument is the key (`null` = null).
- `fetch` returns `List<Record>` (`offset`, `key`, `value`).
- `metadata()` returns brokers + topics.
- `deleteTopic` is implemented (same opcode as Python/Go) but not part of
  the one-liner advertised API.
- `createTopic` returns the broker-assigned topic id (callers may ignore).

`ProtocolException` covers magic / version / checksum / framing / I/O.
`BrokerException` covers `error_code != 0` and the Error opcode.

## Tests

| File | What |
|------|------|
| `clients/java/src/test/java/io/volant/FrameTest.java` | Roundtrip, IEEE CRC, magic/version/checksum reject |
| `clients/java/src/test/java/io/volant/CodecTest.java` | Exact-byte produce/fetch/create/metadata fixtures |
| `clients/java/src/test/java/io/volant/E2ETest.java` | Live create/produce/fetch; skip unless `VOLANT_E2E=1` |

```bash
cd clients/java && mvn -q test
VOLANT_E2E=1 mvn -q test   # needs volant-server
```

`scripts/java_client_smoke.sh` runs the non-e2e suite when `mvn` exists.

## Honesty

This is a bounded native MVP. It is **not** a Kafka client, **not**
`kafka-clients`, and **not** production-hardened (no TLS, no redirects,
no groups). Broker and Rust `volant-client` are unchanged.

See [clients/java/README.md](../clients/java/README.md).
