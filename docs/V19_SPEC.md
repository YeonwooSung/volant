# v0.19 — Go native client (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** A sync Go client that speaks the **native** Volant protocol
(not Kafka, not `kafka-go`) for create / produce / fetch / metadata.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change broker behavior.

## Goals

1. **Module** `clients/go/` (`github.com/volant-mq/volant/clients/go`,
   package `volant`).
2. **Frame** encode/decode matching `crates/volant-protocol/src/codec.rs`
   and `clients/python/src/volant/frame.py`: magic `V` (0x56), version 1,
   big-endian 16-byte header, CRC32 of payload only (`hash/crc32` IEEE ≡
   `zlib.crc32` ≡ `crc32fast`).
3. **Payloads** matching `crates/volant-protocol/src/payload.rs` and
   `clients/python/src/volant/codec.py` for Produce / Fetch / CreateTopic /
   Metadata / DeleteTopic (little-endian bodies, Phase 10 produce trailer,
   Phase 13 create-topic configs).
4. **Sync TCP `Dial`:** incrementing correlation ids; error on magic /
   version / checksum mismatch; `error_code != 0` is `BrokerError`.
5. **Tests** that run without a broker (`frame/frame_test.go`,
   `codec/codec_test.go`). Optional e2e gated by `VOLANT_E2E=1`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Java / other language SDKs | This slice is Go only (Python already shipped in v0.14) |
| `kafka-go` / Kafka wire | Native protocol; shim is `--kafka-listen` |
| Consumer groups / offset commit | Out of MVP |
| TLS / SCRAM / shared-token Auth | Sync plaintext only |
| Async / goroutine fan-out API | Sync is enough for the advertised API |
| Idempotent produce / txn | Trailer written as `(0, 0, -1)` |
| Leader redirect / reconnect | Single connection |
| New Rust crate / workspace member | Go lives under `clients/` |
| Required Go job on default CI | Optional `scripts/go_client_smoke.sh` only |

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

```go
c, err := volant.Dial("127.0.0.1:9092")
err = c.CreateTopic("t", 1)
off, err := c.Produce("t", 0, nil, []byte("hello"))
recs, err := c.Fetch("t", 0, 0)
meta, err := c.Metadata()
c.Close()
```

- `Produce` third argument is the key (`nil` = null).
- `Fetch` returns `[]Record` (`Offset`, `Key`, `Value`).
- `Metadata()` returns brokers + topics.
- `DeleteTopic` is implemented (same opcode as Python) but not part of
  the one-liner advertised API.

## Tests

| File | What |
|------|------|
| `clients/go/frame/frame_test.go` | Roundtrip, IEEE CRC, magic/version/checksum reject |
| `clients/go/codec/codec_test.go` | Exact-byte produce/fetch/create/metadata fixtures |
| `clients/go/e2e_test.go` | Live create/produce/fetch; skip unless `VOLANT_E2E=1` |

```bash
cd clients/go && go test ./...
VOLANT_E2E=1 go test ./...   # needs volant-server
```

`scripts/go_client_smoke.sh` runs the non-e2e suite when `go` exists.

## Honesty

This is a bounded native MVP. It is **not** a multi-language SDK family
(Java still missing), **not** a Kafka client, and **not**
production-hardened (no TLS, no redirects, no groups). Broker and Rust
`volant-client` are unchanged.

See [clients/go/README.md](../clients/go/README.md).
