# v0.14 — Python native client (MVP)

**Status:** Shipped (MVP)  
**Crate:** 0.2.0 (unchanged)  
**Theme:** A sync Python client that speaks the **native** Volant protocol
(not Kafka, not `kafka-python`) for create / produce / fetch / metadata.

This is a residual slice under “Multi-language clients”. It does **not**
open Phase 155, add Kafka API keys, or change broker behavior.

## Goals

1. **Package** `clients/python/` (`volant` on PyPI-style install, import
   `volant`).
2. **Frame** encode/decode matching `crates/volant-protocol/src/codec.rs`:
   magic `V` (0x56), version 1, big-endian 16-byte header, CRC32 of payload
   only (`zlib.crc32` ≡ `crc32fast` IEEE).
3. **Payloads** matching `crates/volant-protocol/src/payload.rs` for
   Produce / Fetch / CreateTopic / Metadata / DeleteTopic (little-endian
   bodies, Phase 10 produce trailer, Phase 13 create-topic configs).
4. **Sync TCP `Client`:** incrementing correlation ids; raise on magic /
   version / checksum mismatch; value-only produce (null key OK).
5. **Tests** that run without a broker (`test_frame.py`, `test_codec.py`).
   Optional e2e gated by `VOLANT_E2E=1`.

## Non-goals

| Deferred | Why |
|----------|-----|
| Java / Go / other language SDKs | This slice is Python only |
| `kafka-python` / Kafka wire | Native protocol; shim is `--kafka-listen` |
| Consumer groups / offset commit | Out of MVP |
| TLS / SCRAM / shared-token Auth | Sync plaintext only |
| Async (asyncio) | Sync is enough for the advertised API |
| Idempotent produce / txn | Trailer written as `(0, 0, -1)` |
| Leader redirect / reconnect | Single connection |
| New Rust crate / workspace member | Python lives under `clients/` |

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

```python
from volant import Client
c = Client("127.0.0.1:9092")
c.create_topic("t", partitions=1)
c.produce("t", 0, value=b"hello")
batch = c.fetch("t", 0, offset=0)
meta = c.metadata()
c.close()
```

- `produce` also accepts `key=` and `messages=`.
- `fetch` returns `FetchResult` (`.records`, `.tuples()` →
  `(offset, key, value)`).
- `metadata()` returns brokers + topics.

## Tests

| File | What |
|------|------|
| `clients/python/tests/test_frame.py` | Roundtrip, IEEE CRC, magic/version/checksum reject |
| `clients/python/tests/test_codec.py` | Exact-byte produce/fetch/create/metadata fixtures |
| `clients/python/tests/test_e2e.py` | Live create/produce/fetch; skip unless `VOLANT_E2E=1` |

```bash
cd clients/python && python3 -m pytest -q
# or: PYTHONPATH=src python3 -m unittest discover -s tests -q
VOLANT_E2E=1 python3 -m pytest -q   # needs volant-server
```

`scripts/python_client_smoke.sh` runs the non-e2e suite when `python3` exists.

## Honesty

This is a bounded native MVP. It is **not** a multi-language SDK family,
**not** a Kafka client, and **not** production-hardened (no TLS, no
redirects, no groups). Broker and Rust `volant-client` are unchanged.

See [clients/python/README.md](../clients/python/README.md).
