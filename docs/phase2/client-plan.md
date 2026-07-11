# Phase 2 Client SDK Plan

## Connection model

- Single TCP connection to the first broker address in `ClientConfig.brokers`.
- `tokio::sync::Mutex<TcpStream>` serializes request/response pairs (simple, correct).
- Each RPC:
  1. Allocate `correlation_id` via `AtomicU32` (starts at 1, increments per request).
  2. `pack_request` → frame with `checksum = crc32(payload)`.
  3. Write full frame under the mutex.
  4. Read frames until a complete frame is available; verify magic/version/checksum/correlation_id.
  5. Decode response payload (LE multi-byte integers).
- `request_timeout` wraps the whole RPC with `tokio::time::timeout`.

## Public API

```rust
Client::connect(ClientConfig) -> Result<Client>
Client::create_topic / delete_topic / metadata / produce / fetch

Producer::new(Arc<Client>) / send(topic, Message) -> ProduceResult
Consumer::new(Arc<Client>, topic, partition) / poll / fetch helpers
```

`ClientConfig`: `brokers`, `client_id`, `request_timeout` (default 5s).

## Error mapping (wire → `volant_core::Error`)

| code | meaning      | mapped error                          |
|------|--------------|---------------------------------------|
| 0    | ok           | (success path)                        |
| 1    | unknown      | Protocol                              |
| 2    | not_found    | NotFound                              |
| 3    | invalid_arg  | InvalidArgument                       |
| 4    | storage      | Storage                               |
| 5    | protocol     | Protocol                              |
| 6    | io           | Io                                    |
| 7    | timeout      | Io(TimedOut)                          |
| 8    | unsupported  | NotImplemented / InvalidArgument      |

Error frames (`opcode=0xFFFF`) and non-zero `error_code` in typed responses both map via this table.

## Protocol dependency (light touch)

Client needs real `Request`/`Response` fields plus:

- `encode_request` / `decode_request` / `encode_response` / `decode_response`
- `pack_request` / `pack_response`
- Opcode update: `DeleteTopic = 5`; OffsetCommit/OffsetFetch reserved as 6/7

Payload encoding: little-endian integers; strings `u16 len` + UTF-8; bytes `u32 len` + data; optional bytes use `u32::MAX` for None.

## Tests

1. **Protocol unit tests** — request/response payload roundtrips, pack/unpack frames with checksum.
2. **Client unit tests** — encode path via `pack_request` helpers; error code mapping.
3. **Mock TCP integration test** — spawn a tiny echo/handler server on `127.0.0.1:0` that responds to CreateTopic / Produce / Fetch / Metadata / DeleteTopic; exercise full client RPC.
4. **E2E with real broker** — `tests/e2e_tcp.rs` marked `#[ignore]` until broker net lands (or skipped when no server).

## Iteration log

- Iteration 1: implement protocol payloads + Client/Producer/Consumer + mock tests.
  - Result: all tests green; review checklist complete; no further iterations needed.
