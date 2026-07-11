# Phase 2 Client SDK Review

**Iteration:** 1 (complete — tests green, no open findings)

## Checklist

- [x] `connect` / `create_topic` / `delete_topic` / `metadata` / `produce` / `fetch` work
  - Exercised end-to-end against mock TCP broker in `tests/mock_tcp.rs`
- [x] `correlation_id` incremented
  - `AtomicU32` starting at 1; verified in unit + integration tests
- [x] Checksum set on outbound frames
  - `pack_request` always sets `checksum = crc32(payload)`; verified in encode-path unit test
- [x] Timeouts configurable
  - `ClientConfig.request_timeout` (default 5s); applied to connect + each RPC via `tokio::time::timeout`
- [x] Producer / Consumer wrappers
  - `Producer::new(Arc<Client>)` / `send` / `send_to` / `send_batch`
  - `Consumer::new(Arc<Client>, topic, partition)` / `poll` / `fetch` with offset tracking
- [x] docs + `missing_docs`
  - `#![deny(missing_docs)]` on client and protocol crates; all public items documented

## Protocol support (light touch)

Implemented real `Request` / `Response` payloads and codec helpers per `PHASE2_SPEC.md`:

- Opcodes: Produce=1, Fetch=2, CreateTopic=3, Metadata=4, DeleteTopic=5, Error=0xFFFF
- `encode_request` / `decode_request` / `encode_response` / `decode_response`
- `pack_request` / `pack_response` with CRC32
- Frame decode verifies checksum and max payload (16 MiB)
- Payload multi-byte integers are little-endian

## Error mapping

Wire codes → `volant_core::Error` via `error_map::map_error_code` (not_found, invalid_arg, storage, protocol, io, timeout, etc.).

## Tests

| Suite | Result |
|-------|--------|
| `cargo test -p volant-protocol` | 8 passed |
| `cargo test -p volant-client` unit | 4 passed |
| `tests/mock_tcp.rs` | 3 passed |
| `tests/e2e_tcp.rs` | 1 ignored (needs real server) |

## Findings

None open after iteration 1.

### Fixed during implementation

1. Double-borrow on `ClientStream` fields during write+read — destructure under mutex.
2. `Debug` for `Client` required by Producer/Consumer derives — manual `Debug` impl (stream omitted).

## Non-goals / deferred

- Connection pipelining / split read-write tasks (mutex sequential RPC is OK per spec)
- Real broker e2e (ignored until server net lands)
- Retry / idempotent produce (stretch)
