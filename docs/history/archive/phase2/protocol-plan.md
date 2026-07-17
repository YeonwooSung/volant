# Phase 2 Protocol Codec — Plan

## Scope

Implement wire request/response payload codecs in `crates/volant-protocol` only.
TCP server/client/CLI are owned by other agents.

## Files to touch

| Path | Action |
|------|--------|
| `docs/phase2/protocol-plan.md` | Create (this file) |
| `docs/phase2/protocol-review.md` | Create after implementation |
| `crates/volant-protocol/src/request.rs` | Full Request enum + opcodes + field structs |
| `crates/volant-protocol/src/response.rs` | Full Response enum + opcodes + ErrorCode + field structs |
| `crates/volant-protocol/src/codec.rs` | encode/decode + pack helpers + LE payload primitives |
| `crates/volant-protocol/src/lib.rs` | Re-export public API |
| `crates/volant-protocol/src/frame.rs` | Leave mostly as-is (header already BE) |

## Opcodes (PHASE2_SPEC)

| Opcode | Request | Response |
|--------|---------|----------|
| 1 | Produce | Produce |
| 2 | Fetch | Fetch |
| 3 | CreateTopic | CreateTopic |
| 4 | Metadata | Metadata |
| 5 | DeleteTopic | DeleteTopic |
| 6 | (reserved OffsetCommit) | |
| 7 | (reserved OffsetFetch) | |
| 0xFFFF | — | Error |

## Request / Response shapes

### Request

```rust
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    CreateTopic(CreateTopicRequest),
    Metadata(MetadataRequest),
    DeleteTopic(DeleteTopicRequest),
}

// ProduceRequest { topic, partition: i32, acks: u8, messages: Vec<ProduceMessage> }
// ProduceMessage { key: Option<Bytes>, value: Bytes, timestamp_ms: i64, headers: Vec<Header> }
// Header { name: String, value: Bytes }
// FetchRequest { topic, partition, from_offset, max_messages, max_bytes, max_wait_ms }
// CreateTopicRequest { name, partitions }
// DeleteTopicRequest { name }
// MetadataRequest { topics: Vec<String> } // empty = all
```

### Response

```rust
pub enum Response {
    Produce(ProduceResponse),
    Fetch(FetchResponse),
    CreateTopic(CreateTopicResponse),
    Metadata(MetadataResponse),
    DeleteTopic(DeleteTopicResponse),
    Error(ErrorResponse),
}
```

Plus nested metadata/broker/partition structs and `ErrorCode` (0–8).

## Encode / decode helpers

Payload encoding (all multi-byte integers LE):

- string: `u16 len` + UTF-8
- bytes: `u32 len` + data
- optional bytes: `u32 len` where `u32::MAX` ⇒ None

Public API:

```rust
pub fn encode_request(req: &Request) -> Result<Bytes>;
pub fn decode_request(opcode: u16, payload: &[u8]) -> Result<Request>;
pub fn encode_response(resp: &Response) -> Result<Bytes>;
pub fn decode_response(opcode: u16, payload: &[u8]) -> Result<Response>;
pub fn pack_request(corr: u32, req: &Request) -> Result<Frame>;
pub fn pack_response(corr: u32, resp: &Response) -> Result<Frame>;
```

`pack_*` builds `Frame` with correct opcode, `payload_len`, and `checksum = crc32(payload)`.

Decode path: optional strict CRC verify helper; pack uses CRC; add `verify_frame_checksum` / verify inside unpack helpers where appropriate.

Max payload: 16 MiB — reject larger on encode and decode.

## Test cases

1. Roundtrip every Request variant (Produce empty/non-empty, Fetch, CreateTopic, DeleteTopic, Metadata empty/all topics)
2. Roundtrip every Response variant including Error
3. Optional key None (`u32::MAX`) and Some
4. Headers present / empty
5. Large-ish string (within u16)
6. Empty message batches / empty records
7. pack_request / pack_response: opcode, corr, crc match
8. checksum mismatch rejection on verify
9. Truncated / trailing garbage payload → Protocol error
10. Unknown opcode → Protocol error

## Risks

- LE payload vs BE frame header — must not mix endianness
- `u16` string length caps at 65535 bytes; reject longer
- Optional bytes sentinel `u32::MAX` must not collide with real lengths (real max 16 MiB so OK)
- `#![deny(missing_docs)]` requires docs on all public items
- Changing opcode enums (DeleteTopic=5, drop OffsetCommit/Fetch from public Request) may break future agents — follow PHASE2_SPEC
