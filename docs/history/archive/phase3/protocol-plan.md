# Phase 3 Protocol — Consumer Group & Offset Wire Payloads

## Scope

Implement consumer-group and offset **wire** request/response types and LE encode/decode
in `crates/volant-protocol` only. No broker/client TCP logic.

## Files to touch

| Path | Action |
|------|--------|
| `docs/phase3/protocol-plan.md` | Create (this file) |
| `docs/phase3/protocol-review.md` | Create after implementation |
| `crates/volant-protocol/src/request.rs` | Full OffsetCommit/OffsetFetch + JoinGroup/Heartbeat/LeaveGroup |
| `crates/volant-protocol/src/response.rs` | Matching responses + ErrorCode 9–12 |
| `crates/volant-protocol/src/payload.rs` | LE encode/decode for all new variants + roundtrip tests |
| `crates/volant-protocol/src/lib.rs` | Re-export new public types |
| `crates/volant-broker/src/net.rs` | Minimal match-arm update so workspace builds (NotImplemented) |

## Opcodes (PHASE3_SPEC)

| Opcode | Request | Response |
|--------|---------|----------|
| 6 | OffsetCommit | OffsetCommit |
| 7 | OffsetFetch | OffsetFetch |
| 8 | JoinGroup | JoinGroup |
| 9 | Heartbeat | Heartbeat |
| 10 | LeaveGroup | LeaveGroup |

## Error codes (additions)

| Code | Name |
|------|------|
| 9 | RebalanceInProgress |
| 10 | UnknownMemberId |
| 11 | IllegalGeneration |
| 12 | InconsistentGroupProtocol |

## Request / response shapes

### Request

```rust
// OffsetCommit { group_id, member_id, generation, entries: Vec<OffsetCommitEntry> }
// OffsetCommitEntry { topic, partition: u32, offset: u64, metadata: String }

// OffsetFetch { group_id, entries: Vec<OffsetFetchEntry> } // empty entries = all
// OffsetFetchEntry { topic, partition: u32 }

// JoinGroup { group_id, member_id, session_timeout_ms: u32, topics: Vec<String> }
// Heartbeat { group_id, member_id, generation: u32 }
// LeaveGroup { group_id, member_id }
```

### Response

```rust
// OffsetCommit { error_code: u16 }
// OffsetFetch { error_code: u16, entries: Vec<OffsetFetchResult> }
// OffsetFetchResult { topic, partition: u32, offset: u64, metadata: String }
// JoinGroup { error_code: u16, generation: u32, member_id, assignment: Vec<TopicPartition> }
// TopicPartition { topic, partition: u32 }
// Heartbeat { error_code: u16 }
// LeaveGroup { error_code: u16 }
```

## Encode / decode

Reuse existing LE helpers in `payload.rs`:

- string: `u16 len` + UTF-8
- multi-byte integers: little-endian
- repeated fields: `u32 count` + items

Public API unchanged:

```rust
encode_request / decode_request
encode_response / decode_response
pack_request / pack_response
```

## Tests

1. Roundtrip OffsetCommit request (multi-entry + empty metadata)
2. Roundtrip OffsetFetch request (empty entries = all; non-empty filter)
3. Roundtrip JoinGroup request/response (with assignment)
4. Roundtrip Heartbeat request/response
5. Roundtrip LeaveGroup request/response
6. Roundtrip OffsetCommit/OffsetFetch responses
7. ErrorCode::from_u16 maps 9–12 correctly
8. Opcode values 6–10 parse correctly
9. Existing Phase 2 tests still pass

## Risks

- Changing unit-variant placeholders to field structs breaks broker match arms — fix with `{ .. }` + NotImplemented
- `#![deny(missing_docs)]` requires docs on all new public items
- Empty `entry_count` for OffsetFetch means "all" — encode as count 0, not a special sentinel
- `u64::MAX` offset means unknown on OffsetFetch response (semantic, not encode special-case)
