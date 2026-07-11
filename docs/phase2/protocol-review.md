# Phase 2 Protocol Codec — Review

## Iteration 1

### Checklist

- [x] Matches PHASE2_SPEC opcodes & LE payload encoding
  - Produce=1, Fetch=2, CreateTopic=3, Metadata=4, DeleteTopic=5, Error=0xFFFF
  - OffsetCommit/OffsetFetch reserved at 6/7 (rejected on decode)
  - Payload multi-byte integers little-endian; frame header remains big-endian
  - Strings `u16`+UTF-8; bytes `u32`+data; optional bytes `u32::MAX` = None
- [x] CRC/frame helpers correct
  - `checksum` = crc32fast over payload
  - `pack_request` / `pack_response` set opcode, payload_len, checksum
  - `verify_frame_checksum`, `unpack_request`, `unpack_response` verify CRC
  - `encode_frame` / `decode_frame` enforce 16 MiB max payload
- [x] No placeholder Request/Response variants left
  - All variants carry real field structs
- [x] `#![deny(missing_docs)]` satisfied (compiles clean)
- [x] Edge cases covered by unit tests
  - Empty produce batch / empty fetch records / empty metadata
  - Null keys (`u32::MAX`)
  - Large strings (1000 chars)
  - Headers present and empty
  - Truncated / trailing bytes / unknown opcode / checksum mismatch

### Findings

1. **None blocking.** Implementation matches PHASE2_SPEC payload layouts field-for-field.
2. **Note:** `Header` exists in both `request` and `response` modules (same shape). Re-exported as `RequestHeader` / `ResponseHeader` from lib to avoid ambiguity. Acceptable.
3. **Note:** `unpack_request` / `unpack_response` added beyond the minimal public API; useful for server/client agents and covered by tests.
4. **Note:** Phase 3 reserved opcodes present on enum for documentation but decode returns Protocol error — correct per Phase 2 scope.

### Test results (iteration 1)

```
cargo test -p volant-protocol
# 25 passed; 0 failed
```

### Decision

No code fixes required after review. Proceed to workspace test.

## Iteration log

| Iter | Action | Result |
|------|--------|--------|
| 1 | Plan → implement → review → test | Green; no fixes needed |
