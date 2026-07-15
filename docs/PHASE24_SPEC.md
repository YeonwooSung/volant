# Phase 24 — Kafka RecordBatch (magic 2)

## Goals

1. **Decode** Kafka RecordBatch (magic=2) on Produce (shim port)
2. **Encode** RecordBatch on Fetch responses for modern clients
3. Keep MessageSet magic 0/1 working (Phase 23)
4. Auto-detect produce payload format by magic byte
5. Bump advertised Produce/Fetch API version ranges modestly
6. Tests + docs honesty

## Non-goals

- Compression (gzip/snappy/lz4/zstd) — attributes compression bits must be 0
- Transactional / idempotent producer fields (pid/epoch/sequence stored but ignored)
- Flexible versions / tagged fields
- Full Fetch v4 aborted-transactions semantics
- Kafka SASL / consumer groups

## Detection

Record sets are inspected at byte offset 16 (magic):

| Magic | Format |
|------:|--------|
| 0, 1 | Legacy MessageSet (Phase 23) |
| 2 | RecordBatch (this phase) |

## RecordBatch layout (Kafka)

```
baseOffset: i64
batchLength: i32          # bytes after this field through end of batch
partitionLeaderEpoch: i32
magic: i8                 # = 2
crc: u32                  # CRC-32C (Castagnoli) over attributes..end
attributes: i16           # compression must be 0 for MVP
lastOffsetDelta: i32
firstTimestamp: i64
maxTimestamp: i64
producerId: i64
producerEpoch: i16
baseSequence: i32
recordsCount: i32
records: Record × N
```

### Record (varint fields, zig-zag for signed)

```
length: varint
attributes: i8
timestampDelta: varint
offsetDelta: varint
keyLen: varint            # -1 = null
key: bytes
valueLen: varint          # -1 = null
value: bytes
headerCount: varint
headers: [keyLen, key, valueLen, value]
```

Headers map to Volant `Message.headers`.

## API versions (advertised)

| API | Min | Max | Notes |
|-----|----:|----:|-------|
| Produce | 0 | 3 | v0 body; RecordBatch accepted on any version |
| Fetch | 0 | 4 | v0 body shape for v0–3; v4 adds throttle + LSO |
| Metadata | 0 | 1 | unchanged |
| ApiVersions | 0 | 0 | unchanged |

### Fetch response

- **v0–3:** MessageSet in records field (Phase 23 behaviour)
- **v4:** RecordBatch in records field; `throttle_time_ms=0`, `last_stable_offset=hwm`, empty aborted txns

## Exit criteria

1. Produce RecordBatch → readable via Volant client and Kafka Fetch
2. Fetch v4 returns RecordBatch round-trip of keys/values/timestamps/headers
3. MessageSet produce/fetch still green
4. Unsupported compression → clear error
5. `cargo test --workspace` green

## Honest limitations

- No compression
- No control batches / transactional markers
- CRC is CRC-32C only (RecordBatch); MessageSet remains IEEE CRC-32
- Fetch v4 is a minimal subset (no isolation / aborted txn filtering)
