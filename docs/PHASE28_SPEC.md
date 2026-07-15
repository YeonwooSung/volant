# Phase 28 — Kafka RecordBatch compression

## Goals

1. **Decompress** RecordBatch payloads on Produce (shim) for codecs used by real clients
2. **Compress** when encoding RecordBatches (tests + optional Fetch path)
3. Keep uncompressed path as default for Fetch (compatibility / CPU)
4. Tests for gzip / snappy / lz4 / zstd round-trips
5. Docs honesty

## Non-goals

- Compressing legacy MessageSet magic 0/1 (still uncompressed only)
- Preferring a codec based on client config negotiation
- Partial batch / control-batch compression edge cases
- Hardware-accelerated codecs

## Codecs (attributes bits 0–2)

| Value | Codec | Notes |
|------:|-------|-------|
| 0 | none | existing path |
| 1 | gzip | `flate2` |
| 2 | snappy | Xerial framed (Kafka default) + raw snappy fallback |
| 3 | lz4 | LZ4 frame (`lz4_flex`) with block fallback |
| 4 | zstd | `zstd` crate |

## Wire semantics

When compression ≠ none, bytes after `recordsCount` are a single compressed blob.
CRC-32C still covers attributes…end (including compressed bytes). After
decompress, parse `recordsCount` DefaultRecords from the plain buffer.

## Encode

- `encode_record_batch` remains **uncompressed** (attributes=0) for Fetch responses
- `encode_record_batch_compressed(records, codec)` for tests and future use
- Codec 0 delegates to uncompressed

## Exit criteria

1. Produce gzip/snappy/lz4/zstd RecordBatch → native fetch sees plain records
2. Compressed encode → `decode_records` round-trip
3. Uncompressed path still green
4. Unknown codec bits → clear protocol error
5. `cargo test --workspace` green

## Honest limitations

- MessageSet compression still unsupported
- Snappy/LZ4 framing is best-effort Kafka-compatible; exotic client variants may fail
- Fetch still returns uncompressed batches
- No compression level knobs
