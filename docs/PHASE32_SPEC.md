# Phase 32 — Kafka compressed Fetch (RecordBatch)

## Goals

1. **Compress** Fetch v4 RecordBatch responses on `--kafka-listen` (gzip /
   snappy / lz4 / zstd), reusing Phase 28 codecs
2. Default codec **lz4**; override via `VOLANT_KAFKA_FETCH_COMPRESSION`
3. Clients that already decompress Produce batches can read Fetch the same way
4. Tests + docs honesty

## Non-goals

- Compressing legacy MessageSet Fetch (v0–3) — still uncompressed
- Client-negotiated preferred codec (no Kafka wire field for this on classic Fetch)
- Storing compressed segments on disk (Volant log stays plain; re-encode on Fetch)
- Compression level knobs
- `READ_COMMITTED` / control batches

## Policy

| Fetch version | Record encoding | Compression |
|---------------|-----------------|-------------|
| 0–3 | MessageSet magic 1 | **none** |
| 4 | RecordBatch magic 2 | configurable (default **lz4**) |

- Empty partition responses stay empty (no batch).
- Non-empty v4 responses use `encode_record_batch_compressed`.
- Encode failure falls back to uncompressed (should not happen for supported codecs).

### Env: `VOLANT_KAFKA_FETCH_COMPRESSION`

| Value | Codec |
|-------|-------|
| `none` / `0` | uncompressed |
| `gzip` / `1` | gzip |
| `snappy` / `2` | snappy (Xerial) |
| `lz4` / `3` | lz4 (default) |
| `zstd` / `4` | zstd |

Unknown values → warn once, use **lz4**. Read once at first Fetch (process lifetime).

## Wire

Unchanged Fetch request/response framing. Only the `record_set` BYTES payload
for v4 may have attributes bits 0–2 ≠ 0. CRC-32C covers attributes…end as in
Phase 28.

## Exit criteria

1. Produce plain records → Fetch v4 returns compressed batch (default lz4);
   decode yields original values
2. Env `none` → Fetch v4 attributes compression bits = 0
3. Env gzip/snappy/lz4/zstd → matching attributes + round-trip decode
4. Fetch v0–3 still MessageSet uncompressed
5. Phase 28 produce-compressed tests still green (decode Fetch regardless of codec)
6. `cargo test --workspace` green

## Honest limitations

- MessageSet Fetch never compressed
- Codec is broker-global env, not per-topic / per-client
- Log storage remains uncompressed (CPU cost on every Fetch encode)
- No flexible versions / preferred-read-replica interaction
