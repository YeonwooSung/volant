# Phase 33 — Kafka MessageSet compression

## Goals

1. **Decompress** compressed MessageSet (magic 0/1) on Produce
2. **Compress** MessageSet on Fetch v0–3 when fetch compression is enabled
3. Reuse Phase 28 codecs (gzip / snappy / lz4); zstd maps to **lz4** for MessageSet
4. Tests + docs honesty

## Non-goals

- Storing compressed MessageSets on disk (Volant log stays plain)
- Nested multi-level compression beyond one wrapper (decode still recursive with care)
- Magic-0-only encode path (we encode magic 1 wrappers)
- Per-topic compression config

## Wire semantics (Kafka MessageSet)

Compressed MessageSets use a **wrapper message**:

1. Encode each logical record as a normal MessageSet (attributes = 0)
2. Compress that inner MessageSet blob with the codec
3. Emit a **single** outer message:
   - `offset` = last record offset
   - `magic` = 0 or 1
   - `attributes` bits 0–2 = codec (1 gzip, 2 snappy, 3 lz4; **no classic zstd**)
   - `key` = null
   - `value` = compressed inner MessageSet

Decode: if attributes compression ≠ none, decompress `value` and parse as a nested MessageSet (do not surface the wrapper as a record).

## Produce

`decode_message_set` honors attributes bits 0–2. Clients that still produce
MessageSet (pre–RecordBatch) with compression work on the shim.

## Fetch v0–3

Uses the same `VOLANT_KAFKA_FETCH_COMPRESSION` as Phase 32:

| Env value | MessageSet codec |
|-----------|------------------|
| `none` | uncompressed |
| `gzip` / `snappy` / `lz4` | as named |
| `zstd` | **lz4** (MessageSet has no zstd) |

Default remains **lz4** (wrapper MessageSet).

Fetch v4 unchanged (RecordBatch compression from Phase 32).

## Exit criteria

1. Produce compressed MessageSet (gzip/snappy/lz4) → native fetch sees plain records
2. `encode_message_set_compressed` ↔ `decode_message_set` round-trip
3. Fetch v0 with default env → compressed wrapper; decode yields values
4. Uncompressed MessageSet path still green
5. `cargo test --workspace` green

## Honest limitations

- No zstd on MessageSet (mapped to lz4 on encode)
- Wrapper always magic 1 on encode
- Compression codec is process-global env only
- No MessageSet compression level knobs
