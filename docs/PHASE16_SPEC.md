# Phase 16 — Log compaction (`cleanup.policy`) (binding)

## Goals

1. Topic config **`cleanup.policy`**: `delete` (default) | `compact`
2. **Sealed-segment compaction**: keep latest value per key; empty value = tombstone
3. Background apply with retention loop; explicit test hook
4. CLI create/config support
5. Docs honesty

## Non-goals

- Compact + delete hybrid dirty-ratio tuning (Kafka `min.cleanable.dirty.ratio`)
- Compacting the active segment
- Transaction markers / control records
- Cooperative rebalance / Kafka shim / SCRAM

## Config

| Key | Values | Default |
|-----|--------|---------|
| `cleanup.policy` | `delete`, `compact`, empty=delete | `delete` |

Stored in `__topic_configs/` with other Phase 13 keys.

## Compaction semantics

1. Only **sealed** segments are compacted (active segment untouched).
2. Requires ≥1 sealed segment.
3. Scan sealed records in offset order:
   - **Keyed**: last non-tombstone wins; **empty value** removes the key (tombstone).
   - **Null key**: retained (not compacted away).
4. Rewrite survivors into one sealed segment at the original first sealed base offset,
   preserving **original offsets** (sparse offsets / holes allowed).
5. Logical end of the compacted segment equals the active segment base (offset holes OK).
6. Fetch still returns survivors; consumers may see offset gaps.

## Storage

- `PartitionLog::compact_sealed() -> Result<CompactStats>`
- Recovery allows offset gaps inside a segment (`offset > expected` OK)
- Open allows `sealed.next_offset ≤ next.base_offset`

## Broker

- Apply `cleanup.policy` via topic config overlay (`set_cleanup_policy`)
- `apply_retention_all` also runs compaction when policy is `compact`
- Optional: same 5s background task

## CLI

```bash
volant topic create kv --cleanup-policy compact --segment-bytes 4096
volant topic config set kv --key cleanup.policy --value compact
```

## Exit criteria

1. Produce key A=v1, A=v2 across sealed segments → compact → fetch has only A=v2
2. Tombstone A (empty value) → compact → A gone
3. Null-key messages retained
4. Restart after compact still serves compacted data
5. `cargo test --workspace` green

## Honest limitations

- No dirty-ratio gating (compacts all sealed when policy=compact)
- Active segment not compacted until roll
- Tombstone retention.ms not implemented (tombstones drop at compact time)
- Multi-node: each replica compacts independently (no cleaner coordination)
