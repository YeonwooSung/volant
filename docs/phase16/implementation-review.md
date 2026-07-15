# Phase 16 — Implementation review

## Delivered

| Item | Status |
|------|--------|
| `cleanup.policy` config | Done |
| Sparse offset recovery / append_allow_gap | Done |
| `PartitionLog::compact_sealed` | Done |
| Background via apply_retention | Done |
| CLI `--cleanup-policy` | Done |
| Tests | Done |

## Honest limits

- No dirty-ratio gating
- Active segment not compacted until roll
- Tombstones drop at compact (no tombstone retention.ms)
- Per-replica independent compact in cluster

## Verification

`cargo test --workspace` green.
