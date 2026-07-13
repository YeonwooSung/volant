# Phase 12 implementation review

## Scope delivered

| Item | Status |
|------|--------|
| ListGroups 36/37 | Done |
| DeleteOffsets 38/39 | Done |
| JoinGroup `group_instance_id` (static membership) | Done |
| Client + CLI | `list_groups`, `delete_offsets`, `join_group_with_instance`, `volant group list|delete-offsets` |
| Tests | `phase12_group_admin` + protocol unit |

## Verify

```bash
cargo test --workspace
cargo test -p volant-broker --test phase12_group_admin
```

## Honest limitations

- Eager rebalance only (no cooperative incremental revoke)
- Static membership is Volant-local (`static:{instance_id}` member ids)
- No SCRAM / mTLS / transactions / Kafka shim
