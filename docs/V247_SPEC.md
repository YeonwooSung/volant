# v0.247 — ACL TransactionalId on txn APIs

**Status:** Shipped
**Crate:** 0.2.0 (unchanged)
**Theme:** KAFKA_COMPAT said ACLs have **no TransactionalId**. Add
`ResourceType::TransactionalId` and consult it on txn APIs when ACLs
are enabled.

This is residual **v0.247**. LITERAL only. Empty `transactional_id`
still skips (idempotent-only path). Do **not** add Kafka API keys.
Do **not** touch DescribeQuorum, AllocateProducerIds, SyncGroup apply,
AlterReplicaLogDirs, or `group.rs`.

## Goals

1. Native `ResourceType::TransactionalId = 4` (next unused u8).
   Parse names `transactionalid` / `TransactionalId`.
2. Kafka ResourceType TransactionalId = **5**. Map 4 ↔ 5 in
   `kafka/acl_api.rs` (`volant_rt_to_kafka` / `kafka_rt_to_volant`).
   Create/Describe/DeleteAcls accept Kafka 5 / native 4 at every
   advertised version. LITERAL only still.
3. When ACLs are enabled and `transactional_id` is non-empty,
   authorize **Write** on resource = transactional id for:
   - InitProducerId
   - AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
   - Native InitProducerId (BeginTxn/EndTxn do not take a txn id)
4. Empty `transactional_id` → skip (existing Cluster Write /
   idempotent path).
5. Denied Kafka InitProducerId → **53**
   `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (tests also accept **29**).

## Non-goals

| Deferred | Why |
|----------|-----|
| DelegationToken resource type | Still unsupported |
| PREFIX / host != `*` | LITERAL / `*` only |
| Kafka API keys | Frozen |
| DescribeQuorum / AllocateProducerIds / SyncGroup apply / AlterReplicaLogDirs / `group.rs` | Sibling leftovers |
| Crate 0.3.0 | Stays 0.2.0 |

## Semantics

```
txn API (Init / AddPartitions / AddOffsets / End / TxnOffsetCommit)
  │
  ├─ ACLs off → unchanged
  ├─ transactional_id empty → skip TransactionalId (idempotent)
  │
  └─ ACLs on + non-empty transactional_id
          └─ Write on ResourceType::TransactionalId / id
             deny → 53 (or 29)
```

## Tests

```bash
cargo test -p volant-broker --lib acl -- --test-threads=1
cargo test -p volant-broker --test v247_txn_acl -- --test-threads=1
```

| Case | Expect |
|------|--------|
| Kafka CreateAcls TransactionalId + InitProducerId without grant | **29** or txn auth failed (**53**) |
| With grant | **0** |
| Native list/create ACL type name | `transactionalid` / `TransactionalId` |

| File | What |
|------|------|
| `crates/volant-broker/src/acl.rs` | `ResourceType::TransactionalId = 4` |
| `crates/volant-broker/src/kafka/acl_api.rs` | Kafka 5 ↔ native 4 |
| `crates/volant-broker/src/kafka/txn.rs` | consult Write on txn APIs |
| `crates/volant-broker/src/net/dispatch.rs` | native InitProducerId |
| `crates/volant-broker/tests/v247_txn_acl.rs` | boot_kafka + native list/create |
| `docs/KAFKA_COMPAT.md` | ACL row |
| `docs/V247_SPEC.md` | This spec |

## Honesty leftovers

- DelegationToken is still rejected.
- Host is still always `*`. Patterns are still LITERAL only.
- User ACLs remain storage + admin only (no SCRAM-admin gating).
- Native BeginTxn / EndTxn do not carry a transactional id, so they
  stay Cluster Write.

## Related

- [KAFKA_COMPAT.md](./KAFKA_COMPAT.md) — ACL honesty
- [PHASE85_SPEC.md](./PHASE85_SPEC.md) — User resource v3
