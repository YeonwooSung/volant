# v0.213 — IsrUpdate skips homemade 154 when openraft is on

**Status:** Shipped  
**Crate:** 0.2.0 (unchanged)  
**Theme:** Close the production leak where controller `IsrUpdate` (94)
always fanned out homemade 154 AppendEntries (**98/99**) whenever
`VOLANT_METADATA_RAFT` was on — even if `VOLANT_OPENRAFT_METADATA` was
also on. Match `maybe_fanout_assignment_consensus`: openraft first,
then 154 only if openraft is off and 154 is on, else Phase 150 notes.

This is residual **v0.213**. It is **not** Phase 155. It does **not**
delete `metadata_raft.rs`, add Kafka API keys, add native opcodes, or
change overlay membership.

## Goals

1. After a successful controller `apply_leader_isr_update`, fan out
   with the same preference as CreateTopic / DeleteTopic /
   CreatePartitions (`maybe_fanout_assignment_consensus`).
2. When openraft is on, IsrUpdate must **not** send opcode **98**
   (homemade `MetadataRaftAppend`). It uses openraft
   `client_write(SetAssignment)` (opcodes **108/109**).
3. When openraft is off and `VOLANT_METADATA_RAFT` is on, opcode **98**
   is still used (154 path stays).
4. When both raft flags are off, Phase 150 notes (**96/97**) stay the
   best-effort path.
5. IsrUpdate remains best-effort: consensus miss does not fail the
   94/95 response.

## Non-goals

| Deferred | Why |
|----------|-----|
| Delete `cluster/metadata_raft.rs` | Code stays; do not grow homemade Raft |
| Inbound 98 handling | Sibling v0.214 |
| Overlay membership / AddBroker | Frozen; keep this hunk local |
| New opcodes / Kafka API keys | Frozen; `SUPPORTED_APIS` stays 38 |
| Flip openraft / 154 defaults | Phase 155 / v0.2 freeze |
| Crate 0.3.0 | After 155 ships, not during |

## Preference

Same order as `net/fanout.rs` `maybe_fanout_assignment_consensus`:

```
IsrUpdate error_code == 0
    │
    ├─ VOLANT_OPENRAFT_METADATA on
    │     → client_write SetAssignment (108/109); skip 154 and 150
    │
    ├─ openraft off + VOLANT_METADATA_RAFT on
    │     → fanout_metadata_raft_append (98/99)
    │
    └─ else if VOLANT_ASSIGNMENT_CONSENSUS on
          → AssignmentConsensusNote (96/97)
```

CreateTopic already took this path via `complete_assignment_mutation`.
The leak was IsrUpdate calling `fanout_metadata_raft_append` directly.

## Tests

`crates/volant-broker/tests/v213_isr_update_skips_154.rs`:

1. Openraft **off** + 154 **on** — IsrUpdate sends opcode **98**;
   homemade `last_index` advances.
2. Openraft **on** + 154 **on** — IsrUpdate does **not** append
   homemade 154; a follower installs the reported ISR via openraft
   apply (108).

```bash
cargo test -p volant-broker --lib -- --test-threads=1
cargo test -p volant-broker --test v213_isr_update_skips_154 -- --test-threads=1
```

## Honesty leftovers

- Homemade 154 still has no election / InstallSnapshot.
- IsrUpdate is still best-effort (94/95 success does not wait on
  consensus unless the shared helper is in a wait mode).
- Phase 150 notes remain when both raft flags are off.
- Kafka stays 38 keys.

## Merge notes

v0.214 also edits `dispatch.rs` (inbound 98). Keep this hunk local to
the IsrUpdate **outbound** fanout. Keep both.

## Related

- [V16_SPEC.md](./V16_SPEC.md) — openraft SetAssignment apply
- [V40_SPEC.md](./V40_SPEC.md) — homemade 154 wait-commit
- [PHASE142_SPEC.md](./PHASE142_SPEC.md) — IsrUpdate 94/95
- [PHASE154_SPEC.md](./PHASE154_SPEC.md) — homemade metadata Raft
- [PHASE150_SPEC.md](./PHASE150_SPEC.md) — assignment notes 96/97
