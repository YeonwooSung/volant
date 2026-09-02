# v0.9 — Distributed EOS 2PC MVP (changelog-backed state)

**Status:** Shipped (bounded MVP)  
**Theme:** State mutations travel in the same Volant transaction as sink
produce + group offsets, and another process can rebuild state from a broker
changelog topic.  
**Crate:** 0.2.0 (unchanged)

Phase 153 is **process-local** staging: crash after local `commit_checkpoint`
vs broker is a residual; state is **not** in the broker log. v0.9 closes that
gap for an opt-in changelog without multi-worker stream runtime, homemade
Raft, or new Kafka API keys.

## Goals

1. **Changelog topic** — each staged put/delete is one record inside the EOS txn
2. **EOS order** — changelog produce sits between sink produce and offset commit
3. **Staged deltas** — `DurableStore::staged_changelog` (no redb leak)
4. **Replay** — `DurableStore::open_with_changelog` / `replay_changelog`
5. **Opt-in builder** — `StreamBuilder::exactly_once(...).changelog_topic(...)`
6. **Honesty** — one process, regular topic, last-write-wins

## Non-goals

| Deferred | Why |
|----------|-----|
| Multi-worker task assignment | Still one-process topology |
| Broker-side stream state / KIP-890 `__transaction_state` | Existing write-through txn only |
| Cross-app fencing / `application_id` | Sibling slice |
| New Kafka API keys / inter-broker opcodes | Not required |
| ALO immediate-put change | ALO stays Phase 149 |
| Multi-partition restore / standby task | Single-partition last-write-wins |
| Per-store changelog namespaces | One topic per topology in this MVP |

## Defaults

| Knob | Default | Notes |
|------|---------|-------|
| Changelog | **off** | `exactly_once` + `DurableStore::open` remains Phase 153 |
| Topic name | `__volant_changelog` | Via `StreamBuilder::changelog()`. Prefer `{topology_or_store}__changelog` when sharing a cluster. |
| Partitions | 1 | Auto-created on first use if missing; RF = cluster default |
| Format version | `1` | Header `volant-changelog` = `1` (ASCII) |

## Record format (version 1)

| Field | Meaning |
|-------|---------|
| **key** | Store key bytes |
| **value** | Store value bytes. **Empty payload = delete** (tombstone). Empty values cannot be distinguished from deletes. |
| **header** | `volant-changelog` = `1` |

Replay applies every keyed record on the topic (dedicated changelog). Last
write per key wins. Native fetch is **committed-only**: open and aborted
transactional ranges are hidden.

## EOS step order

```
begin_checkpoint
process / punctuate
(empty → abort_checkpoint, no txn)
txn.begin
sink produce                  // existing
changelog produce of staged   // new; skip if no topic or no deltas
add offsets / commit offsets  // existing
EndTxn
  ok  → commit_checkpoint     // local redb apply, existing
  err → abort_checkpoint
```

Changelog produce **must** be in the same txn as sink + offsets.

## API

```rust
// Builder (opt-in)
StreamBuilder::exactly_once("txn-id")
    .changelog()                          // default __volant_changelog
    .changelog_topic("myapp__changelog")  // explicit name

// Store
DurableStore::staged_changelog(&self) -> Vec<(Bytes, Option<Bytes>)>
DurableStore::open_with_changelog(path, client, topic).await
replay_changelog(store, client, topic).await
ensure_changelog_topic(client, topic).await

// Pipeline / operators forward staged_changelog + apply_changelog
```

`KeyValueStore::staged_changelog` defaults to empty. `MemoryStore` stays
ephemeral. ALO path never stages and never produces changelog.

## Honesty / leftovers

- **One process topology.** Not multi-worker assignment or standby tasks.
- **Not** broker-side stream state machine / KIP-890 `__transaction_state`.
- Changelog is a **regular topic**; retention/compaction apply. Old state can
  expire if the topic is truncated.
- Local redb is a **cache**. The broker log is the recoverability story.
- Replay is **best-effort last-write-wins** per key from earliest (or whatever
  is still retained). Not a multi-partition restore.
- Shared changelog is applied to **every** operator store (not namespaced).
- If EndTxn succeeds and `commit_checkpoint` then fails, offsets + changelog
  are committed while local redb may lag (replay on next open repairs this).
- Empty store values cannot be changelog'd distinctly from deletes.
- No cross-app fencing (`application_id` ignored).

## Tests

`crates/volant-stream/tests/v09_eos_changelog.rs`

1. Regression: EOS + DurableStore, no changelog → local commit after EndTxn
2. Happy path: reduce/count → changelog put + sink records after commit
3. Abort / empty step → no changelog records
4. Replay: fresh DurableStore dir + changelog → keys match
5. Txn fail / abort → staged local aborted; changelog not visible (native
   committed-only / post-abort fetch)

Regression: `phase153_eos_durable_atomic`, `phase151_exactly_once`,
`phase149_durable_state`.
