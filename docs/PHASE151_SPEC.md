# Phase 151 — Stream exactly-once (EOS) MVP

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Couple sink produce + consumer group offset commit in one Volant
transaction so crash between produce and commit cannot redeliver (duplicate
sink outputs). Sibling Phase 152 covers consensus depth — **out of scope**
here (no assignment consensus / Metadata gating changes).

## Problem

`StreamApp::step` (Phases 4–150) does:

1. poll source  
2. process + punctuate  
3. sink produce (`acks=1`)  
4. commit group offsets  

Crash between **3** and **4** → at-least-once redelivery → **duplicate outputs**.
The client already has `TransactionalProducer` + deferred group offsets
(`add_offsets` / `commit_transaction`).

## Goals

1. **Expose consumer positions** for txn offset commit  
   - `GroupConsumer::group_id` / `positions` (already public)  
   - `TopicSource::group_id` / `positions` / `pending_offsets`  
2. **Transactional sink path** — `TopicSink::send_all_in_txn`  
3. **EOS `StreamApp` path** — `ProcessingGuarantee::ExactlyOnce`  
4. **Builder** — `StreamBuilder::exactly_once(transactional_id)`  
5. **Tests** — ALO regression + EOS e2e + empty-step no-op  
6. **Docs** — honesty vs full Kafka Streams EOS  

## Non-goals

| Deferred | Why |
|----------|-----|
| Distributed stream workers | Topology remains in-process |
| Full 2PC with durable stream state in same txn | State store not joined to broker txn |
| Full Kafka Streams EOS / KIP-890 depth | Volant write-through + soft markers only |
| Assignment consensus / Metadata gating | Sibling Phase 152 |
| openraft | Out of scope |

## Design

### Processing guarantee

```rust
pub enum ProcessingGuarantee {
    AtLeastOnce, // default — produce then OffsetCommit
    ExactlyOnce { transactional_id: String },
}
```

- `StreamBuilder::exactly_once(id)` stores on `Topology::processing_guarantee`
- `StreamApp::start` reads topology flag
- `StreamApp::start_with_guarantee` / `start_exactly_once` override explicitly

### EOS step algorithm

1. `poll()` input records  
2. process + punctuate → outputs  
3. If **no input** and **no outputs**: return `Ok` (**skip txn**)  
4. `txn.begin()`  
5. `sink.send_all_in_txn` (transactional produce)  
6. `txn.add_offsets(group_id, pending_offsets)` — next offsets after poll  
7. `txn.commit()` — atomic produce + deferred group offsets  
8. On produce/commit failure: `txn.abort()` and return `Err`  

Empty poll (no records, no position advance, no punctuate emit): **no transaction**.

### Durable store

EOS pairs best with [`DurableStore`] (Phase 149) so aggregates survive restart.
**Phase 153** stages durable puts during an EOS step and commits the checkpoint
only after successful EndTxn (process-local; not distributed 2PC with the broker).
ALO path still uses immediate put.

## API surface

```rust
// volant-client (pre-existing)
GroupConsumer::group_id(&self) -> &str
GroupConsumer::positions(&self) -> &HashMap<(String, u32), u64>
GroupConsumer::commit(&self) // ALO path unchanged

// volant-stream
TopicSource::group_id(&self) -> &str
TopicSource::positions(&self) -> &HashMap<(String, u32), u64>
TopicSource::pending_offsets(&self) -> Vec<(String, u32, u64)>

TopicSink::send_all(...)                 // ALO
TopicSink::send_all_in_txn(txn, records) // EOS

enum ProcessingGuarantee { AtLeastOnce, ExactlyOnce { transactional_id } }

StreamBuilder::exactly_once(transactional_id)
StreamBuilder::processing_guarantee(g)
Topology::processing_guarantee

StreamApp::start(client, topology)                 // uses topology guarantee
StreamApp::start_with_guarantee(client, g, topology)
StreamApp::start_exactly_once(client, topology, id)
StreamApp::processing_guarantee(&self) -> &ProcessingGuarantee
StreamApp::step() // dispatches ALO vs EOS
```

## Honesty / limitations

- Depends on Volant **write-through transactions + soft markers** (Phases 18+),
  not full Kafka Streams exactly-once semantics  
- Fence via **`transactional_id`** (InitProducerId epoch bump)  
- **READ_COMMITTED** consumers see committed sink data (LSO gating); default
  fetch after commit is visible in single-node tests  
- Durable stream state is **not** in the same atomic txn as produce+offsets  
- In-process only; no distributed task assignment  
- Empty steps skip txn (no empty begin/commit chatter)  
- Soft-marker / abort-filter edge cases inherit broker txn honesty  

## Exit criteria

1. [x] `TopicSource` exposes group id + pending offsets  
2. [x] `send_all_in_txn` + EOS step (begin / produce / add_offsets / commit)  
3. [x] `StreamBuilder::exactly_once` + `ProcessingGuarantee`  
4. [x] ALO path regression green  
5. [x] EOS e2e: sink counts + group offsets committed  
6. [x] Empty EOS step no-ops without error  
7. [x] Docs (this file, TODO, ROADMAP, features, README, PHASE_HISTORY, INDEX)

## Tests

- `crates/volant-stream/tests/phase151_exactly_once.rs`
  - builder flag unit tests  
  - ALO live regression  
  - EOS live word-count + offset commit  
  - empty step no-op  
  - `start_exactly_once` API  
- Regression: `cargo test -p volant-stream` (incl. `e2e_word_count`)

## Related

- Phase 4 streams · Phase 18 transactions · Phase 149 durable state  
- Sibling: Phase 152 assignment consensus depth (do not change here)
