# v0.8 — Cross-app EOS fencing via `application_id`

**Status:** ✅ Shipped (MVP vertical slice)  
**Theme:** Exactly-once stream apps that share an **`application_id`** fence
each other even when their per-task `transactional_id`s differ. Sibling
slices (distributed 2PC, homemade Raft, Kafka API keys) are **out of scope**.

## Problem

Phase 151/153 EOS fences only a single `transactional_id` (broker
`InitProducerId` epoch bump). Two `StreamApp`s that share that id already
fence via InitProducerId — that is **not** enough when tasks use **different**
transactional ids under one application. A second process of the same app
must stop the first.

## Goals

1. Optional **`application_id`** on `ProcessingGuarantee::ExactlyOnce`
2. `StreamBuilder::exactly_once(tid)` unchanged (no app fence)
3. `StreamBuilder::exactly_once_app(application_id, transactional_id)` and
   `exactly_once(tid).application_id(...)`
4. Dedicated fence transactional id `{application_id}::__volant_app_fence`
5. Second runtime with the same `application_id` (different `transactional_id`)
   fences the first; first's next EOS step fails with a fenced / invalid-epoch
   error
6. Empty / absent `application_id` ≡ Phase 151/153

## Non-goals

| Deferred | Why |
|----------|-----|
| Kafka Streams `application.server` / task assignment | In-process only |
| Distributed workers | One process per `StreamApp` |
| Multiple fence ids / per-task tokens | One fence id per `application_id` |
| KIP-890 | Use existing InitProducerId + epoch |
| Distributed 2PC (state ↔ broker) | Sibling slice; DurableStore unchanged |
| Kafka API keys / homemade Raft | Explicitly out of scope |
| Silent rename of `exactly_once(tid)` txn id | Must stay the raw `transactional_id` |

## Design

### API

```rust
pub enum ProcessingGuarantee {
    AtLeastOnce,
    ExactlyOnce {
        transactional_id: String,
        application_id: Option<String>, // v0.8
    },
}

StreamBuilder::exactly_once(tid)                      // application_id = None
StreamBuilder::exactly_once_app(app, tid)
StreamBuilder::exactly_once(tid).application_id(app)
StreamApp::start_exactly_once(client, topo, tid)      // no app fence
StreamApp::start_exactly_once_app(client, topo, app, tid)

pub const APP_FENCE_TXN_SUFFIX: &str = "::__volant_app_fence";
pub fn app_fence_transactional_id(application_id: &str) -> String;
```

Per-task transactional id is **always** the caller-supplied
`transactional_id` (not `{application_id}::{transactional_id}`).

### Fence key

When `application_id` is `Some(app)` and non-empty, at `StreamApp` start the
runtime:

1. Connects a process-wide **app-fence** `TransactionalProducer` on
   `{application_id}::__volant_app_fence`
2. Heartbeats it immediately (`BeginTxn` + `EndTxn` abort) so
   `InitProducerId` runs and **claims** the fence epoch before the first EOS
   step

The task producer still uses `transactional_id` only.

### Detection (heartbeat)

Each `step_exactly_once` (including empty polls) heartbeats the fence
producer with `BeginTxn` + abort. The broker checks the stored producer
epoch (`InvalidProducerEpoch` / native **19**).

- Second app `start` claims the same fence id → `InitProducerId` **bumps**
  epoch and fences the first app's fence producer
- First app's next heartbeat `BeginTxn` uses the stale epoch → fail
- Runtime maps that to
  `Error::Protocol("application fenced (application_id=…): …")`
- The in-flight step (if any) that already passed its heartbeat may still
  commit; the **next** step fails

Empty / absent `application_id`: no fence producer, no heartbeat.

### Why not only `transactional_id`?

The txn coordinator fences **one** transactional id. Task ids may differ
across processes of the same app, so they would not fence each other. The
shared `{app}::__volant_app_fence` id is the extra token.

### Why BeginTxn heartbeat (not re-Init each step)?

A second `InitProducerId` on the same id **always succeeds** and returns a
**new** epoch (the caller becomes owner). Detection would require tracking
epoch gaps and would re-claim the fence every step. `BeginTxn` with the
epoch stored at start is the existing broker epoch check and does not bump.

## Honesty / limitations

- Not Kafka Streams `application.server`, static membership, or task
  assignment
- Not distributed workers; one fence id per `application_id`
- Not KIP-890 / Kafka `ProducerFenced` (90) on the native path — Volant
  native epoch fence is `InvalidProducerEpoch` (19)
- Fence is detected at **step start**, not mid-produce
- Heartbeat is an extra BeginTxn/abort pair per EOS step (empty included)
- Does not join DurableStore to the broker txn (Phase 153 unchanged)
- Does not implement distributed 2PC
- Same `application_id` **and** same `transactional_id` still fences via
  both the task id and the app fence (last starter wins)

## Exit criteria

1. [x] `application_id: Option<String>` on `ExactlyOnce`
2. [x] `exactly_once(tid)` signature and behavior unchanged
3. [x] `exactly_once_app` / `.application_id(...)`
4. [x] Fence id `{application_id}::__volant_app_fence` claimed at start
5. [x] Same app id + different tid: second start fences first
6. [x] Different app ids do not fence each other
7. [x] Tests `v08_cross_app_fence`; Phase 151/153 regression
8. [x] This spec + `volant-stream` module honesty note

## Tests

`crates/volant-stream/tests/v08_cross_app_fence.rs`

- Unit: fence id formatting; builder `exactly_once` / `exactly_once_app` /
  chain / empty app id
- Live: `exactly_once(tid)` produce + offset commit (no app id)
- Live: same `application_id`, different tid → A fenced, B completes a step
- Live: different `application_id` → both complete steps

Regression:

```
cargo test -p volant-stream --test v08_cross_app_fence -- --test-threads=1
cargo test -p volant-stream --test phase151_exactly_once -- --test-threads=1
cargo test -p volant-stream --test phase153_eos_durable_atomic -- --test-threads=1
```

## Related

- Phase 10 / 18 InitProducerId + epoch fencing
- Phase 151 EOS MVP · Phase 153 durable checkpoint
- `TransactionalProducer`, `Broker::init_producer_id_with_txn`
