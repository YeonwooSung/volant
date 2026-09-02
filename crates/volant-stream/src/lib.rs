//! Lightweight stream processing operators for Volant.
//!
//! Phase 4 provides Kafka Streams–style operators that run **in-process**:
//!
//! - Stateless: [`map`](ops::map), [`filter`](ops::filter), [`flat_map`](ops::flat_map), [`foreach`](ops::foreach)
//! - Stateful: [`reduce`](ops::reduce) / [`count_reduce`](ops::count_reduce), [`TumblingWindow`](window::TumblingWindow)
//! - Durable state (Phase 149): [`DurableStore`](state::DurableStore), [`count_reduce_durable`](ops::count_reduce_durable)
//! - Durable windows: [`TumblingWindow::durable`] persists open buckets via
//!   [`DurableStore`] so one app survives process restart; default
//!   [`TumblingWindow::new`] stays in-memory (ALO)
//! - Topology: [`StreamBuilder`](topology::StreamBuilder) → [`StreamApp`](runtime::StreamApp)
//! - Exactly-once MVP (Phase 151): [`ProcessingGuarantee::ExactlyOnce`],
//!   [`StreamBuilder::exactly_once`]
//! - EOS + durable checkpoint (Phase 153): stage [`DurableStore`] puts until
//!   EndTxn succeeds; abort on empty step / txn fail
//! - Cross-app EOS fence (v0.8): optional `application_id` claims
//!   `{application_id}::__volant_app_fence` so a second runtime with the
//!   same app id (even a different `transactional_id`) fences the first
//!
//! # At-least-once (default)
//!
//! The runtime commits consumer offsets **after** a successful sink produce.
//! A crash between produce and commit can cause duplicate outputs.
//! Durable puts remain immediate (no checkpoint).
//!
//! # Exactly-once (Phase 151 / 153)
//!
//! Opt in with [`StreamBuilder::exactly_once`] or
//! [`StreamApp::start_exactly_once`]. Each non-empty step:
//! `begin_checkpoint` → process → `txn.begin` → transactional sink produce →
//! `add_offsets(group, positions)` → `txn.commit` → `commit_checkpoint`.
//! Atomic produce + offset commit via Volant write-through transactions + soft
//! markers. Durable state is process-local staging (not distributed 2PC with
//! the broker). Fence via `transactional_id`. Empty polls abort the checkpoint
//! and skip the txn.
//!
//! Optional **cross-app** fencing (v0.8): [`StreamBuilder::exactly_once_app`]
//! or [`StreamBuilder::application_id`] stores `application_id`. At start the
//! runtime `InitProducerId`s `{application_id}::__volant_app_fence` and
//! heartbeats that id (BeginTxn + abort) every EOS step. A second process
//! with the same `application_id` bumps the fence epoch; the first app's next
//! step fails with a fenced / invalid-epoch error. This is **not** Kafka
//! Streams `application.server`, distributed workers, or KIP-890 — one fence
//! id per application, in-process only. Empty / absent `application_id` is
//! identical to Phase 151/153.
//!
//! Durable window buckets and reduce aggregates survive restart in **one
//! process** when configured. This is not cluster EOS or distributed 2PC.
//!
//! # Offline processing
//!
//! For tests, use [`Pipeline`](pipeline::Pipeline) + [`process_pipeline`](runtime::process_pipeline)
//! without a broker.

#![deny(missing_docs)]

pub mod operator;
pub mod ops;
pub mod pipeline;
pub mod runtime;
pub mod sink;
pub mod source;
pub mod state;
pub mod topology;
pub mod window;

pub use operator::Operator;
pub use ops::{
    count_reduce, count_reduce_durable, count_reduce_with_store, filter, flat_map, foreach, map,
    reduce, reduce_with_store, Reduce,
};
pub use pipeline::Pipeline;
pub use runtime::{
    app_fence_transactional_id, process_pipeline, ProcessingGuarantee, StreamApp,
    APP_FENCE_TXN_SUFFIX,
};
pub use sink::TopicSink;
pub use source::{record_from_value, SourceConfig, TopicSource};
pub use state::{DurableStore, KeyValueStore, MemoryStore, StreamStateError};
pub use topology::{StreamBuilder, Topology};
pub use window::TumblingWindow;
