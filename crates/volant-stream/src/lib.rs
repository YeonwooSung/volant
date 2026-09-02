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
//! - EOS changelog (v0.9, opt-in): [`StreamBuilder::changelog_topic`] produces
//!   staged deltas in the same txn; [`DurableStore::open_with_changelog`] /
//!   [`replay_changelog`] rebuild state from the broker log
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
//! **changelog produce (if configured)** → `add_offsets(group, positions)` →
//! `txn.commit` → `commit_checkpoint`. Atomic produce + offset commit via
//! Volant write-through transactions + soft markers. Without
//! [`StreamBuilder::changelog_topic`], durable state remains Phase 153
//! process-local staging. With changelog, mutations ride the same txn as sink
//! + offsets; another process can rebuild via replay. Fence via
//! `transactional_id`. Empty polls abort the checkpoint and skip the txn.
//!
//! Durable window buckets and reduce aggregates survive restart in **one
//! process** when configured. Changelog recovery is last-write-wins per key
//! (not multi-worker task assignment or broker-side stream state).
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
pub use runtime::{process_pipeline, ProcessingGuarantee, StreamApp};
pub use sink::TopicSink;
pub use source::{record_from_value, SourceConfig, TopicSource};
pub use state::{
    changelog_message, ensure_changelog_topic, produce_changelog_in_txn, replay_changelog,
    DurableStore, KeyValueStore, MemoryStore, StreamStateError, CHANGELOG_HEADER,
    CHANGELOG_VERSION, DEFAULT_CHANGELOG_TOPIC,
};
pub use topology::{StreamBuilder, Topology};
pub use window::TumblingWindow;
