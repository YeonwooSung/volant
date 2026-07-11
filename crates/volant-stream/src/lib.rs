//! Lightweight stream processing operators for Volant.
//!
//! Phase 4 provides Kafka Streams–style operators that run **in-process**:
//!
//! - Stateless: [`map`](ops::map), [`filter`](ops::filter), [`flat_map`](ops::flat_map), [`foreach`](ops::foreach)
//! - Stateful: [`reduce`](ops::reduce) / [`count_reduce`](ops::count_reduce), [`TumblingWindow`](window::TumblingWindow)
//! - Topology: [`StreamBuilder`](topology::StreamBuilder) → [`StreamApp`](runtime::StreamApp)
//!
//! # At-least-once
//!
//! The runtime commits consumer offsets **after** a successful sink produce.
//! A crash between produce and commit can cause duplicate outputs.
//! Exactly-once / transactions are a stretch goal (not implemented).
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
pub use ops::{count_reduce, filter, flat_map, foreach, map, reduce, Reduce};
pub use pipeline::Pipeline;
pub use runtime::{process_pipeline, StreamApp};
pub use sink::TopicSink;
pub use source::{record_from_value, SourceConfig, TopicSource};
pub use state::{KeyValueStore, MemoryStore};
pub use topology::{StreamBuilder, Topology};
pub use window::TumblingWindow;
