//! Lightweight stream processing operators for Volant.
//!
//! Phase 4 delivers map/filter/aggregate and exactly-once hooks. This crate
//! currently exposes the operator trait surface only.

#![deny(missing_docs)]

pub mod operator;
pub mod pipeline;

pub use operator::Operator;
pub use pipeline::Pipeline;
