//! Async client SDK for producing and consuming Volant topics.
//!
//! Networked transport is Phase 2; the in-process client is available for
//! embedded / testing use today.

#![deny(missing_docs)]

pub mod config;
pub mod consumer;
pub mod producer;

pub use config::ClientConfig;
pub use consumer::Consumer;
pub use producer::Producer;
