//! Async client SDK for producing and consuming Volant topics.
//!
//! Phase 2 provides a networked [`Client`] over TCP using the Volant frame protocol.

#![deny(missing_docs)]

pub mod client;
pub mod config;
pub mod consumer;
pub mod producer;

pub use client::{Client, FetchResult, Metadata, ProduceResult};
pub use config::ClientConfig;
pub use consumer::Consumer;
pub use producer::Producer;
