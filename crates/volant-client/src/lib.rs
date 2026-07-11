//! Async client SDK for producing and consuming Volant topics.
//!
//! Phase 2 provides a networked [`Client`] over TCP using the Volant frame protocol.
//! Phase 3 adds consumer groups via [`GroupConsumer`].
//! Phase 8 adds leader redirect and optional TLS (`tls` feature).

#![deny(missing_docs)]

pub mod client;
pub mod config;
mod conn;
pub mod consumer;
pub mod group;
pub mod producer;

pub use client::{
    produce_value, Client, FetchResult, HeartbeatResult, JoinGroupResult, Metadata, ProduceResult,
};
pub use config::ClientConfig;
pub use consumer::Consumer;
pub use group::{FetchedRecord, GroupConsumer};
pub use producer::Producer;
