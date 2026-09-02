//! Async client SDK for producing and consuming Volant topics.
//!
//! Phase 2 provides a networked [`Client`] over TCP using the Volant frame protocol.
//! Phase 3 adds consumer groups via [`GroupConsumer`].
//! Phase 8 adds leader redirect and optional TLS (`tls` feature).
//! v0.44 adds a background heartbeat task on [`GroupConsumer`] so a silent
//! consumer does not expire (`heartbeat_interval`; opt out with
//! [`GroupConsumer::join_with_heartbeat`]).

#![deny(missing_docs)]

pub mod client;
pub mod config;
mod conn;
pub mod consumer;
pub mod group;
pub mod producer;
mod scram;
pub mod txn;

pub use client::{
    produce_value, Client, DeleteOffsetsResult, DeleteRecordsResult, DescribeConfigsResult,
    DescribeGroupResult, FetchResult, HeartbeatResult, JoinGroupResult, ListOffsetsResult,
    MembershipList, Metadata, PartitionOffsets, ProduceResult,
};
pub use config::ClientConfig;
pub use consumer::Consumer;
pub use group::{heartbeat_interval, FetchedRecord, GroupConsumer};
pub use producer::Producer;
pub use txn::TransactionalProducer;
