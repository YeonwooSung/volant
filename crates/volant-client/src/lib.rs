//! Async client SDK for producing and consuming Volant topics.
//!
//! Phase 2 provides a networked [`Client`] over TCP using the Volant frame protocol.
//! Phase 3 adds consumer groups via [`GroupConsumer`].
//! Phase 8 adds leader redirect and optional TLS (`tls` feature).
//! v0.44 adds a background heartbeat task on [`GroupConsumer`] so a silent
//! consumer does not expire (`heartbeat_interval`; opt out with
//! [`GroupConsumer::join_with_heartbeat`]).
//! v0.60 adds opt-in auto-commit after a successful [`GroupConsumer::poll`]
//! that returned records ([`GroupConsumer::join_with_auto_commit`]; default
//! off). Not Kafka `enable.auto.commit`.
//! v0.67 adds opt-in [`GroupConsumer::join_with_auto_offset_reset`]
//! (`earliest` / `latest` / `none`) when OffsetFetch is missing or
//! `OFFSET_UNKNOWN`. Default remains `earliest` (native ListOffsets
//! earliest; v0.71). Not Kafka `auto.offset.reset`.
//! v0.73 adds opt-in [`GroupConsumer::join_with_assignor`] (`"range"`)
//! which replaces the fetch set from DescribeGroup members via
//! `range_assign_multi`. Default remains broker JoinGroup assignment.
//! Still no SyncGroup.

#![deny(missing_docs)]

mod assignor;
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
