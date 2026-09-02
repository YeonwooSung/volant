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
//! v0.76 adds opt-in [`GroupConsumer::join_with_fetch_knobs`] so
//! `poll` can set Fetch `max_messages` / `max_bytes` (default **100 /
//! 4 MiB**; `0` clamps to those). [`Client::fetch_opts`] exposes
//! `max_bytes`; [`Client::fetch`] still uses 4 MiB.
//! v0.79 redirects controller-gated admin (`create_topic` / `delete_topic` /
//! `create_partitions` / `reassign_partitions` / `create_acls` /
//! `delete_acls`) on error **14** (`NotController`) using a
//! `controller_id=N` message hint or the first other advertised broker.
//! v0.77 adds a Metadata `controller_id` trailer (`0` = unknown).
//! v0.80 retries [`Client::heartbeat`] on the same transient set as
//! produce ([`ClientConfig::max_retries`], default 0). Rebalance 9/10/11
//! is not retried. GroupConsumer poll / background heartbeat inherit.
//! v0.83 retries [`Client::commit_offsets`] / [`Client::fetch_offsets`] /
//! [`Client::delete_offsets`] on that same set. Error 13 / 14 / 9 / 10 /
//! 11 / 2 is not retried. [`GroupConsumer::commit`] inherits via
//! `commit_offsets`.
//! v0.84 retries [`Client::list_offsets`] on that same transient set
//! (default 0). Error 2 (NotFound) is not retried. GroupConsumer
//! earliest/latest reset inherit.
//! v0.87 retries [`Client::leave_group`] on that same transient set
//! (default 0). Error 10 (`UnknownMemberId`) is success (already
//! left). 9 / 11 / 13 / 14 / 2 and protocol are not retried.
//! [`GroupConsumer::leave`] inherits.
//! v0.88 redirects SCRAM-admin (`create_scram_user` /
//! `delete_scram_user` / `list_scram_users`) and `list_acls` on error
//! **14** using the same `redirect_to_controller` / `max_redirects`
//! budget as v0.79.
//! v0.94 redirects topic `describe_configs` / `alter_configs` on that
//! same error **14** budget. Topic-only (not Kafka BROKER configs).

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
