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
//! v0.91 redirects [`Client::add_broker`] / [`Client::remove_broker`]
//! on error **14** via the same `admin_round_trip` /
//! `redirect_to_controller` / `max_redirects` budget. Overlay is still
//! SoT; this is not Kafka broker catalog.
//! v0.92 retries [`Client::describe_group`] / [`Client::list_groups`]
//! on that same transient set (default 0). Error 2 (no live members),
//! 9 / 10 / 11, 13 / 14, and protocol are not retried. Range
//! assignor inherits via `describe_group`.
//! v0.94 redirects topic `describe_configs` / `alter_configs` on that
//! same error **14** budget. Topic-only (not Kafka BROKER configs).
//! v0.96 retries [`Client::metadata`] / [`Client::list_members`] on
//! that same transient set (default 0). Native Metadata has no
//! top-level error_code; failures arrive as [`volant_protocol::Response::Error`]
//! or transport. Error 2 / 9 / 10 / 11 / 13 / 14 and protocol are
//! not retried. Admin-14 and leader-13 redirect inherit this retry.
//! v0.98 redirects [`Client::delete_offsets`] (and OffsetCommit /
//! OffsetFetch, which share `offset_admin_round_trip`) on error **14**
//! via `redirect_to_controller` / `max_redirects`. Transient 6/7/15/16
//! stay on `max_retries`. `max_redirects=0` does not redirect.
//! v0.100 retries [`Client::begin_transaction`] / EndTxn (commit /
//! abort inherit) on that same transient set (default 0). InvalidTxnState
//! (22), fence / epoch / abortable, 13 / 14 / 9 / 10 / 11 / 2, and
//! protocol are not retried. [`TransactionalProducer`] inherits.
//! v0.102 retries InitProducerId (`ensure_producer_id`) on that same
//! transient set (default 0). Error 13 / 14 / 9 / 10 / 11 / 2,
//! protocol, and UnknownProducerId (21) on Init itself are not retried.
//! Already initialized clients skip Init.
//! v0.104 retries controller-gated admin (`create_topic` / ACLs /
//! SCRAM-admin / Add/RemoveBroker / Describe/AlterConfigs, which share
//! `admin_round_trip`) on that same transient set (default 0). Error
//! **14** stays on `max_redirects` (independent counter). 13 / 9 / 10 /
//! 11 / 2 / 21 / InvalidTxnState (22) and protocol are not retried.
//! v0.107 retries shared-token Auth (`authenticate`) on that same
//! transient set (default 0). Error 17 / 18 (auth failed / required)
//! is not retried. 13 / 14 / 9 / 10 / 11 / 2 / 21 / 22, Protocol, and
//! InvalidArgument are not retried. Already-connected clients skip
//! Auth. Each `connect` / `reconnect` has its own retry budget.
//! v0.109 retries the SCRAM-SHA-256 handshake (`authenticate_scram`)
//! on that same transient set (default 0). First+final is one unit;
//! a transient on either step restarts from ScramFirst with a new
//! nonce. Error 17 / 18 / 13 / 14 / 9 / 10 / 11 / 2 / 21 / 22,
//! protocol (including server signature mismatch), and InvalidArgument
//! are not retried.
//! v0.111 retries [`Client::delete_records`] /
//! `delete_records_with_wait_flag` on that same transient set
//! (default 0). Error **13** stays on `max_redirects` via
//! `redirect_to_leader` (independent counter). 14 / 9 / 10 / 11 / 2 /
//! 17 / 18 / 21 / 22 and protocol are not retried. `wait_majority`
//! trailer is unchanged.
//! v0.113 redirects [`Client::list_offsets`] on error **13**
//! (`NotLeaderForPartition`) via `redirect_to_leader` /
//! `max_redirects`. Transient 6/7/15/16 stay on `max_retries`.
//! `max_redirects=0` does not redirect. 14 / 2 / 9 / 10 / 11 / 17 /
//! 18 / 21 / 22 and protocol are not redirected.
//! v0.114 adds [`Client::metadata_topics`] so a Rust caller can send
//! a native Metadata topic filter. [`Client::metadata`] stays
//! empty-list (all topics). Retry (v0.96) is inherited via
//! `metadata_list_members_round_trip`. Empty remains “all”. Not
//! Kafka `allow_auto_topic_creation` / topic ids.
//! v0.120 redirects [`Client::list_members`] on error **14**
//! (`NotController`) via `redirect_to_controller` /
//! `max_redirects`. Transient 6/7/15/16 stay on `max_retries`
//! (v0.96 helper). `max_redirects=0` does not redirect. 13 / 2 /
//! 9 / 10 / 11 / 17 / 18 / 21 / 22 and protocol are not redirected.
//! [`Client::metadata`] is unchanged (not controller-gated).
//! v0.125 redirects [`Client::describe_group`] / [`Client::list_groups`]
//! on error **14** (`NotController`) via `redirect_to_controller` /
//! `max_redirects`. Transient 6/7/15/16 stay on `max_retries`
//! (v0.92 helper). `max_redirects=0` does not redirect. 13 / 2 /
//! 9 / 10 / 11 / 17 / 18 / 21 / 22 and protocol are not redirected.
//! Range assignor inherits via `describe_group`.
//! v0.135 redirects [`Client::heartbeat`] on error **14**
//! (`NotController`) via `redirect_to_controller` /
//! `max_redirects`. Transient 6/7/15/16 stay on `max_retries`
//! (v0.80). `max_redirects=0` does not redirect. 13 / 2 /
//! 9 / 10 / 11 / 17 / 18 / 21 / 22 and protocol are not redirected.
//! Rebalance 9/10/11 is still not retried. Typed non-zero codes
//! still return [`HeartbeatResult`]. [`GroupConsumer`] inherits.
//! v0.137 redirects [`Client::leave_group`] on error **14**
//! (`NotController`) via `redirect_to_controller` /
//! `max_redirects`. Transient 6/7/15/16 stay on `max_retries`
//! (v0.87). `max_redirects=0` does not redirect. Error 10
//! (`UnknownMemberId`) stays success. 13 / 2 / 9 / 11 / 17 / 18 /
//! 21 / 22 and protocol are not redirected. Rebalance 9/11 is
//! still not retried. [`GroupConsumer::leave`] inherits.
//! v0.144 adds [`ClientConfig::fetch_max_messages`] /
//! [`ClientConfig::fetch_max_bytes`] /
//! [`ClientConfig::fetch_max_wait_ms`] (defaults **128 / 4 MiB / 0**)
//! and [`Client::fetch_default`]. [`Client::fetch`] still requires
//! `max_messages` / `max_wait_ms` and uses
//! [`ClientConfig::fetch_max_bytes`] (v0.149).
//! GroupConsumer poll knobs stay historical (v0.76; 100 / 4 MiB).
//! v0.153 adds one-entry [`Client::commit_offset`] /
//! [`Client::commit_offset_meta`] / [`Client::commit_offset_member`] /
//! [`Client::commit_offset_member_meta`] wrapping [`Client::commit_offsets`].
//! Admin path is empty member, generation 0. Error 14 / transient retry inherit.

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
