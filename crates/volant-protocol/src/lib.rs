//! Wire protocol codec and RPC framing for Volant.
//!
//! Binary framing is designed for zero-copy decode paths where possible:
//! length-prefixed frames, CRC-protected headers, and batch-oriented payloads.

#![deny(missing_docs)]

pub mod codec;
pub mod frame;
pub mod payload;
pub mod request;
pub mod response;

pub use frame::{Frame, FrameHeader, FRAME_MAGIC, PROTOCOL_VERSION};
pub use payload::{
    decode_request, decode_response, encode_request, encode_response, pack_request, pack_response,
    MAX_PAYLOAD,
};
pub use request::{
    metadata_raft_cmd, AclBinding, MembershipBroker, MetadataRaftLogEntry, OffsetCommitEntry,
    OffsetEntry, ProduceMessage, Request, RequestOpcode, TxnOffsetCommit,
};
pub use response::{
    Assignment, BrokerInfo, ClusterPartitionState, ClusterTopicState, ErrorCode, FetchRecord,
    GroupListing, GroupMemberInfo, GroupState, OffsetFetchEntry, OffsetListing, PartitionInfo,
    Response, ResponseOpcode, TopicInfo, TxnProduceResult,
};
