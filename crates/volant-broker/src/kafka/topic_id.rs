//! Shared TopicId / topic-name wire identity for Kafka handlers.
//!
//! Produce, Fetch, OffsetCommit/Fetch, TxnOffsetCommit, and admin paths all need
//! the same deterministic UUID mapping and response echo. Keep that logic here
//! instead of copy-pasting `use_topic_id` triples across encode_* functions.

use bytes::{Buf, BytesMut};
use volant_core::Result;

use crate::broker::Broker;

use super::codec::{
    get_uuid, parse_volant_topic_uuid, put_compact_string, put_string, put_uuid, KAFKA_UUID_ZERO,
};
use super::wire;

/// How a topic is identified on the wire for a given API version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicWireId {
    /// Name-based request/response (STRING or COMPACT_STRING).
    Name(String),
    /// TopicId UUID request/response (always 16 bytes).
    Uuid([u8; 16]),
}

/// Topic after wire read + catalog resolution.
#[derive(Debug, Clone)]
pub struct ResolvedTopic {
    /// Identity to echo on the response (name or UUID as received).
    pub wire: TopicWireId,
    /// Resolved Volant topic name when known; `None` means UnknownTopicId.
    pub name: Option<String>,
}

impl ResolvedTopic {
    /// Name-based topic that always resolves (catalog presence checked later).
    pub fn from_name(name: String) -> Self {
        Self {
            wire: TopicWireId::Name(name.clone()),
            name: Some(name),
        }
    }

    /// Resolve a UUID against the broker catalog.
    pub fn from_uuid(broker: &Broker, uuid: [u8; 16]) -> Self {
        let name = parse_volant_topic_uuid(&uuid).and_then(|id| broker.topic_name_by_id(id));
        Self {
            wire: TopicWireId::Uuid(uuid),
            name,
        }
    }

    /// True when TopicId did not map to a known Volant topic.
    pub fn is_unknown(&self) -> bool {
        self.name.is_none()
    }

    /// Resolved name, or empty string when unknown (for response-side name fields).
    pub fn name_or_empty(&self) -> &str {
        self.name.as_deref().unwrap_or("")
    }
}

/// Read topic name or TopicId from the request and resolve against the catalog.
///
/// * `by_id` — when true, read UUID; when false, read string (compact iff `flex`).
pub fn read_and_resolve(
    broker: &Broker,
    src: &mut impl Buf,
    flex: bool,
    by_id: bool,
) -> Result<ResolvedTopic> {
    if by_id {
        let uuid = get_uuid(src)?;
        Ok(ResolvedTopic::from_uuid(broker, uuid))
    } else {
        let name = wire::read_string(src, flex)?;
        Ok(ResolvedTopic::from_name(name))
    }
}

/// Write topic identity for a response (name string or UUID).
///
/// UUID paths are always written as raw 16 bytes (no string framing).
/// Name paths use compact string when `flex` is true.
pub fn write_wire_id(out: &mut BytesMut, flex: bool, wire_id: &TopicWireId) {
    match wire_id {
        TopicWireId::Uuid(uuid) => put_uuid(out, uuid),
        TopicWireId::Name(name) if flex => put_compact_string(out, name),
        TopicWireId::Name(name) => put_string(out, name),
    }
}

/// Write a name-based topic field (no TopicId), classic or flexible.
pub fn write_name(out: &mut BytesMut, flex: bool, name: &str) {
    if flex {
        put_compact_string(out, name);
    } else {
        put_string(out, name);
    }
}

/// Resolve known-good name into a wire id for list-all responses that emit UUIDs.
pub fn wire_id_for_name(broker: &Broker, name: &str, by_id: bool) -> TopicWireId {
    if by_id {
        // Prefer catalog topic_id when available; fall back to zero UUID.
        use volant_core::TopicName;
        let uuid = broker
            .metadata(Some(&[TopicName::new(name.to_string())]))
            .topics
            .first()
            .map(|t| super::codec::volant_topic_uuid(t.topic_id.0))
            .unwrap_or(KAFKA_UUID_ZERO);
        TopicWireId::Uuid(uuid)
    } else {
        TopicWireId::Name(name.to_string())
    }
}
