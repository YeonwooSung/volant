//! Shared TopicId / topic-name wire identity for Kafka handlers.
//!
//! Produce, Fetch, OffsetCommit/Fetch, TxnOffsetCommit, Metadata, DeleteTopics,
//! and admin paths all need the same deterministic UUID mapping and response
//! echo. Keep that logic here instead of copy-pasting resolve triples.

use bytes::{Buf, BytesMut};
use volant_core::{Result, TopicName};

use crate::broker::Broker;

use super::codec::{
    get_uuid, parse_volant_topic_uuid, put_compact_string, put_string, put_uuid, volant_topic_uuid,
    KAFKA_UUID_ZERO,
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
        let name = name_for_uuid(broker, &uuid);
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

/// Resolve a TopicId UUID to a catalog name (`None` if zero/invalid/unknown).
pub fn name_for_uuid(broker: &Broker, uuid: &[u8; 16]) -> Option<String> {
    parse_volant_topic_uuid(uuid).and_then(|id| broker.topic_name_by_id(id))
}

/// True when `uuid` is the all-zero TopicId.
pub fn is_zero_uuid(uuid: &[u8; 16]) -> bool {
    uuid == &KAFKA_UUID_ZERO
}

/// Numeric catalog id for a known topic name, if present.
pub fn numeric_id_for_name(broker: &Broker, name: &str) -> Option<u32> {
    broker
        .metadata(Some(&[TopicName::new(name.to_string())]))
        .topics
        .first()
        .map(|t| t.topic_id.0)
}

/// Deterministic wire UUID for a Volant topic id.
pub fn uuid_for_numeric_id(id: u32) -> [u8; 16] {
    volant_topic_uuid(id)
}

/// Wire UUID for a topic name (zero UUID when not in the catalog).
pub fn uuid_for_name(broker: &Broker, name: &str) -> [u8; 16] {
    numeric_id_for_name(broker, name)
        .map(volant_topic_uuid)
        .unwrap_or(KAFKA_UUID_ZERO)
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

/// Write a raw TopicId UUID field.
pub fn write_uuid(out: &mut BytesMut, uuid: &[u8; 16]) {
    put_uuid(out, uuid);
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
        TopicWireId::Uuid(uuid_for_name(broker, name))
    } else {
        TopicWireId::Name(name.to_string())
    }
}

/// Result of resolving a Metadata v10+ topic entry (uuid + optional name).
#[derive(Debug, Clone)]
pub struct MetadataTopicRef {
    pub name: Option<String>,
    pub uuid: [u8; 16],
    /// Client asked by id only and the id was unknown.
    pub unknown_id: bool,
}

/// Resolve a Metadata topic entry given already-read uuid + optional name.
///
/// * `allow_id_lookup` — Metadata v12+ permits null name + uuid lookup.
/// * Zero uuid + null name → `None` (skip entry).
pub fn resolve_metadata_entry(
    broker: &Broker,
    uuid: [u8; 16],
    name: Option<String>,
    allow_id_lookup: bool,
) -> Option<MetadataTopicRef> {
    if let Some(n) = name {
        return Some(MetadataTopicRef {
            name: Some(n),
            uuid,
            unknown_id: false,
        });
    }
    if !allow_id_lookup {
        // v10–11: null name not used for lookup.
        return None;
    }
    if is_zero_uuid(&uuid) {
        // Zero id + null name → skip.
        return None;
    }
    match name_for_uuid(broker, &uuid) {
        Some(n) => Some(MetadataTopicRef {
            name: Some(n),
            uuid,
            unknown_id: false,
        }),
        None => Some(MetadataTopicRef {
            name: None,
            uuid,
            unknown_id: true,
        }),
    }
}

/// Result of resolving a DeleteTopics v6 topic entry.
#[derive(Debug, Clone)]
pub struct DeleteTopicRef {
    pub request_name: Option<String>,
    pub uuid: [u8; 16],
    pub resolved_name: Option<String>,
    pub numeric_id: Option<u32>,
    pub unknown_topic_id: bool,
}

/// Resolve DeleteTopics v6 entry (nullable name + uuid).
pub fn resolve_delete_entry(
    broker: &Broker,
    name: Option<String>,
    uuid: [u8; 16],
) -> DeleteTopicRef {
    if let Some(n) = name {
        let id = numeric_id_for_name(broker, &n);
        return DeleteTopicRef {
            request_name: Some(n.clone()),
            uuid,
            resolved_name: Some(n),
            numeric_id: id,
            unknown_topic_id: false,
        };
    }
    if let Some(id) = parse_volant_topic_uuid(&uuid) {
        match broker.topic_name_by_id(id) {
            Some(n) => DeleteTopicRef {
                request_name: None,
                uuid,
                resolved_name: Some(n),
                numeric_id: Some(id),
                unknown_topic_id: false,
            },
            None => DeleteTopicRef {
                request_name: None,
                uuid,
                resolved_name: None,
                numeric_id: None,
                unknown_topic_id: true,
            },
        }
    } else {
        DeleteTopicRef {
            request_name: None,
            uuid,
            resolved_name: None,
            numeric_id: None,
            unknown_topic_id: true,
        }
    }
}

/// Prefer request uuid; fall back to resolved numeric mapping / zero.
pub fn echo_uuid(request_uuid: [u8; 16], numeric_id: Option<u32>) -> [u8; 16] {
    if !is_zero_uuid(&request_uuid) {
        request_uuid
    } else {
        numeric_id.map(volant_topic_uuid).unwrap_or(KAFKA_UUID_ZERO)
    }
}
