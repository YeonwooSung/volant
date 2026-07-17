//! Kafka wire handlers: ApiVersions, Metadata, FindCoordinator, DescribeCluster.

use bytes::{Buf, BufMut, BytesMut};
use volant_core::TopicName;

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::Broker;

use super::acl_api::volant_op_to_kafka;
use super::codec::{
    get_compact_array_len, get_compact_nullable_string, get_compact_string,
    get_string, get_uuid, put_compact_array_len, put_compact_nullable_string, put_compact_string,
    put_empty_tag_buffer, put_nullable_string, put_string, put_uuid, skip_tag_buffer,
    KAFKA_UUID_ZERO,
};
use super::topic_id;
use super::{KafkaErrorCode, SUPPORTED_APIS};

/// DescribeCluster v0–2 (always flexible, KIP-700 / Phase 65–66 / 70).
///
/// Request: include_cluster_authorized_operations (bool), EndpointType (v1+),
/// IncludeFencedBrokers (v2+, accepted; Volant has no fenced brokers), TAG_BUFFER.
/// Response (header v1): throttle, error, error_message, EndpointType (v1+),
/// cluster_id, controller_id, brokers[{id, host, port, rack, IsFenced (v2+), tags}],
/// cluster_authorized_ops, tags.
pub(crate) fn encode_describe_cluster(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    let include_ops = if src.has_remaining() {
        src.get_u8() != 0
    } else {
        false
    };
    // EndpointType: 1=brokers (default), 2=controllers. Volant only serves brokers.
    let endpoint_type = if version >= 1 && src.has_remaining() {
        src.get_i8()
    } else {
        1
    };
    // IncludeFencedBrokers (v2+): parse and ignore — no fenced membership.
    if version >= 2 && src.has_remaining() {
        let _include_fenced = src.get_u8() != 0;
    }
    let _ = skip_tag_buffer(src);

    if version >= 1 && endpoint_type != 1 {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::UnsupportedEndpointType.as_i16());
        put_compact_nullable_string(out, Some("only brokers endpoint (type=1) is supported"));
        out.put_i8(endpoint_type);
        put_compact_string(out, KAFKA_CLUSTER_ID);
        out.put_i32(-1);
        put_compact_array_len(out, 0);
        out.put_i32(AUTH_OPS_OMITTED);
        put_empty_tag_buffer(out);
        return;
    }

    // Cluster Describe ACL when ACLs are enabled.
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i32(0); // throttle
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        put_compact_nullable_string(out, Some("Cluster authorization failed"));
        if version >= 1 {
            out.put_i8(1);
        }
        put_compact_string(out, KAFKA_CLUSTER_ID);
        out.put_i32(-1); // controller
        put_compact_array_len(out, 0); // brokers
        out.put_i32(AUTH_OPS_OMITTED);
        put_empty_tag_buffer(out);
        return;
    }

    let snap = broker.metadata(None);
    out.put_i32(0); // throttle
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_compact_nullable_string(out, None); // error_message
    if version >= 1 {
        out.put_i8(1); // EndpointType = brokers
    }
    put_compact_string(out, KAFKA_CLUSTER_ID);
    out.put_i32(snap.controller_id as i32);
    put_compact_array_len(out, snap.brokers.len());
    for (id, host, port) in &snap.brokers {
        out.put_i32(*id as i32);
        put_compact_string(out, host);
        out.put_i32(i32::from(*port));
        put_compact_nullable_string(out, None); // rack
        if version >= 2 {
            out.put_u8(0); // IsFenced = false (no fenced brokers)
        }
        put_empty_tag_buffer(out);
    }
    out.put_i32(cluster_authorized_ops(broker, principal, include_ops));
    put_empty_tag_buffer(out);
}

/// ListTransactions v0–2 (always flexible, Phase 65–66 / 70).
///
/// Request: compact StateFilters[], compact ProducerIdFilters[],
/// DurationFilter (v1+, ignored), TransactionalIdPattern (v2+, simple glob), tags.
/// Response: throttle, error, compact UnknownStateFilters[], compact
/// TransactionStates[{id, producer_id, state, tags}], tags.
pub(crate) fn encode_list_transactions(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    version: i16,
    principal: &str,
) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        out.put_i32(0);
        out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
        put_compact_array_len(out, 0);
        put_compact_array_len(out, 0);
        put_empty_tag_buffer(out);
        return;
    }

    // Known Kafka transaction states (subset we might filter against).
    const KNOWN_STATES: &[&str] = &[
        "Empty",
        "Ongoing",
        "PrepareCommit",
        "PrepareAbort",
        "CompleteCommit",
        "CompleteAbort",
        "Dead",
        "PrepareEpochFence",
    ];

    let mut state_filters: Vec<String> = Vec::new();
    if let Ok(Some(n)) = get_compact_array_len(src) {
        for _ in 0..n {
            if let Ok(s) = get_compact_string(src) {
                state_filters.push(s);
            }
        }
    }

    let mut pid_filters: Vec<i64> = Vec::new();
    if let Ok(Some(n)) = get_compact_array_len(src) {
        for _ in 0..n {
            if src.remaining() >= 8 {
                pid_filters.push(src.get_i64());
            }
        }
    }
    // DurationFilter (v1+): accept and ignore (no start-time tracking).
    if version >= 1 && src.remaining() >= 8 {
        let _duration_filter = src.get_i64();
    }
    // TransactionalIdPattern (v2+): nullable compact string; simple `*` glob.
    let id_pattern = if version >= 2 {
        get_compact_nullable_string(src).ok().flatten()
    } else {
        None
    };
    let _ = skip_tag_buffer(src);

    let mut unknown_filters: Vec<&str> = Vec::new();
    for f in &state_filters {
        if !KNOWN_STATES.iter().any(|k| *k == f.as_str()) {
            unknown_filters.push(f.as_str());
        }
    }

    let open = broker.list_open_transactions();
    let filtered: Vec<_> = open
        .into_iter()
        .filter(|(tid, pid, state)| {
            if !state_filters.is_empty()
                && !state_filters.iter().any(|f| f == state)
            {
                return false;
            }
            if !pid_filters.is_empty() && !pid_filters.contains(&(*pid as i64)) {
                return false;
            }
            if let Some(ref pat) = id_pattern {
                if !txn_id_pattern_matches(pat, tid) {
                    return false;
                }
            }
            true
        })
        .collect();

    out.put_i32(0); // throttle
    out.put_i16(KafkaErrorCode::None.as_i16());
    put_compact_array_len(out, unknown_filters.len());
    for f in &unknown_filters {
        put_compact_string(out, f);
    }
    put_compact_array_len(out, filtered.len());
    for (tid, pid, state) in &filtered {
        put_compact_string(out, tid);
        out.put_i64(*pid as i64);
        put_compact_string(out, state);
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

/// Match a transactional id against ListTransactions v2 pattern.
///
/// Kafka uses RE2J; Volant supports a minimal glob: `*` = any sequence
/// (including empty), other characters are literal. Empty pattern matches all.
pub(crate) fn txn_id_pattern_matches(pattern: &str, tid: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let pat = pattern.as_bytes();
    let s = tid.as_bytes();
    let mut pi = 0usize;
    let mut si = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_si = 0usize;
    while si < s.len() {
        if pi < pat.len() && pat[pi] == b'*' {
            star_pi = Some(pi);
            star_si = si;
            pi += 1;
        } else if pi < pat.len() && pat[pi] == s[si] {
            pi += 1;
            si += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }
    pi == pat.len()
}

/// DescribeTransactions v0 (always flexible, Phase 66).
///
/// Request: compact TransactionalIds[] (strings), tags.
/// Response: throttle, compact TransactionStates[{error, id, state, timeout,
/// start, producer_id, epoch, compact topics[{name, compact partitions[], tags}],
/// tags}], tags.
pub(crate) fn encode_describe_transactions(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    if broker.acls().is_enabled()
        && !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        )
    {
        // Still emit one entry per requested id with auth error if we can parse ids.
        let mut ids = Vec::new();
        if let Ok(Some(n)) = get_compact_array_len(src) {
            for _ in 0..n {
                if let Ok(s) = get_compact_string(src) {
                    ids.push(s);
                }
            }
        }
        let _ = skip_tag_buffer(src);
        out.put_i32(0);
        put_compact_array_len(out, ids.len());
        for id in &ids {
            out.put_i16(KafkaErrorCode::ClusterAuthorizationFailed.as_i16());
            put_compact_string(out, id);
            put_compact_string(out, "");
            out.put_i32(0);
            out.put_i64(0);
            out.put_i64(-1);
            out.put_i16(-1);
            put_compact_array_len(out, 0);
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
        return;
    }

    let mut ids = Vec::new();
    if let Ok(Some(n)) = get_compact_array_len(src) {
        for _ in 0..n {
            if let Ok(s) = get_compact_string(src) {
                ids.push(s);
            }
        }
    }
    let _ = skip_tag_buffer(src);

    out.put_i32(0); // throttle
    put_compact_array_len(out, ids.len());
    for id in &ids {
        match broker.describe_transaction(id) {
            None => {
                out.put_i16(KafkaErrorCode::TransactionalIdNotFound.as_i16());
                put_compact_string(out, id);
                put_compact_string(out, "");
                out.put_i32(0);
                out.put_i64(0);
                out.put_i64(-1);
                out.put_i16(-1);
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
            }
            Some((state, timeout_ms, start_ms, pid, epoch, topics)) => {
                out.put_i16(KafkaErrorCode::None.as_i16());
                put_compact_string(out, id);
                put_compact_string(out, &state);
                out.put_i32(timeout_ms);
                out.put_i64(start_ms);
                out.put_i64(pid as i64);
                out.put_i16(epoch as i16);
                put_compact_array_len(out, topics.len());
                for (topic, parts) in &topics {
                    put_compact_string(out, topic);
                    put_compact_array_len(out, parts.len());
                    for p in parts {
                        out.put_i32(*p);
                    }
                    put_empty_tag_buffer(out); // topic tags
                }
                put_empty_tag_buffer(out); // state tags
            }
        }
    }
    put_empty_tag_buffer(out);
}

/// DescribeProducers v0 (always flexible, Phase 66).
///
/// Request: compact topics[{name, compact partition_indexes[], tags}], tags.
/// Response: throttle, compact topics[{name, compact partitions[{index, error,
/// error_message, compact active_producers[{pid, epoch, last_seq, last_ts,
/// coord_epoch, txn_start, tags}], tags}], tags}], tags.
pub(crate) fn encode_describe_producers(
    broker: &Broker,
    src: &mut impl Buf,
    out: &mut BytesMut,
    principal: &str,
) {
    // Parse topics.
    let mut topics: Vec<(String, Vec<i32>)> = Vec::new();
    if let Ok(Some(n)) = get_compact_array_len(src) {
        for _ in 0..n {
            let name = match get_compact_string(src) {
                Ok(s) => s,
                Err(_) => break,
            };
            let mut parts = Vec::new();
            if let Ok(Some(pn)) = get_compact_array_len(src) {
                for _ in 0..pn {
                    if src.remaining() >= 4 {
                        parts.push(src.get_i32());
                    }
                }
            }
            let _ = skip_tag_buffer(src);
            topics.push((name, parts));
        }
    }
    let _ = skip_tag_buffer(src);

    out.put_i32(0); // throttle
    put_compact_array_len(out, topics.len());
    for (name, parts) in &topics {
        put_compact_string(out, name);
        put_compact_array_len(out, parts.len());
        for &part in parts {
            out.put_i32(part);
            // ACL: Describe on topic when enabled.
            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(principal),
                    ResourceType::Topic,
                    name,
                    AclOperation::Describe,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                put_compact_nullable_string(out, Some("Topic authorization failed"));
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
                continue;
            }
            if !broker.partition_exists(name, part as u32) {
                out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                put_compact_nullable_string(out, Some("Unknown topic or partition"));
                put_compact_array_len(out, 0);
                put_empty_tag_buffer(out);
                continue;
            }
            let active = broker.describe_producers_for_partition(name, part as u32);
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_compact_nullable_string(out, None);
            put_compact_array_len(out, active.len());
            for (pid, epoch, last_seq, last_ts, coord_epoch, txn_start) in &active {
                out.put_i64(*pid as i64);
                out.put_i32(*epoch);
                out.put_i32(*last_seq);
                out.put_i64(*last_ts);
                out.put_i32(*coord_epoch);
                out.put_i64(*txn_start);
                put_empty_tag_buffer(out);
            }
            put_empty_tag_buffer(out); // partition tags
        }
        put_empty_tag_buffer(out); // topic tags
    }
    put_empty_tag_buffer(out);
}

/// ApiVersions classic v0–2 + flexible v3–5 (Phase 50/51/83).
///
/// Classic response: error, api_keys[{key,min,max}], throttle (v1+ trailing).
/// Flexible v3–5: compact api_keys (each entry ends with TAG_BUFFER), throttle,
/// top-level empty TAG_BUFFER. Response **header** stays v0 (correlation only).
///
/// Request body:
/// - v0–2: empty
/// - v3–4: compact ClientSoftwareName/Version + tags (parsed, ignored)
/// - v5: same + ClusterId (nullable compact) + NodeId (int32) + tags (ignored;
///   no REBOOTSTRAP_REQUIRED — Volant does not check cluster/node identity)
///
/// Response body for v3–5 is wire-identical: empty feature tags (no
/// SupportedFeatures / FinalizedFeatures / ZkMigrationReady). v4 only changes
/// Apache Kafka's MinVersion=0 feature serialization rule; with empty features
/// there is no delta.
pub(crate) fn encode_api_versions(src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    if version >= 3 {
        // Parse and ignore client software fields (KIP-511).
        let _name = get_compact_string(src).ok();
        let _ver = get_compact_string(src).ok();
        if version >= 5 {
            // KIP-1242: ClusterId + NodeId for rebootstrap checks — ignored.
            let _cluster_id = get_compact_nullable_string(src).ok();
            if src.remaining() >= 4 {
                let _node_id = src.get_i32();
            }
        }
        let _ = skip_tag_buffer(src);

        out.put_i16(KafkaErrorCode::None.as_i16());
        put_compact_array_len(out, SUPPORTED_APIS.len());
        for (key, min_v, max_v) in SUPPORTED_APIS {
            out.put_i16(*key as i16);
            out.put_i16(*min_v);
            out.put_i16(*max_v);
            put_empty_tag_buffer(out); // per-struct tags
        }
        out.put_i32(0); // throttle_time_ms
        // Empty top-level tags: no SupportedFeatures (tag 0), FinalizedFeaturesEpoch
        // (tag 1), FinalizedFeatures (tag 2), or ZkMigrationReady (tag 3).
        put_empty_tag_buffer(out);
        return;
    }

    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(SUPPORTED_APIS.len() as i32);
    for (key, min_v, max_v) in SUPPORTED_APIS {
        out.put_i16(*key as i16);
        out.put_i16(*min_v);
        out.put_i16(*max_v);
    }
    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
}

/// Stable cluster id advertised on Metadata v2+ (classic).
const KAFKA_CLUSTER_ID: &str = "volant";

/// Kafka `Integer.MIN_VALUE` — authorized operations not included in the response.
pub(crate) const AUTH_OPS_OMITTED: i32 = i32::MIN;

pub(crate) fn encode_metadata(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16, principal: &str) {
    // Request:
    //   topics: array (v0) / nullable array (v1+) / compact nullable (v9+)
    //     v0: empty = all topics
    //     v1+: null = all topics; empty = no topics
    //     v10+: each topic = TopicId uuid + compact nullable Name + tags
    //   allow_auto_topic_creation: bool (v4+, ignored)
    //   include_cluster_authorized_operations: bool (v8–10 only)
    //   include_topic_authorized_operations: bool (v8+)
    //   TAG_BUFFER (v9+)
    // Response v13+: top-level ErrorCode (int16) before body TAG_BUFFER.
    let flexible = version >= 9;
    // Per-request topic: resolved name (if any) and raw uuid for error rows.
    struct ReqTopic {
        name: Option<String>,
        uuid: [u8; 16],
        // True when the client asked by id only (v12+) and id was unknown.
        unknown_id: bool,
    }

    let list_all: bool;
    let mut requested: Vec<ReqTopic> = Vec::new();

    if flexible {
        match get_compact_array_len(src) {
            Ok(None) => {
                list_all = true;
            }
            Ok(Some(n)) => {
                list_all = false;
                for _ in 0..n {
                    if version >= 10 {
                        let uuid = match get_uuid(src) {
                            Ok(u) => u,
                            Err(_) => break,
                        };
                        let name = match get_compact_nullable_string(src) {
                            Ok(n) => n,
                            Err(_) => break,
                        };
                        let _ = skip_tag_buffer(src);
                        if let Some(r) =
                            topic_id::resolve_metadata_entry(broker, uuid, name, version >= 12)
                        {
                            requested.push(ReqTopic {
                                name: r.name,
                                uuid: r.uuid,
                                unknown_id: r.unknown_id,
                            });
                        }
                    } else {
                        match get_compact_string(src) {
                            Ok(t) => {
                                requested.push(ReqTopic {
                                    name: Some(t),
                                    uuid: KAFKA_UUID_ZERO,
                                    unknown_id: false,
                                });
                                let _ = skip_tag_buffer(src);
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            Err(_) => {
                list_all = true;
            }
        }
    } else {
        let topic_len = if src.remaining() >= 4 {
            src.get_i32()
        } else {
            0
        };
        if version == 0 {
            list_all = topic_len <= 0;
            if topic_len > 0 {
                for _ in 0..topic_len {
                    match get_string(src) {
                        Ok(t) => requested.push(ReqTopic {
                            name: Some(t),
                            uuid: KAFKA_UUID_ZERO,
                            unknown_id: false,
                        }),
                        Err(_) => break,
                    }
                }
            }
        } else if topic_len < 0 {
            list_all = true;
        } else {
            list_all = false;
            for _ in 0..topic_len {
                match get_string(src) {
                    Ok(t) => requested.push(ReqTopic {
                        name: Some(t),
                        uuid: KAFKA_UUID_ZERO,
                        unknown_id: false,
                    }),
                    Err(_) => break,
                }
            }
        }
    }

    // v4+: allow_auto_topic_creation (ignored — Volant does not auto-create on Metadata).
    if version >= 4 && src.remaining() >= 1 {
        let _allow_auto = src.get_u8();
    }

    let mut include_cluster_ops = false;
    let mut include_topic_ops = false;
    if version >= 8 {
        // Cluster authorized ops flag only on request versions 8–10.
        if version <= 10 && src.remaining() >= 1 {
            include_cluster_ops = src.get_u8() != 0;
        }
        if src.remaining() >= 1 {
            include_topic_ops = src.get_u8() != 0;
        }
    }
    if flexible {
        let _ = skip_tag_buffer(src); // request body tags
    }

    // Response includes ClusterAuthorizedOperations only on v8–10.
    let emit_cluster_ops = (8..=10).contains(&version);

    let need_cluster_describe = list_all;
    if broker.acls().is_enabled() && need_cluster_describe {
        if !broker.acls().authorize(
            Some(principal),
            ResourceType::Cluster,
            CLUSTER_RESOURCE,
            AclOperation::Describe,
        ) {
            write_metadata_empty(out, version, include_cluster_ops && emit_cluster_ops, principal, broker);
            return;
        }
    }

    // Unknown-id-only entries (v12) are emitted as error topics after the snap.
    let unknown_ids: Vec<[u8; 16]> = requested
        .iter()
        .filter(|r| r.unknown_id)
        .map(|r| r.uuid)
        .collect();
    let named: Vec<String> = requested
        .iter()
        .filter_map(|r| r.name.clone())
        .collect();

    let filter: Option<Vec<TopicName>> = if list_all {
        None
    } else if named.is_empty() && unknown_ids.is_empty() {
        write_metadata_brokers_header(broker, out, version);
        if flexible {
            put_compact_array_len(out, 0); // empty topics
            if emit_cluster_ops {
                out.put_i32(cluster_authorized_ops(
                    broker,
                    principal,
                    include_cluster_ops,
                ));
            }
            if version >= 13 {
                out.put_i16(KafkaErrorCode::None.as_i16()); // top-level ErrorCode
            }
            put_empty_tag_buffer(out);
        } else {
            out.put_i32(0); // topics
            if version >= 8 {
                out.put_i32(cluster_authorized_ops(
                    broker,
                    principal,
                    include_cluster_ops,
                ));
            }
        }
        return;
    } else if named.is_empty() {
        // Only unknown ids — still write broker header, then error topics.
        write_metadata_brokers_header(broker, out, version);
        if flexible {
            put_compact_array_len(out, unknown_ids.len());
            for uuid in &unknown_ids {
                out.put_i16(KafkaErrorCode::UnknownTopicId.as_i16());
                put_compact_nullable_string(out, None); // name null
                put_uuid(out, uuid);
                out.put_u8(0); // is_internal
                put_compact_array_len(out, 0); // partitions
                out.put_i32(AUTH_OPS_OMITTED);
                put_empty_tag_buffer(out);
            }
            if emit_cluster_ops {
                out.put_i32(cluster_authorized_ops(
                    broker,
                    principal,
                    include_cluster_ops,
                ));
            }
            if version >= 13 {
                out.put_i16(KafkaErrorCode::None.as_i16());
            }
            put_empty_tag_buffer(out);
        }
        return;
    } else {
        Some(named.iter().map(|t| TopicName::new(t.clone())).collect())
    };

    let snap = match &filter {
        None => broker.metadata(None),
        Some(ts) => broker.metadata(Some(ts.as_slice())),
    };

    // throttle_time_ms (v3+)
    if version >= 3 {
        out.put_i32(0);
    }

    // Brokers
    if flexible {
        put_compact_array_len(out, snap.brokers.len());
        for (id, host, port) in &snap.brokers {
            out.put_i32(*id as i32);
            put_compact_string(out, host);
            out.put_i32(i32::from(*port));
            put_compact_nullable_string(out, None); // rack
            put_empty_tag_buffer(out);
        }
    } else {
        out.put_i32(snap.brokers.len() as i32);
        for (id, host, port) in &snap.brokers {
            out.put_i32(*id as i32);
            put_string(out, host);
            out.put_i32(i32::from(*port));
            if version >= 1 {
                put_nullable_string(out, None); // rack
            }
        }
    }

    // cluster_id (v2+)
    if version >= 2 {
        if flexible {
            put_compact_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        } else {
            put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        }
    }

    // controller_id (v1+)
    if version >= 1 {
        out.put_i32(snap.controller_id as i32);
    }

    // Topics
    let topics: Vec<_> = snap
        .topics
        .into_iter()
        .filter(|t| {
            if !broker.acls().is_enabled() {
                return true;
            }
            broker.acls().authorize(
                Some(principal),
                ResourceType::Topic,
                t.name.as_str(),
                AclOperation::Describe,
            )
        })
        .collect();

    if flexible {
        let total = topics.len() + unknown_ids.len();
        put_compact_array_len(out, total);
        for t in topics {
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_compact_string(out, t.name.as_str());
            if version >= 10 {
                topic_id::write_uuid(out, &topic_id::uuid_for_numeric_id(t.topic_id.0));
            }
            out.put_u8(0); // is_internal
            put_compact_array_len(out, t.partitions.len());
            for p in &t.partitions {
                out.put_i16(KafkaErrorCode::None.as_i16());
                out.put_i32(p.partition_id.0 as i32);
                out.put_i32(p.leader as i32);
                out.put_i32(-1); // leader_epoch
                put_compact_array_len(out, p.replicas.len());
                for r in &p.replicas {
                    out.put_i32(*r as i32);
                }
                put_compact_array_len(out, p.isr.len());
                for r in &p.isr {
                    out.put_i32(*r as i32);
                }
                put_compact_array_len(out, 0); // offline_replicas
                put_empty_tag_buffer(out); // partition tags
            }
            out.put_i32(topic_authorized_ops(
                broker,
                principal,
                t.name.as_str(),
                include_topic_ops,
            ));
            put_empty_tag_buffer(out); // topic tags
        }
        for uuid in &unknown_ids {
            out.put_i16(KafkaErrorCode::UnknownTopicId.as_i16());
            put_compact_nullable_string(out, None);
            put_uuid(out, uuid);
            out.put_u8(0);
            put_compact_array_len(out, 0);
            out.put_i32(AUTH_OPS_OMITTED);
            put_empty_tag_buffer(out);
        }
        if emit_cluster_ops {
            out.put_i32(cluster_authorized_ops(
                broker,
                principal,
                include_cluster_ops,
            ));
        }
        if version >= 13 {
            out.put_i16(KafkaErrorCode::None.as_i16()); // top-level ErrorCode
        }
        put_empty_tag_buffer(out); // top-level tags
    } else {
        out.put_i32(topics.len() as i32);
        for t in topics {
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_string(out, t.name.as_str());
            if version >= 1 {
                out.put_u8(0); // is_internal = false
            }
            out.put_i32(t.partitions.len() as i32);
            for p in &t.partitions {
                out.put_i16(KafkaErrorCode::None.as_i16());
                out.put_i32(p.partition_id.0 as i32);
                out.put_i32(p.leader as i32);
                if version >= 7 {
                    out.put_i32(-1); // leader_epoch unknown
                }
                out.put_i32(p.replicas.len() as i32);
                for r in &p.replicas {
                    out.put_i32(*r as i32);
                }
                out.put_i32(p.isr.len() as i32);
                for r in &p.isr {
                    out.put_i32(*r as i32);
                }
                if version >= 5 {
                    out.put_i32(0); // offline_replicas empty
                }
            }
            if version >= 8 {
                out.put_i32(topic_authorized_ops(
                    broker,
                    principal,
                    t.name.as_str(),
                    include_topic_ops,
                ));
            }
        }

        if version >= 8 {
            out.put_i32(cluster_authorized_ops(
                broker,
                principal,
                include_cluster_ops,
            ));
        }
    }
}

/// Write brokers + cluster framing for Metadata when the topic list is empty.
pub(crate) fn write_metadata_brokers_header(broker: &Broker, out: &mut BytesMut, version: i16) {
    // Brokers/controller only; topic array is written by the caller.
    let snap = broker.metadata(None);
    let flexible = version >= 9;
    if version >= 3 {
        out.put_i32(0); // throttle
    }
    if flexible {
        put_compact_array_len(out, snap.brokers.len());
        for (id, host, port) in &snap.brokers {
            out.put_i32(*id as i32);
            put_compact_string(out, host);
            out.put_i32(i32::from(*port));
            put_compact_nullable_string(out, None);
            put_empty_tag_buffer(out);
        }
        put_compact_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        out.put_i32(snap.controller_id as i32);
    } else {
        out.put_i32(snap.brokers.len() as i32);
        for (id, host, port) in &snap.brokers {
            out.put_i32(*id as i32);
            put_string(out, host);
            out.put_i32(i32::from(*port));
            if version >= 1 {
                put_nullable_string(out, None);
            }
        }
        if version >= 2 {
            put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        }
        if version >= 1 {
            out.put_i32(snap.controller_id as i32);
        }
    }
}

/// ACL-denied Metadata: empty brokers and topics with correct versioned fields.
pub(crate) fn write_metadata_empty(
    out: &mut BytesMut,
    version: i16,
    include_cluster_ops: bool,
    _principal: &str,
    _broker: &Broker,
) {
    let flexible = version >= 9;
    let emit_cluster_ops = (8..=10).contains(&version);
    if version >= 3 {
        out.put_i32(0);
    }
    if flexible {
        put_compact_array_len(out, 0); // brokers
        put_compact_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        out.put_i32(-1); // controller_id
        put_compact_array_len(out, 0); // topics
        if emit_cluster_ops {
            out.put_i32(if include_cluster_ops {
                0
            } else {
                AUTH_OPS_OMITTED
            });
        }
        if version >= 13 {
            out.put_i16(KafkaErrorCode::None.as_i16());
        }
        put_empty_tag_buffer(out);
    } else {
        out.put_i32(0); // brokers
        if version >= 2 {
            put_nullable_string(out, Some(KAFKA_CLUSTER_ID));
        }
        if version >= 1 {
            out.put_i32(-1); // controller_id
        }
        out.put_i32(0); // topics
        if version >= 8 {
            // Denied cluster describe → omit ops (or empty bitfield). Use omitted when not requested.
            out.put_i32(if include_cluster_ops {
                0
            } else {
                AUTH_OPS_OMITTED
            });
        }
    }
}

pub(crate) fn topic_authorized_ops(
    broker: &Broker,
    principal: &str,
    topic: &str,
    include: bool,
) -> i32 {
    if !include {
        return AUTH_OPS_OMITTED;
    }
    let ops = [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Create,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::Describe,
    ];
    let mut bits = 0i32;
    for op in ops {
        let allowed = if broker.acls().is_enabled() {
            broker
                .acls()
                .authorize(Some(principal), ResourceType::Topic, topic, op)
        } else {
            true
        };
        if allowed {
            bits |= 1i32 << (volant_op_to_kafka(op) as u32);
        }
    }
    bits
}

pub(crate) fn cluster_authorized_ops(broker: &Broker, principal: &str, include: bool) -> i32 {
    if !include {
        return AUTH_OPS_OMITTED;
    }
    let ops = [
        AclOperation::Create,
        AclOperation::Alter,
        AclOperation::Describe,
        AclOperation::ClusterAction,
    ];
    let mut bits = 0i32;
    for op in ops {
        let allowed = if broker.acls().is_enabled() {
            broker
                .acls()
                .authorize(Some(principal), ResourceType::Cluster, CLUSTER_RESOURCE, op)
        } else {
            true
        };
        if allowed {
            bits |= 1i32 << (volant_op_to_kafka(op) as u32);
        }
    }
    bits
}

pub(crate) fn encode_find_coordinator(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // FindCoordinator classic v0–2 + flexible v3–6:
    //   v0: key
    //   v1–2: key + key_type; response throttle + error_message
    //   v3: compact key + key_type + tags; compact host/error_message + tags
    //   v4: key_type + compact CoordinatorKeys batch → Coordinators array
    //   v5: wire-identical to v4 (KIP-890 TRANSACTION_ABORTABLE — never emitted)
    //   v6: wire-identical to v4/v5 (KIP-932 share key_type 2 rejected)
    let flexible = version >= 3;
    let snap = broker.metadata(None);
    let (id, host, port) = snap
        .brokers
        .first()
        .cloned()
        .unwrap_or((snap.node_id, snap.host.clone(), snap.port));
    let node_id = id as i32;
    let port_i32 = i32::from(port);

    if version >= 4 {
        // v4–6 request: KeyType + CoordinatorKeys (compact) + tags
        if src.remaining() < 1 {
            write_find_coordinator_v4_error(out, &[], "missing key_type");
            return;
        }
        let key_type = src.get_i8();
        // 0 = group, 1 = transaction — both resolve to this broker.
        // 2 = share (KIP-932) — not supported; reject with InvalidRequest.
        if key_type != 0 && key_type != 1 {
            write_find_coordinator_v4_error(out, &[], "unsupported key_type");
            return;
        }
        let keys = match get_compact_array_len(src) {
            Ok(Some(n)) => {
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    match get_compact_string(src) {
                        Ok(k) => keys.push(k),
                        Err(_) => break,
                    }
                }
                keys
            }
            Ok(None) | Err(_) => Vec::new(),
        };
        let _ = skip_tag_buffer(src);

        out.put_i32(0); // throttle
        put_compact_array_len(out, keys.len());
        for key in &keys {
            put_compact_string(out, key);
            out.put_i32(node_id);
            put_compact_string(out, &host);
            out.put_i32(port_i32);
            out.put_i16(KafkaErrorCode::None.as_i16());
            put_compact_nullable_string(out, None); // error_message
            put_empty_tag_buffer(out);
        }
        put_empty_tag_buffer(out);
        return;
    }

    // v0–3 single key
    let key_result = if flexible {
        get_compact_string(src)
    } else {
        get_string(src)
    };
    let _key = match key_result {
        Ok(g) => g,
        Err(_) => {
            write_find_coordinator_error(
                out,
                version,
                flexible,
                KafkaErrorCode::InvalidRequest,
                Some("invalid key"),
            );
            return;
        }
    };

    if version >= 1 {
        if src.remaining() < 1 {
            write_find_coordinator_error(
                out,
                version,
                flexible,
                KafkaErrorCode::InvalidRequest,
                Some("missing key_type"),
            );
            return;
        }
        let key_type = src.get_i8();
        // 0 = group, 1 = transaction — both resolve to this broker.
        if key_type != 0 && key_type != 1 {
            write_find_coordinator_error(
                out,
                version,
                flexible,
                KafkaErrorCode::InvalidRequest,
                Some("unsupported key_type"),
            );
            return;
        }
    }
    if flexible {
        let _ = skip_tag_buffer(src);
    }

    if version >= 1 {
        out.put_i32(0); // throttle_time_ms
    }
    out.put_i16(KafkaErrorCode::None.as_i16());
    if version >= 1 {
        if flexible {
            put_compact_nullable_string(out, None);
        } else {
            put_nullable_string(out, None);
        }
    }
    out.put_i32(node_id);
    if flexible {
        put_compact_string(out, &host);
    } else {
        put_string(out, &host);
    }
    out.put_i32(port_i32);
    if flexible {
        put_empty_tag_buffer(out);
    }
}

pub(crate) fn write_find_coordinator_error(
    out: &mut BytesMut,
    version: i16,
    flexible: bool,
    code: KafkaErrorCode,
    msg: Option<&str>,
) {
    if version >= 1 {
        out.put_i32(0); // throttle
    }
    out.put_i16(code.as_i16());
    if version >= 1 {
        if flexible {
            put_compact_nullable_string(out, msg);
        } else {
            put_nullable_string(out, msg);
        }
    }
    out.put_i32(-1);
    if flexible {
        put_compact_string(out, "");
    } else {
        put_string(out, "");
    }
    out.put_i32(-1);
    if flexible {
        put_empty_tag_buffer(out);
    }
}

pub(crate) fn write_find_coordinator_v4_error(out: &mut BytesMut, keys: &[&str], msg: &str) {
    out.put_i32(0); // throttle
    put_compact_array_len(out, keys.len());
    for key in keys {
        put_compact_string(out, key);
        out.put_i32(-1);
        put_compact_string(out, "");
        out.put_i32(-1);
        out.put_i16(KafkaErrorCode::InvalidRequest.as_i16());
        put_compact_nullable_string(out, Some(msg));
        put_empty_tag_buffer(out);
    }
    put_empty_tag_buffer(out);
}

