//! Kafka connection accept loop and API handlers.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use volant_core::{Error, MessageBatch, Offset, PartitionId, Result, TopicName};

use crate::acl::{AclOperation, ResourceType, CLUSTER_RESOURCE};
use crate::broker::Broker;

use super::codec::{
    decode_records, decode_request_header, encode_message_set, encode_record_batch,
    encode_response_frame, get_bytes, get_nullable_string, get_string, put_bytes,
    put_response_header, put_string, try_decode_request,
};
use super::{ApiKey, KafkaErrorCode, KAFKA_ANONYMOUS_PRINCIPAL, SUPPORTED_APIS};

/// Accept Kafka-protocol connections until the listener fails fatally.
pub async fn serve_kafka_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    if let Ok(local) = listener.local_addr() {
        info!(%local, "volant kafka shim listening");
    }
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                broker.metrics().record_connection();
                debug!(%peer, "kafka connection accepted");
                let b = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(e) = handle_kafka_connection(stream, b).await {
                        debug!(%peer, error = %e, "kafka connection closed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "kafka accept failed");
                return Err(Error::Io(e));
            }
        }
    }
}

async fn handle_kafka_connection(mut stream: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            match try_decode_request(&mut buf)? {
                Some(body) => {
                    let response = dispatch_kafka(&broker, body);
                    let frame = encode_response_frame(&response);
                    stream.write_all(&frame).await?;
                }
                None => break,
            }
        }
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

fn dispatch_kafka(broker: &Broker, body: bytes::Bytes) -> BytesMut {
    let mut src = body;
    let hdr = match decode_request_header(&mut src) {
        Ok(h) => h,
        Err(e) => {
            debug!(error = %e, "kafka header decode failed");
            // Cannot respond without correlation_id.
            return BytesMut::new();
        }
    };
    let corr = hdr.correlation_id;
    let mut out = BytesMut::new();
    put_response_header(&mut out, corr);

    match ApiKey::from_i16(hdr.api_key) {
        Some(ApiKey::ApiVersions) if hdr.api_version == 0 => {
            encode_api_versions(&mut out);
        }
        Some(ApiKey::Metadata) if (0..=1).contains(&hdr.api_version) => {
            encode_metadata(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::Produce) if (0..=3).contains(&hdr.api_version) => {
            encode_produce(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(ApiKey::Fetch) if (0..=4).contains(&hdr.api_version) => {
            encode_fetch(broker, &mut src, &mut out, hdr.api_version);
        }
        Some(_) => {
            // Supported API but wrong version — use a generic error body when possible.
            // ApiVersions clients probe versions; return UnsupportedVersion-shaped empty.
            out.put_i16(KafkaErrorCode::UnsupportedVersion.as_i16());
        }
        None => {
            out.put_i16(KafkaErrorCode::UnsupportedVersion.as_i16());
        }
    }
    out
}

fn encode_api_versions(out: &mut BytesMut) {
    out.put_i16(KafkaErrorCode::None.as_i16());
    out.put_i32(SUPPORTED_APIS.len() as i32);
    for (key, min_v, max_v) in SUPPORTED_APIS {
        out.put_i16(*key as i16);
        out.put_i16(*min_v);
        out.put_i16(*max_v);
    }
}

fn encode_metadata(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // Authorization: Cluster Describe when listing all; Topic Describe per topic.
    if broker.acls().is_enabled() {
        // We'll check per-topic below; for empty list need cluster describe.
    }

    let topic_count = if src.remaining() >= 4 {
        src.get_i32()
    } else {
        0
    };
    let mut requested: Vec<String> = Vec::new();
    if topic_count > 0 {
        for _ in 0..topic_count {
            match get_string(src) {
                Ok(t) => requested.push(t),
                Err(_) => break,
            }
        }
    }

    if broker.acls().is_enabled() {
        if requested.is_empty() {
            if !broker.acls().authorize(
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Cluster,
                CLUSTER_RESOURCE,
                AclOperation::Describe,
            ) {
                // Return empty with error on a synthetic topic — Metadata v0 has no top-level error.
                // Emit empty brokers + empty topics.
                out.put_i32(0); // brokers
                if version >= 1 {
                    out.put_i32(-1); // controller_id
                }
                out.put_i32(0); // topics
                return;
            }
        }
    }

    let filter: Option<Vec<TopicName>> = if requested.is_empty() {
        None
    } else {
        Some(requested.iter().map(|t| TopicName::new(t.clone())).collect())
    };
    let snap = match &filter {
        None => broker.metadata(None),
        Some(ts) => broker.metadata(Some(ts.as_slice())),
    };

    // Brokers
    out.put_i32(snap.brokers.len() as i32);
    for (id, host, port) in &snap.brokers {
        out.put_i32(*id as i32);
        put_string(out, host);
        out.put_i32(i32::from(*port));
    }
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
                Some(KAFKA_ANONYMOUS_PRINCIPAL),
                ResourceType::Topic,
                t.name.as_str(),
                AclOperation::Describe,
            )
        })
        .collect();
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
            out.put_i32(p.replicas.len() as i32);
            for r in &p.replicas {
                out.put_i32(*r as i32);
            }
            out.put_i32(p.isr.len() as i32);
            for r in &p.isr {
                out.put_i32(*r as i32);
            }
        }
    }
}

fn encode_produce(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // Produce v3+ prepends transactional_id (nullable string).
    if version >= 3 {
        let _txn_id = match get_nullable_string(src) {
            Ok(v) => v,
            Err(_) => {
                out.put_i32(0);
                return;
            }
        };
    }

    if src.remaining() < 2 + 4 {
        out.put_i32(0); // topic responses empty
        return;
    }
    let acks = src.get_i16();
    let _timeout_ms = src.get_i32();
    let volant_acks: u8 = match acks {
        -1 => 255,
        0 => 0,
        _ => 1,
    };

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        put_string(out, &topic);

        if src.remaining() < 4 {
            out.put_i32(0);
            break;
        }
        let part_count = src.get_i32();
        out.put_i32(part_count.max(0));

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 {
                break;
            }
            let partition = src.get_i32();
            let record_set = match get_bytes(src) {
                Ok(b) => b.unwrap_or_default(),
                Err(_) => {
                    out.put_i32(partition);
                    out.put_i16(KafkaErrorCode::InvalidMessage.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1); // log_append_time
                    }
                    continue;
                }
            };

            out.put_i32(partition);

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(KAFKA_ANONYMOUS_PRINCIPAL),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Write,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                out.put_i64(-1);
                if version >= 2 {
                    out.put_i64(-1);
                }
                continue;
            }

            let messages = match decode_records(&record_set) {
                Ok(m) => m,
                Err(e) => {
                    debug!(error = %e, "kafka produce records decode failed");
                    out.put_i16(KafkaErrorCode::CorruptMessage.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                    continue;
                }
            };
            if messages.is_empty() {
                out.put_i16(KafkaErrorCode::None.as_i16());
                out.put_i64(0);
                if version >= 2 {
                    out.put_i64(-1);
                }
                continue;
            }

            let name = TopicName::new(topic.clone());
            let batch = MessageBatch { messages };
            let wait = if volant_acks == 255 {
                Some(Duration::from_secs(5))
            } else {
                None
            };
            match broker.produce_with_acks(
                &name,
                PartitionId(partition as u32),
                batch,
                volant_acks,
                wait,
            ) {
                Ok((records, 0)) => {
                    let base = records
                        .first()
                        .map(|r| r.offset.raw() as i64)
                        .unwrap_or(0);
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    out.put_i64(base);
                    if version >= 2 {
                        out.put_i64(-1); // log_append_time unused
                    }
                }
                Ok((_, err))
                    if err == volant_protocol::ErrorCode::NotLeaderForPartition as u16 =>
                {
                    out.put_i16(KafkaErrorCode::NotLeaderForPartition.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                }
                Ok((_, _)) => {
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                }
                Err(Error::NotFound(_)) => {
                    out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                }
                Err(_) => {
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                    out.put_i64(-1);
                    if version >= 2 {
                        out.put_i64(-1);
                    }
                }
            }
        }
    }

    // Produce v1+ appends throttle_time_ms at the end.
    if version >= 1 {
        out.put_i32(0);
    }
}

fn encode_fetch(broker: &Broker, src: &mut impl Buf, out: &mut BytesMut, version: i16) {
    // FetchRequest v0–2: replica_id, max_wait, min_bytes, [topic ...]
    // v3: + max_bytes
    // v4: + isolation_level
    if src.remaining() < 4 + 4 + 4 {
        if version >= 1 {
            out.put_i32(0); // throttle
        }
        out.put_i32(0);
        return;
    }
    let _replica_id = src.get_i32();
    let _max_wait = src.get_i32();
    let _min_bytes = src.get_i32();
    if version >= 3 {
        if src.remaining() < 4 {
            if version >= 1 {
                out.put_i32(0);
            }
            out.put_i32(0);
            return;
        }
        let _max_bytes = src.get_i32();
    }
    if version >= 4 {
        if src.remaining() < 1 {
            out.put_i32(0); // throttle
            out.put_i32(0);
            return;
        }
        let _isolation = src.get_u8();
    }

    // Fetch response v1+ starts with throttle_time_ms.
    if version >= 1 {
        out.put_i32(0);
    }

    if src.remaining() < 4 {
        out.put_i32(0);
        return;
    }
    let topic_count = src.get_i32();
    out.put_i32(topic_count.max(0));

    for _ in 0..topic_count.max(0) {
        let topic = match get_string(src) {
            Ok(t) => t,
            Err(_) => break,
        };
        put_string(out, &topic);

        if src.remaining() < 4 {
            out.put_i32(0);
            break;
        }
        let part_count = src.get_i32();
        out.put_i32(part_count.max(0));

        for _ in 0..part_count.max(0) {
            if src.remaining() < 4 + 8 + 4 {
                break;
            }
            let partition = src.get_i32();
            let fetch_offset = src.get_i64();
            let max_bytes = src.get_i32().max(0) as usize;

            out.put_i32(partition);

            if broker.acls().is_enabled()
                && !broker.acls().authorize(
                    Some(KAFKA_ANONYMOUS_PRINCIPAL),
                    ResourceType::Topic,
                    &topic,
                    AclOperation::Read,
                )
            {
                out.put_i16(KafkaErrorCode::TopicAuthorizationFailed.as_i16());
                out.put_i64(-1); // high_watermark
                if version >= 4 {
                    out.put_i64(-1); // last_stable_offset
                    out.put_i32(0); // aborted_transactions
                }
                put_bytes(out, Some(&[]));
                continue;
            }

            let name = TopicName::new(topic.clone());
            // Estimate max messages from max_bytes (rough).
            let max_messages = (max_bytes / 64).clamp(1, 10_000);
            match broker.fetch(
                &name,
                PartitionId(partition as u32),
                Offset::new(fetch_offset.max(0) as u64),
                max_messages,
            ) {
                Ok(records) => {
                    // Trim to max_bytes approximately.
                    let mut selected = Vec::new();
                    let mut used = 0usize;
                    for r in records {
                        let approx =
                            r.value.len() + r.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32;
                        if !selected.is_empty() && used + approx > max_bytes {
                            break;
                        }
                        used += approx;
                        selected.push(r);
                    }
                    let hwm = selected
                        .last()
                        .map(|r| r.offset.raw() as i64 + 1)
                        .unwrap_or(fetch_offset);
                    // Prefer broker HWM if available via metadata.
                    let hwm = broker
                        .metadata(Some(&[name.clone()]))
                        .topics
                        .first()
                        .and_then(|t| {
                            t.partitions
                                .iter()
                                .find(|p| p.partition_id.0 == partition as u32)
                                .map(|p| p.hwm as i64)
                        })
                        .unwrap_or(hwm);

                    let set = if version >= 4 {
                        encode_record_batch(&selected)
                    } else {
                        encode_message_set(&selected)
                    };
                    out.put_i16(KafkaErrorCode::None.as_i16());
                    out.put_i64(hwm);
                    if version >= 4 {
                        out.put_i64(hwm); // last_stable_offset ≈ hwm (no txns)
                        out.put_i32(0); // aborted_transactions empty
                    }
                    put_bytes(out, Some(&set));
                }
                Err(Error::NotFound(_)) => {
                    out.put_i16(KafkaErrorCode::UnknownTopicOrPartition.as_i16());
                    out.put_i64(-1);
                    if version >= 4 {
                        out.put_i64(-1);
                        out.put_i32(0);
                    }
                    put_bytes(out, Some(&[]));
                }
                Err(_) => {
                    out.put_i16(KafkaErrorCode::Unknown.as_i16());
                    out.put_i64(-1);
                    if version >= 4 {
                        out.put_i64(-1);
                        out.put_i32(0);
                    }
                    put_bytes(out, Some(&[]));
                }
            }
        }
    }
}
