//! Background follower ReplicaFetch loops.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, warn};
use volant_core::{Message, Offset, PartitionId, Record, TopicName};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_response, pack_request, ErrorCode, FetchRecord, Request, Response,
};

use crate::broker::Broker;

/// Spawn a task that periodically ReplicaFetches for all local follower partitions.
pub fn run_follower_loops(broker: Arc<Broker>) {
    tokio::spawn(async move {
        let interval = broker
            .cluster_config()
            .map(|c| {
                Duration::from_millis(u64::from(c.replica_fetch_max_wait_ms).max(50))
            })
            .unwrap_or(Duration::from_millis(200));
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if broker.cluster_config().is_none() {
                continue;
            }
            let targets = broker.follower_targets();
            for (topic, partition, leader_id, from_offset) in targets {
                if let Err(e) = fetch_once(&broker, &topic, partition, leader_id, from_offset).await
                {
                    debug!(
                        topic = %topic,
                        partition,
                        leader_id,
                        error = %e,
                        "replica fetch failed"
                    );
                }
            }
        }
    });
}

async fn fetch_once(
    broker: &Broker,
    topic: &str,
    partition: u32,
    leader_id: u32,
    from_offset: u64,
) -> volant_core::Result<()> {
    let Some(addr) = broker.broker_addr(leader_id) else {
        return Ok(());
    };
    let max_bytes = broker
        .cluster_config()
        .map(|c| c.replica_fetch_max_bytes)
        .unwrap_or(1_048_576);
    let replica_id = broker.node_id();

    let mut stream = TcpStream::connect(&addr).await?;
    let req = Request::ReplicaFetch {
        topic: topic.to_owned(),
        partition,
        from_offset,
        max_bytes,
        replica_id,
    };
    let frame = pack_request(1, &req)?;
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out)?;
    stream.write_all(&out).await?;

    let mut buf = BytesMut::with_capacity(64 * 1024);
    let resp = loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(volant_core::Error::Protocol(
                "replica fetch connection closed".into(),
            ));
        }
        if let Some(frame) = decode_frame(&mut buf)? {
            break decode_response(frame.header.opcode, &frame.payload)?;
        }
    };

    match resp {
        Response::ReplicaFetch {
            error_code,
            records,
            high_watermark: _,
            leader_epoch,
            ..
        } => {
            if error_code == ErrorCode::NotLeaderForPartition as u16 {
                // Metadata will catch up via ClusterState.
                return Ok(());
            }
            if error_code != 0 {
                warn!(error_code, "replica fetch error");
                return Ok(());
            }
            if records.is_empty() {
                return Ok(());
            }
            let converted: Vec<Record> = records.into_iter().map(wire_to_record).collect();
            broker.append_replica_records(
                &TopicName::new(topic),
                PartitionId(partition),
                &converted,
                leader_epoch,
            )?;
            Ok(())
        }
        Response::Error { code, message } => {
            debug!(code, %message, "replica fetch error response");
            Ok(())
        }
        other => Err(volant_core::Error::Protocol(format!(
            "unexpected replica fetch response: {other:?}"
        ))),
    }
}

fn wire_to_record(r: FetchRecord) -> Record {
    Record {
        offset: Offset::new(r.offset),
        timestamp_ms: r.timestamp_ms,
        key: r.key,
        value: r.value,
        headers: r.headers,
    }
}

/// Build a Message from a Record (unused helper kept for clarity).
#[allow(dead_code)]
fn record_to_message(r: &Record) -> Message {
    Message {
        key: r.key.clone(),
        value: r.value.clone(),
        timestamp_ms: Some(r.timestamp_ms),
        headers: r.headers.clone(),
    }
}
