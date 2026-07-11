//! Framed TCP server for the Volant broker.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use volant_core::{Error, Message, MessageBatch, Offset, PartitionId, Result, TopicName};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_request, pack_response, BrokerInfo, ErrorCode, FetchRecord, Frame, PartitionInfo,
    Request, Response, TopicInfo,
};

use crate::broker::Broker;

/// Bind and serve until the accept loop fails fatally.
pub async fn run_server(addr: SocketAddr, broker: Arc<Broker>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    broker.set_advertised(local.ip().to_string(), local.port());
    info!(%local, "volant broker listening");
    serve_listener(listener, broker).await
}

/// Accept loop over an already-bound listener (useful for port-0 e2e tests).
pub async fn serve_listener(listener: TcpListener, broker: Arc<Broker>) -> Result<()> {
    if let Ok(local) = listener.local_addr() {
        broker.set_advertised(local.ip().to_string(), local.port());
        info!(%local, "volant broker accept loop started");
    }
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(%peer, "accepted connection");
                let b = Arc::clone(&broker);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, b).await {
                        debug!(%peer, error = %e, "connection closed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "accept failed");
                return Err(Error::Io(e));
            }
        }
    }
}

async fn handle_connection(mut stream: TcpStream, broker: Arc<Broker>) -> Result<()> {
    let mut buf = BytesMut::with_capacity(8 * 1024);
    loop {
        // Drain any complete frames already buffered.
        loop {
            match decode_frame(&mut buf)? {
                Some(frame) => {
                    let corr = frame.header.correlation_id;
                    let response = dispatch(&broker, frame).await;
                    write_response(&mut stream, corr, response).await?;
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

async fn write_response(stream: &mut TcpStream, corr: u32, response: Response) -> Result<()> {
    let frame = pack_response(corr, &response)?;
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out)?;
    stream.write_all(&out).await?;
    Ok(())
}

async fn dispatch(broker: &Broker, frame: Frame) -> Response {
    let req = match decode_request(frame.header.opcode, &frame.payload) {
        Ok(r) => r,
        Err(e) => {
            return Response::Error {
                code: ErrorCode::Protocol as u16,
                message: e.to_string(),
            };
        }
    };

    match handle_request(broker, req).await {
        Ok(resp) => resp,
        Err(e) => map_error(e),
    }
}

async fn handle_request(broker: &Broker, req: Request) -> Result<Response> {
    match req {
        Request::CreateTopic { name, partitions } => {
            let topic = TopicName::new(name.clone());
            let id = broker.create_topic(topic, partitions)?;
            Ok(Response::CreateTopic {
                topic_id: id.0,
                name,
                partitions,
                error_code: 0,
            })
        }
        Request::DeleteTopic { name } => {
            let topic = TopicName::new(name.clone());
            broker.delete_topic(&topic)?;
            Ok(Response::DeleteTopic {
                name,
                error_code: 0,
            })
        }
        Request::Metadata { topics } => {
            let filter: Option<Vec<TopicName>> = if topics.is_empty() {
                None
            } else {
                Some(topics.into_iter().map(TopicName::new).collect())
            };
            let snap = broker.metadata(filter.as_deref());
            Ok(Response::Metadata {
                brokers: vec![BrokerInfo {
                    node_id: snap.node_id,
                    host: snap.host,
                    port: snap.port,
                }],
                topics: snap
                    .topics
                    .into_iter()
                    .map(|t| TopicInfo {
                        name: t.name.0,
                        topic_id: t.topic_id.0,
                        error_code: 0,
                        partitions: t
                            .partitions
                            .into_iter()
                            .map(|p| PartitionInfo {
                                partition_id: p.partition_id.0,
                                leader: p.leader,
                                hwm: p.hwm,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
        Request::Produce {
            topic,
            partition,
            acks: _,
            messages,
        } => {
            let topic_name = TopicName::new(topic.clone());
            if messages.is_empty() {
                return Err(Error::InvalidArgument("empty produce batch".into()));
            }

            // Resolve partition: -1 → key of first message or round-robin.
            let pid = if partition < 0 {
                let key = messages[0].key.as_deref();
                broker.select_partition(&topic_name, key)?
            } else {
                PartitionId(partition as u32)
            };

            let mut batch = MessageBatch::default();
            for m in messages {
                let timestamp_ms = if m.timestamp_ms < 0 {
                    None
                } else {
                    Some(m.timestamp_ms)
                };
                batch.messages.push(Message {
                    key: m.key,
                    value: m.value,
                    timestamp_ms,
                    headers: m.headers,
                });
            }

            let records = broker.produce(&topic_name, pid, batch)?;
            let base_offset = records.first().map(|r| r.offset.raw()).unwrap_or(0);
            let count = records.len() as u32;
            // Flush for acks=1 durability on single node.
            broker.flush(&topic_name, pid)?;
            Ok(Response::Produce {
                topic,
                partition: pid.0,
                base_offset,
                count,
                error_code: 0,
            })
        }
        Request::Fetch {
            topic,
            partition,
            from_offset,
            max_messages,
            max_bytes: _,
            max_wait_ms,
        } => {
            let topic_name = TopicName::new(topic.clone());
            let pid = PartitionId(partition);
            let from = Offset::new(from_offset);
            let max = max_messages as usize;

            let mut records = broker.fetch(&topic_name, pid, from, max)?;
            if records.is_empty() && max_wait_ms > 0 {
                // Simple long-poll: poll every 10ms until data or timeout.
                let deadline =
                    tokio::time::Instant::now() + Duration::from_millis(u64::from(max_wait_ms));
                while records.is_empty() && tokio::time::Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    records = broker.fetch(&topic_name, pid, from, max)?;
                }
            }

            let hwm = broker.high_watermark(&topic_name, pid)?;
            let wire_records = records
                .into_iter()
                .map(|r| FetchRecord {
                    offset: r.offset.raw(),
                    timestamp_ms: r.timestamp_ms,
                    key: r.key,
                    value: r.value,
                    headers: r.headers,
                })
                .collect();

            Ok(Response::Fetch {
                topic,
                partition,
                high_watermark: hwm,
                error_code: 0,
                records: wire_records,
            })
        }
        Request::OffsetCommit | Request::OffsetFetch => Err(Error::NotImplemented(
            "offset commit/fetch reserved for Phase 3",
        )),
    }
}

fn map_error(e: Error) -> Response {
    let (code, message) = match &e {
        Error::NotFound(m) => (ErrorCode::NotFound as u16, m.clone()),
        Error::InvalidArgument(m) => (ErrorCode::InvalidArg as u16, m.clone()),
        Error::Storage(m) => (ErrorCode::Storage as u16, m.clone()),
        Error::Protocol(m) => (ErrorCode::Protocol as u16, m.clone()),
        Error::Io(err) => (ErrorCode::Io as u16, err.to_string()),
        Error::NotImplemented(m) => (ErrorCode::Unsupported as u16, (*m).to_string()),
    };
    Response::Error { code, message }
}
