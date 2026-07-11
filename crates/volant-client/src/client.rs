//! Networked async client for Volant brokers.

use std::sync::atomic::{AtomicU32, Ordering};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use volant_core::{Error, Message, Offset, Result, TopicId};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_response, pack_request, Assignment, BrokerInfo, ErrorCode, FetchRecord,
    OffsetCommitEntry, OffsetEntry, OffsetFetchEntry, ProduceMessage, Request, Response, TopicInfo,
};

use crate::config::ClientConfig;

/// Result of a successful produce call.
#[derive(Debug, Clone)]
pub struct ProduceResult {
    /// Topic name.
    pub topic: String,
    /// Partition written to.
    pub partition: u32,
    /// First offset of the batch.
    pub base_offset: u64,
    /// Number of records written.
    pub count: u32,
}

/// Result of a successful fetch call.
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: u32,
    /// High watermark.
    pub high_watermark: u64,
    /// Fetched records.
    pub records: Vec<FetchRecord>,
}

/// Cluster metadata returned by the broker.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// Known brokers.
    pub brokers: Vec<BrokerInfo>,
    /// Topics.
    pub topics: Vec<TopicInfo>,
}

/// Result of a successful JoinGroup.
#[derive(Debug, Clone)]
pub struct JoinGroupResult {
    /// Group generation.
    pub generation: u32,
    /// Broker-assigned member id.
    pub member_id: String,
    /// Partition assignment for this member.
    pub assignment: Vec<Assignment>,
}

/// Result of a Heartbeat call.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// Embedded error code (`0` ok; `9` rebalance in progress).
    pub error_code: u16,
}

impl HeartbeatResult {
    /// Whether the client should re-join the group.
    pub fn needs_rebalance(&self) -> bool {
        self.error_code == ErrorCode::RebalanceInProgress as u16
            || self.error_code == ErrorCode::IllegalGeneration as u16
            || self.error_code == ErrorCode::UnknownMemberId as u16
    }
}

/// Async TCP client (sequential request/response over one connection).
#[derive(Debug)]
pub struct Client {
    stream: Mutex<TcpStream>,
    next_corr: AtomicU32,
    #[allow(dead_code)]
    config: ClientConfig,
}

impl Client {
    /// Connect to the first configured broker address.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let addr = config
            .brokers
            .first()
            .ok_or_else(|| Error::InvalidArgument("no brokers configured".into()))?;
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: Mutex::new(stream),
            next_corr: AtomicU32::new(1),
            config,
        })
    }

    /// Connect to a single `host:port`.
    pub async fn connect_addr(addr: impl AsRef<str>) -> Result<Self> {
        Self::connect(ClientConfig {
            brokers: vec![addr.as_ref().to_owned()],
            ..ClientConfig::default()
        })
        .await
    }

    /// Create a topic; returns assigned topic id.
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<TopicId> {
        let resp = self
            .round_trip(Request::CreateTopic {
                name: name.to_owned(),
                partitions,
            })
            .await?;
        match resp {
            Response::CreateTopic {
                topic_id,
                error_code,
                ..
            } => {
                check_ok(error_code, "create_topic")?;
                Ok(TopicId(topic_id))
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for create_topic: {other:?}"
            ))),
        }
    }

    /// Delete a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let resp = self
            .round_trip(Request::DeleteTopic {
                name: name.to_owned(),
            })
            .await?;
        match resp {
            Response::DeleteTopic { error_code, .. } => {
                check_ok(error_code, "delete_topic")?;
                Ok(())
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for delete_topic: {other:?}"
            ))),
        }
    }

    /// Fetch cluster metadata (all topics).
    pub async fn metadata(&self) -> Result<Metadata> {
        let resp = self
            .round_trip(Request::Metadata { topics: vec![] })
            .await?;
        match resp {
            Response::Metadata { brokers, topics } => Ok(Metadata { brokers, topics }),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for metadata: {other:?}"
            ))),
        }
    }

    /// Produce messages to a topic (default acks from config, usually `1`).
    ///
    /// `partition = None` asks the broker to assign (key-hash / round-robin).
    /// On `NotLeaderForPartition`, refreshes metadata once and retries.
    pub async fn produce(
        &self,
        topic: &str,
        partition: Option<u32>,
        messages: Vec<Message>,
    ) -> Result<ProduceResult> {
        let acks = self.config.acks;
        self.produce_with_acks(topic, partition, messages, acks)
            .await
    }

    /// Produce with explicit acks (`1` leader, `255` all ISR).
    pub async fn produce_with_acks(
        &self,
        topic: &str,
        partition: Option<u32>,
        messages: Vec<Message>,
        acks: u8,
    ) -> Result<ProduceResult> {
        if messages.is_empty() {
            return Err(Error::InvalidArgument("empty produce batch".into()));
        }
        let wire: Vec<ProduceMessage> = messages
            .into_iter()
            .map(|m| ProduceMessage {
                key: m.key,
                value: m.value,
                timestamp_ms: m.timestamp_ms.unwrap_or(-1),
                headers: m.headers,
            })
            .collect();

        let part = partition.map(|p| p as i32).unwrap_or(-1);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let resp = self
                .round_trip(Request::Produce {
                    topic: topic.to_owned(),
                    partition: part,
                    acks,
                    messages: wire.clone(),
                })
                .await?;

            match resp {
                Response::Produce {
                    topic,
                    partition,
                    base_offset,
                    count,
                    error_code,
                } => {
                    if error_code == ErrorCode::NotLeaderForPartition as u16 && attempt < 2 {
                        // Refresh metadata and retry once against the same connection
                        // (caller should reconnect to the leader if multi-broker).
                        let _ = self.metadata().await;
                        if part < 0 {
                            // keep broker-side assignment
                        }
                        continue;
                    }
                    check_ok(error_code, "produce")?;
                    return Ok(ProduceResult {
                        topic,
                        partition,
                        base_offset,
                        count,
                    });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotLeaderForPartition as u16 && attempt < 2 {
                        let _ = self.metadata().await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for produce: {other:?}"
                    )))
                }
            }
        }
    }

    /// Fetch records from a partition.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
    ) -> Result<FetchResult> {
        let resp = self
            .round_trip(Request::Fetch {
                topic: topic.to_owned(),
                partition,
                from_offset: from.raw(),
                max_messages,
                max_bytes: 4 * 1024 * 1024,
                max_wait_ms,
            })
            .await?;

        match resp {
            Response::Fetch {
                topic,
                partition,
                high_watermark,
                error_code,
                records,
            } => {
                check_ok(error_code, "fetch")?;
                Ok(FetchResult {
                    topic,
                    partition,
                    high_watermark,
                    records,
                })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for fetch: {other:?}"
            ))),
        }
    }

    /// Join a consumer group; returns generation, member id, and assignment.
    pub async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: u32,
        topics: Vec<String>,
    ) -> Result<JoinGroupResult> {
        let resp = self
            .round_trip(Request::JoinGroup {
                group_id: group_id.to_owned(),
                member_id: member_id.to_owned(),
                session_timeout_ms,
                topics,
            })
            .await?;
        match resp {
            Response::JoinGroup {
                error_code,
                generation,
                member_id,
                assignment,
            } => {
                check_ok(error_code, "join_group")?;
                Ok(JoinGroupResult {
                    generation,
                    member_id,
                    assignment,
                })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for join_group: {other:?}"
            ))),
        }
    }

    /// Heartbeat for group membership. Returns `true` if rebalance is required.
    pub async fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
    ) -> Result<HeartbeatResult> {
        let resp = self
            .round_trip(Request::Heartbeat {
                group_id: group_id.to_owned(),
                member_id: member_id.to_owned(),
                generation,
            })
            .await?;
        match resp {
            Response::Heartbeat { error_code } => Ok(HeartbeatResult { error_code }),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for heartbeat: {other:?}"
            ))),
        }
    }

    /// Leave a consumer group.
    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let resp = self
            .round_trip(Request::LeaveGroup {
                group_id: group_id.to_owned(),
                member_id: member_id.to_owned(),
            })
            .await?;
        match resp {
            Response::LeaveGroup { error_code } => {
                check_ok(error_code, "leave_group")?;
                Ok(())
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for leave_group: {other:?}"
            ))),
        }
    }

    /// Commit offsets for a consumer group.
    ///
    /// Pass `generation = 0` for admin/CLI commits that skip generation checks.
    pub async fn commit_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        entries: Vec<OffsetCommitEntry>,
    ) -> Result<()> {
        let resp = self
            .round_trip(Request::OffsetCommit {
                group_id: group_id.to_owned(),
                member_id: member_id.to_owned(),
                generation,
                entries,
            })
            .await?;
        match resp {
            Response::OffsetCommit { error_code } => {
                check_ok(error_code, "commit_offsets")?;
                Ok(())
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for commit_offsets: {other:?}"
            ))),
        }
    }

    /// Fetch committed offsets. Empty `entries` returns all offsets for the group.
    pub async fn fetch_offsets(
        &self,
        group_id: &str,
        entries: Vec<OffsetEntry>,
    ) -> Result<Vec<OffsetFetchEntry>> {
        let resp = self
            .round_trip(Request::OffsetFetch {
                group_id: group_id.to_owned(),
                entries,
            })
            .await?;
        match resp {
            Response::OffsetFetch {
                error_code,
                entries,
            } => {
                check_ok(error_code, "fetch_offsets")?;
                Ok(entries)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for fetch_offsets: {other:?}"
            ))),
        }
    }

    async fn round_trip(&self, req: Request) -> Result<Response> {
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let frame = pack_request(corr, &req)?;
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out)?;

        let mut stream = self.stream.lock().await;
        stream.write_all(&out).await?;

        let mut buf = BytesMut::with_capacity(8 * 1024);
        loop {
            if let Some(resp_frame) = decode_frame(&mut buf)? {
                if resp_frame.header.correlation_id != corr {
                    return Err(Error::Protocol(format!(
                        "correlation mismatch: sent {corr}, got {}",
                        resp_frame.header.correlation_id
                    )));
                }
                return decode_response(resp_frame.header.opcode, &resp_frame.payload);
            }
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed while waiting for response",
                )));
            }
        }
    }
}

fn check_ok(error_code: u16, op: &str) -> Result<()> {
    if error_code == 0 {
        Ok(())
    } else {
        Err(error_from_code(
            error_code,
            format!("{op} failed with error_code={error_code}"),
        ))
    }
}

fn error_from_code(code: u16, message: impl Into<String>) -> Error {
    let message = message.into();
    match ErrorCode::from_u16(code) {
        ErrorCode::NotFound => Error::NotFound(message),
        ErrorCode::InvalidArg => Error::InvalidArgument(message),
        ErrorCode::Storage => Error::Storage(message),
        ErrorCode::Protocol => Error::Protocol(message),
        ErrorCode::Io => Error::Io(std::io::Error::new(std::io::ErrorKind::Other, message)),
        ErrorCode::Unsupported => Error::NotImplemented("unsupported operation"),
        ErrorCode::Timeout => Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, message)),
        ErrorCode::RebalanceInProgress
        | ErrorCode::UnknownMemberId
        | ErrorCode::IllegalGeneration
        | ErrorCode::InconsistentGroupProtocol => Error::Protocol(message),
        ErrorCode::NotLeaderForPartition
        | ErrorCode::NotController
        | ErrorCode::NotEnoughReplicas
        | ErrorCode::BrokerNotAvailable => Error::Protocol(message),
        ErrorCode::Ok | ErrorCode::Unknown => Error::Protocol(message),
    }
}

/// Convenience: produce a single raw value.
pub async fn produce_value(
    client: &Client,
    topic: &str,
    partition: Option<u32>,
    key: Option<Bytes>,
    value: Bytes,
) -> Result<ProduceResult> {
    let mut msg = Message::from_value(value);
    msg.key = key;
    client.produce(topic, partition, vec![msg]).await
}
