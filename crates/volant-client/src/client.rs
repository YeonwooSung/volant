//! Networked async client for Volant brokers.
//!
//! v0.155: [`Client::delete_records`] uses
//! [`crate::ClientConfig::delete_records_wait`] (default 0).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tracing::debug;
use volant_core::{Error, Message, Offset, Result, TopicId};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{
    decode_response, pack_request, Assignment, BrokerInfo, ErrorCode, FetchRecord, GroupListing,
    GroupMemberInfo, MembershipBroker, OffsetCommitEntry, OffsetEntry, OffsetFetchEntry,
    ProduceMessage, Request, Response, TopicInfo, TxnOffsetCommit, TxnProduceResult,
};

use crate::config::ClientConfig;
use crate::conn::ClientConn;

/// In-memory idempotent producer state (Phase 10/18).
#[derive(Debug, Default)]
struct IdempotentState {
    producer_id: u64,
    epoch: u16,
    initialized: bool,
    /// True after BeginTxn until EndTxn (Phase 18).
    in_transaction: bool,
    /// `next_seq` snapshot at BeginTxn (restored on abort).
    seq_at_begin: HashMap<(String, u32), i32>,
    /// Next base_sequence per (topic, partition).
    next_seq: HashMap<(String, u32), i32>,
}

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
    /// Controller node id from the Metadata trailer (v0.77).
    /// `0` means unknown / single-node / no openraft leader.
    pub controller_id: u32,
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
    /// Partitions this member lost vs prior assignment (Phase 17).
    /// May be empty when the broker cannot observe the prior list; clients
    /// should also diff local old vs new assignment.
    pub revoked: Vec<Assignment>,
}

/// Result of a Heartbeat call.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// Embedded error code (`0` ok; `9` rebalance in progress).
    pub error_code: u16,
}

/// Result of a successful DescribeGroup (Phase 11).
#[derive(Debug, Clone)]
pub struct DescribeGroupResult {
    /// Group id.
    pub group_id: String,
    /// Current generation.
    pub generation: u32,
    /// Live members and assignments.
    pub members: Vec<GroupMemberInfo>,
}

/// Result of DeleteOffsets (Phase 12).
#[derive(Debug, Clone)]
pub struct DeleteOffsetsResult {
    /// Number of offset files removed.
    pub deleted_count: u32,
}

/// Result of DescribeConfigs (Phase 13).
#[derive(Debug, Clone)]
pub struct DescribeConfigsResult {
    /// Topic name.
    pub topic: String,
    /// Topic id.
    pub topic_id: u32,
    /// Partition count.
    pub partition_count: u32,
    /// Config key/value pairs (empty value = unset).
    pub configs: Vec<(String, String)>,
}

/// Cluster membership listing (v0.10).
#[derive(Debug, Clone)]
pub struct MembershipList {
    /// Overlay generation (`0` if toml-only).
    pub generation: u64,
    /// Effective configured brokers.
    pub brokers: Vec<MembershipBroker>,
    /// Live broker ids.
    pub live: Vec<u32>,
}

/// Result of DeleteRecords (Phase 14).
#[derive(Debug, Clone)]
pub struct DeleteRecordsResult {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// New log start offset after deletion.
    pub low_watermark: u64,
}

/// One partition offset range from ListOffsets (Phase 15).
#[derive(Debug, Clone)]
pub struct PartitionOffsets {
    /// Partition id.
    pub partition: u32,
    /// Log start offset.
    pub earliest: u64,
    /// Log end offset (next write).
    pub latest: u64,
}

/// Result of ListOffsets (Phase 15).
#[derive(Debug, Clone)]
pub struct ListOffsetsResult {
    /// Topic name.
    pub topic: String,
    /// Per-partition ranges.
    pub entries: Vec<PartitionOffsets>,
}

impl HeartbeatResult {
    /// Whether the client should re-join the group.
    pub fn needs_rebalance(&self) -> bool {
        self.error_code == ErrorCode::RebalanceInProgress as u16
            || self.error_code == ErrorCode::IllegalGeneration as u16
            || self.error_code == ErrorCode::UnknownMemberId as u16
    }
}

/// Async client (sequential request/response over one connection).
///
/// Supports optional shared-token auth, optional SCRAM-SHA-256 (Phase 22),
/// optional TLS (`tls` feature), automatic reconnect to the partition leader
/// on `NotLeaderForPartition`, controller redirect on `NotController` (v0.79),
/// and optional idempotent produce with retries (Phase 10).
#[derive(Debug)]
pub struct Client {
    stream: Mutex<ClientConn>,
    current_addr: Mutex<String>,
    next_corr: AtomicU32,
    config: ClientConfig,
    idempotent: Mutex<IdempotentState>,
}

impl Client {
    /// Connect to the first configured broker address.
    ///
    /// When [`ClientConfig::auth_token`] is set, sends Auth. When
    /// [`ClientConfig::scram_username`] / [`ClientConfig::scram_password`] are
    /// set, runs SCRAM-SHA-256. Token is preferred when both are set.
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let addr = config
            .brokers
            .first()
            .ok_or_else(|| Error::InvalidArgument("no brokers configured".into()))?
            .clone();
        let stream = ClientConn::connect(&addr, &config).await?;
        let client = Self {
            stream: Mutex::new(stream),
            current_addr: Mutex::new(addr),
            next_corr: AtomicU32::new(1),
            config,
            idempotent: Mutex::new(IdempotentState::default()),
        };
        client.maybe_authenticate().await?;
        Ok(client)
    }

    /// Connect to a single `host:port`.
    pub async fn connect_addr(addr: impl AsRef<str>) -> Result<Self> {
        Self::connect(ClientConfig {
            brokers: vec![addr.as_ref().to_owned()],
            ..ClientConfig::default()
        })
        .await
    }

    /// Connect with an explicit shared auth token.
    pub async fn connect_with_auth(
        addr: impl AsRef<str>,
        auth_token: impl Into<String>,
    ) -> Result<Self> {
        Self::connect(ClientConfig {
            brokers: vec![addr.as_ref().to_owned()],
            auth_token: Some(auth_token.into()),
            ..ClientConfig::default()
        })
        .await
    }

    /// Address currently connected to (`host:port`).
    pub async fn current_addr(&self) -> String {
        self.current_addr.lock().await.clone()
    }

    /// Reconnect to `addr`, re-authenticating when a token or SCRAM is configured.
    pub async fn reconnect(&self, addr: impl AsRef<str>) -> Result<()> {
        let addr = addr.as_ref().to_owned();
        let stream = ClientConn::connect(&addr, &self.config).await?;
        {
            let mut guard = self.stream.lock().await;
            *guard = stream;
        }
        {
            let mut a = self.current_addr.lock().await;
            *a = addr;
        }
        self.maybe_authenticate().await?;
        Ok(())
    }

    async fn maybe_authenticate(&self) -> Result<()> {
        if let Some(token) = self.config.auth_token.clone() {
            return self.authenticate(token).await;
        }
        match (
            self.config.scram_username.clone(),
            self.config.scram_password.clone(),
        ) {
            (Some(user), Some(pass)) => self.authenticate_scram(&user, &pass).await,
            (None, None) => Ok(()),
            _ => Err(Error::InvalidArgument(
                "scram_username and scram_password must both be set".into(),
            )),
        }
    }

    /// Shared-token Auth on connect / reconnect.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error 17 /
    /// 18 (auth failed / required), 13 / 14 / 9 / 10 / 11 / 2 / 21 / 22,
    /// Protocol, and InvalidArgument are not retried. SCRAM
    /// (`authenticate_scram`) is unchanged. Each call has its own retry
    /// budget.
    async fn authenticate(&self, token: String) -> Result<()> {
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::Auth {
                    token: token.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::Auth { error_code } => {
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    if error_code == 0 {
                        return Ok(());
                    }
                    return Err(error_from_code(
                        error_code,
                        format!("auth failed with error_code={error_code}"),
                    ));
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for auth: {other:?}"
                    )))
                }
            }
        }
    }

    /// Run SCRAM-SHA-256 first+final as one handshake.
    ///
    /// Transient broker/transport errors on either step retry the whole
    /// handshake from ScramFirst with a new client nonce, up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error 17 / 18 /
    /// 13 / 14 / 9 / 10 / 11 / 2 / 21 / 22, protocol (including server
    /// signature mismatch), and InvalidArgument are not retried.
    async fn authenticate_scram(&self, username: &str, password: &str) -> Result<()> {
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let client_nonce = crate::scram::generate_client_nonce();
            let first = match self
                .round_trip(Request::ScramFirst {
                    username: username.to_owned(),
                    client_nonce: client_nonce.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (combined_nonce, salt, iterations) = match first {
                Response::ScramFirst {
                    error_code,
                    combined_nonce,
                    salt,
                    iterations,
                } => {
                    if error_code != 0 {
                        if is_transient_error_code(error_code) && retry_attempt < max_retries {
                            retry_attempt += 1;
                            tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                                .await;
                            continue;
                        }
                        return Err(error_from_code(
                            error_code,
                            format!("scram first failed error_code={error_code}"),
                        ));
                    }
                    (combined_nonce, salt, iterations)
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for scram first: {other:?}"
                    )))
                }
            };

            let (proof, expected_sig) = crate::scram::client_proof_and_server_sig(
                username,
                password,
                &client_nonce,
                &combined_nonce,
                &salt,
                iterations,
            )?;

            let final_resp = match self
                .round_trip(Request::ScramFinal {
                    username: username.to_owned(),
                    combined_nonce,
                    client_proof: bytes::Bytes::from(proof),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match final_resp {
                Response::ScramFinal {
                    error_code,
                    server_signature,
                } => {
                    if error_code != 0 {
                        if is_transient_error_code(error_code) && retry_attempt < max_retries {
                            retry_attempt += 1;
                            tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                                .await;
                            continue;
                        }
                        return Err(error_from_code(
                            error_code,
                            format!("scram final failed error_code={error_code}"),
                        ));
                    }
                    if server_signature.as_ref() != expected_sig.as_slice() {
                        return Err(Error::Protocol("scram server signature mismatch".into()));
                    }
                    return Ok(());
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for scram final: {other:?}"
                    )))
                }
            }
        }
    }

    /// Create or replace a SCRAM user (Phase 22). Bootstrap allowed when store empty.
    pub async fn create_scram_user(
        &self,
        username: &str,
        password: &str,
        iterations: u32,
    ) -> Result<()> {
        let resp = self
            .admin_round_trip(Request::CreateScramUser {
                username: username.to_owned(),
                password: password.to_owned(),
                iterations,
            })
            .await?;
        match resp {
            Response::CreateScramUser { error_code } if error_code == 0 => Ok(()),
            Response::CreateScramUser { error_code } => Err(error_from_code(
                error_code,
                format!("create_scram_user failed error_code={error_code}"),
            )),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for create_scram_user: {other:?}"
            ))),
        }
    }

    /// Delete a SCRAM user (Phase 22).
    pub async fn delete_scram_user(&self, username: &str) -> Result<()> {
        let resp = self
            .admin_round_trip(Request::DeleteScramUser {
                username: username.to_owned(),
            })
            .await?;
        match resp {
            Response::DeleteScramUser { error_code } if error_code == 0 => Ok(()),
            Response::DeleteScramUser { error_code } => Err(error_from_code(
                error_code,
                format!("delete_scram_user failed error_code={error_code}"),
            )),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for delete_scram_user: {other:?}"
            ))),
        }
    }

    /// List SCRAM usernames (Phase 22).
    pub async fn list_scram_users(&self) -> Result<Vec<String>> {
        let resp = self.admin_round_trip(Request::ListScramUsers).await?;
        match resp {
            Response::ListScramUsers {
                error_code,
                usernames,
            } if error_code == 0 => Ok(usernames),
            Response::ListScramUsers { error_code, .. } => Err(error_from_code(
                error_code,
                format!("list_scram_users failed error_code={error_code}"),
            )),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for list_scram_users: {other:?}"
            ))),
        }
    }

    /// Create a topic; returns assigned topic id.
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<TopicId> {
        self.create_topic_with_configs(name, partitions, vec![])
            .await
    }

    /// Create a topic with optional configs (Phase 13).
    pub async fn create_topic_with_configs(
        &self,
        name: &str,
        partitions: u32,
        configs: Vec<(String, String)>,
    ) -> Result<TopicId> {
        let resp = self
            .admin_round_trip(Request::CreateTopic {
                name: name.to_owned(),
                partitions,
                configs,
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

    /// Describe topic configuration (Phase 13).
    pub async fn describe_configs(&self, topic: &str) -> Result<DescribeConfigsResult> {
        let resp = self
            .admin_round_trip(Request::DescribeConfigs {
                topic: topic.to_owned(),
            })
            .await?;
        match resp {
            Response::DescribeConfigs {
                error_code,
                topic,
                topic_id,
                partition_count,
                configs,
            } => {
                check_ok(error_code, "describe_configs")?;
                Ok(DescribeConfigsResult {
                    topic,
                    topic_id,
                    partition_count,
                    configs,
                })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for describe_configs: {other:?}"
            ))),
        }
    }

    /// Alter topic configuration (Phase 13). Empty value clears a key.
    pub async fn alter_configs(&self, topic: &str, configs: Vec<(String, String)>) -> Result<()> {
        let resp = self
            .admin_round_trip(Request::AlterConfigs {
                topic: topic.to_owned(),
                configs,
            })
            .await?;
        match resp {
            Response::AlterConfigs { error_code, .. } => {
                check_ok(error_code, "alter_configs")?;
                Ok(())
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for alter_configs: {other:?}"
            ))),
        }
    }

    /// Delete records before `before_offset` on a partition (Phase 14).
    ///
    /// Returns the new log start offset (low watermark).
    /// Sends `wait_majority` from [`ClientConfig::delete_records_wait`]
    /// (default 0 = broker default; Phase 137). Use
    /// [`Self::delete_records_with_wait_flag`] for an explicit flag.
    /// Inherits error-13 redirect and transient retry from
    /// [`Self::delete_records_with_wait_flag`].
    pub async fn delete_records(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
    ) -> Result<DeleteRecordsResult> {
        self.delete_records_with_wait_flag(
            topic,
            partition,
            before_offset,
            self.config.delete_records_wait,
        )
        .await
    }

    /// Delete records with Phase 137 majority-wait trailer.
    ///
    /// `wait_majority`: 0 = broker default, 1 = force wait, 2 = force no-wait.
    /// Error **13** (`NotLeaderForPartition`) uses
    /// [`ClientConfig::max_redirects`] via [`Self::redirect_to_leader`]
    /// (independent of retry; default 1 extra; `0` does not redirect).
    /// Transient 6 / 7 / 15 / 16 and [`Error::Io`] retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). 14 / 9 /
    /// 10 / 11 / 2 / 17 / 18 / 21 / 22 and protocol are not retried.
    /// [`Self::delete_records`] inherits.
    pub async fn delete_records_with_wait_flag(
        &self,
        topic: &str,
        partition: u32,
        before_offset: u64,
        wait_majority: u8,
    ) -> Result<DeleteRecordsResult> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        let req = Request::DeleteRecords {
            topic: topic.to_owned(),
            partition,
            before_offset,
            wait_majority,
        };
        loop {
            let resp = match self.round_trip(req.clone()).await {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::DeleteRecords {
                    error_code,
                    topic: t,
                    partition: p,
                    low_watermark,
                } => {
                    if error_code == ErrorCode::NotLeaderForPartition as u16
                        && redirects < max_redirects
                    {
                        let before = self.current_addr().await;
                        if self.redirect_to_leader(&t, p).await.is_ok()
                            && self.current_addr().await != before
                        {
                            redirects += 1;
                            continue;
                        }
                    }
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "delete_records")?;
                    return Ok(DeleteRecordsResult {
                        topic: t,
                        partition: p,
                        low_watermark,
                    });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotLeaderForPartition as u16 && redirects < max_redirects
                    {
                        let before = self.current_addr().await;
                        if self.redirect_to_leader(topic, partition).await.is_ok()
                            && self.current_addr().await != before
                        {
                            redirects += 1;
                            continue;
                        }
                    }
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for delete_records: {other:?}"
                    )))
                }
            }
        }
    }

    /// Increase topic partition count to `total_count` (Phase 15).
    pub async fn create_partitions(&self, topic: &str, total_count: u32) -> Result<u32> {
        let resp = self
            .admin_round_trip(Request::CreatePartitions {
                topic: topic.to_owned(),
                total_count,
            })
            .await?;
        match resp {
            Response::CreatePartitions {
                error_code,
                partitions,
                ..
            } => {
                check_ok(error_code, "create_partitions")?;
                Ok(partitions)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for create_partitions: {other:?}"
            ))),
        }
    }

    /// List earliest/latest offsets for a topic (Phase 15).
    ///
    /// Empty `partitions` means all partitions.
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0).
    /// Error **13** (`NotLeaderForPartition`) uses
    /// [`ClientConfig::max_redirects`] via [`Self::redirect_to_leader`]
    /// (independent of retry). `max_redirects=0` does not redirect.
    /// 14 / 2 / 9 / 10 / 11 / 17 / 18 / 21 / 22 and protocol are not
    /// redirected.
    pub async fn list_offsets(
        &self,
        topic: &str,
        partitions: Vec<u32>,
    ) -> Result<ListOffsetsResult> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirect_attempt = 0u32;
        let redirect_part = partitions.first().copied().unwrap_or(0);
        loop {
            let resp = match self
                .round_trip(Request::ListOffsets {
                    topic: topic.to_owned(),
                    partitions: partitions.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::ListOffsets {
                    error_code,
                    topic: resp_topic,
                    entries,
                } => {
                    if error_code == ErrorCode::NotLeaderForPartition as u16
                        && redirect_attempt + 1 < 1 + max_redirects
                        && self.redirect_to_leader(topic, redirect_part).await.is_ok()
                    {
                        redirect_attempt += 1;
                        continue;
                    }
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "list_offsets")?;
                    return Ok(ListOffsetsResult {
                        topic: resp_topic,
                        entries: entries
                            .into_iter()
                            .map(|e| PartitionOffsets {
                                partition: e.partition,
                                earliest: e.earliest,
                                latest: e.latest,
                            })
                            .collect(),
                    });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotLeaderForPartition as u16
                        && redirect_attempt + 1 < 1 + max_redirects
                        && self.redirect_to_leader(topic, redirect_part).await.is_ok()
                    {
                        redirect_attempt += 1;
                        continue;
                    }
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for list_offsets: {other:?}"
                    )))
                }
            }
        }
    }

    /// List earliest/latest offsets for every partition of `topic`
    /// (empty wire partitions). Same as `list_offsets(topic, vec![])`.
    pub async fn list_offsets_all(&self, topic: &str) -> Result<ListOffsetsResult> {
        self.list_offsets(topic, Vec::new()).await
    }

    /// Delete a topic.
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let resp = self
            .admin_round_trip(Request::DeleteTopic {
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
    ///
    /// Sends an empty native Metadata `topics` list (all topics).
    /// Same as [`Self::metadata_topics`] with `Vec::new()`.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Native
    /// Metadata has no top-level error_code; failures arrive as
    /// [`Response::Error`] or transport. Error **14** (`NotController`)
    /// uses [`ClientConfig::max_redirects`] via
    /// [`Self::redirect_to_controller`] (independent of retry) and
    /// does not increment `retry_attempt`. `max_redirects=0` does not
    /// redirect. Error 2 / 9 / 10 / 11 / 13 / 17 / 18 / 21 / 22 and
    /// protocol errors are not retried or redirected.
    pub async fn metadata(&self) -> Result<Metadata> {
        self.metadata_topics(Vec::new()).await
    }

    /// Fetch cluster metadata for the named topics.
    ///
    /// Empty `topics` means all topics (same as [`Self::metadata`]).
    /// Same decode, retry, redirect, and error handling as
    /// [`Self::metadata`]. This is the native Metadata `topics` list,
    /// not Kafka `allow_auto_topic_creation` / topic ids.
    pub async fn metadata_topics(&self, topics: Vec<String>) -> Result<Metadata> {
        let max_redirects = self.config.max_redirects;
        let mut redirects = 0u32;
        loop {
            let resp = self
                .metadata_list_members_round_trip(Request::Metadata {
                    topics: topics.clone(),
                })
                .await?;
            match &resp {
                Response::Error { code, message }
                    if *code == ErrorCode::NotController as u16
                        && redirects < max_redirects
                        && self
                            .redirect_to_controller(parse_controller_id(message))
                            .await =>
                {
                    redirects += 1;
                    continue;
                }
                _ => return metadata_from_response(resp),
            }
        }
    }

    /// Metadata without the v0.157 error-14 wrap. Used by
    /// [`Self::redirect_to_controller`] so hunt and `metadata` /
    /// `metadata_topics` are not mutually recursive. Transient retry
    /// is still v0.96.
    async fn metadata_rpc(&self, topics: Vec<String>) -> Result<Metadata> {
        let resp = self
            .metadata_list_members_round_trip(Request::Metadata { topics })
            .await?;
        metadata_from_response(resp)
    }

    /// Produce messages to a topic (default acks from config, usually `1`).
    ///
    /// `partition = None` asks the broker to assign (key-hash / round-robin).
    /// On `NotLeaderForPartition`, reconnects to the leader and retries.
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
    ///
    /// When [`ClientConfig::enable_idempotence`] is set, resolves the partition
    /// client-side, attaches producer id/sequence, and de-dupes retries safely.
    /// Transient errors are retried up to [`ClientConfig::max_retries`] times.
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

        let idempotent = self.config.enable_idempotence || self.config.transactional_id.is_some();

        // For idempotence / transactions, pin partition before sequencing.
        let mut part = if idempotent {
            let p = match partition {
                Some(p) => p,
                None => {
                    self.resolve_partition(topic, wire[0].key.as_deref())
                        .await?
                }
            };
            p as i32
        } else {
            partition.map(|p| p as i32).unwrap_or(-1)
        };

        let (producer_id, producer_epoch, base_sequence) = if idempotent {
            self.ensure_producer_id().await?;
            let state = self.idempotent.lock().await;
            let key = (topic.to_owned(), part as u32);
            let seq = *state.next_seq.get(&key).unwrap_or(&0);
            (state.producer_id, state.epoch, seq)
        } else {
            (0, 0, -1)
        };

        // Redirect attempts: 1 initial + max_redirects extras (max_redirects=0 → no redirect).
        let max_redirect_attempts = 1 + self.config.max_redirects;
        let max_retries = self.config.max_retries;
        let mut redirect_attempt = 0u32;
        let mut retry_attempt = 0u32;

        loop {
            redirect_attempt += 1;
            let resp = match self
                .round_trip(Request::Produce {
                    topic: topic.to_owned(),
                    partition: part,
                    acks,
                    messages: wire.clone(),
                    producer_id,
                    producer_epoch,
                    base_sequence,
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    redirect_attempt = redirect_attempt.saturating_sub(1);
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };

            match resp {
                Response::Produce {
                    topic: t,
                    partition: p,
                    base_offset,
                    count,
                    error_code,
                } => {
                    if error_code == ErrorCode::NotLeaderForPartition as u16
                        && redirect_attempt < max_redirect_attempts
                    {
                        part = p as i32;
                        self.redirect_to_leader(&t, p).await?;
                        continue;
                    }
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        redirect_attempt = redirect_attempt.saturating_sub(1);
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "produce")?;
                    if idempotent && base_sequence >= 0 {
                        let mut state = self.idempotent.lock().await;
                        let key = (t.clone(), p);
                        let next = base_sequence.saturating_add(count as i32);
                        state.next_seq.insert(key, next);
                    }
                    return Ok(ProduceResult {
                        topic: t,
                        partition: p,
                        base_offset,
                        count,
                    });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotLeaderForPartition as u16
                        && redirect_attempt < max_redirect_attempts
                    {
                        if part >= 0 {
                            self.redirect_to_leader(topic, part as u32).await?;
                        } else {
                            let _ = self.metadata().await;
                        }
                        continue;
                    }
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        redirect_attempt = redirect_attempt.saturating_sub(1);
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
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

    /// Ensure InitProducerId has been called when idempotence/transactions enabled.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error 13 / 14 /
    /// 9 / 10 / 11 / 2, protocol errors, and UnknownProducerId (21) on Init
    /// itself are not retried. Produce's one-shot unknown-pid re-Init is
    /// unchanged. Already-initialized clients return immediately.
    async fn ensure_producer_id(&self) -> Result<()> {
        {
            let state = self.idempotent.lock().await;
            if state.initialized {
                return Ok(());
            }
        }
        let transactional_id = self.config.transactional_id.clone().unwrap_or_default();
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::InitProducerId {
                    transactional_id: transactional_id.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::InitProducerId {
                    producer_id,
                    epoch,
                    error_code,
                } => {
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "init_producer_id")?;
                    let mut state = self.idempotent.lock().await;
                    state.producer_id = producer_id;
                    state.epoch = epoch;
                    state.initialized = true;
                    state.in_transaction = false;
                    state.next_seq.clear();
                    return Ok(());
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for init_producer_id: {other:?}"
                    )))
                }
            }
        }
    }

    /// Ensure InitProducerId has run (native opcode 32).
    /// Returns the stored producer id and epoch. A second call is a no-op
    /// (already initialized). Produce / BeginTxn still init implicitly.
    pub async fn init_producer_id(&self) -> Result<(u64, u16)> {
        self.ensure_producer_id().await?;
        let state = self.idempotent.lock().await;
        Ok((state.producer_id, state.epoch))
    }

    /// Stored producer id (0 until [`Self::init_producer_id`] or implicit init).
    /// Does not call Init.
    pub async fn producer_id(&self) -> u64 {
        self.idempotent.lock().await.producer_id
    }

    /// Stored producer epoch (0 until [`Self::init_producer_id`] or implicit init).
    /// Does not call Init.
    pub async fn producer_epoch(&self) -> u16 {
        self.idempotent.lock().await.epoch
    }

    /// Begin a transaction (Phase 18). Requires `transactional_id` in config.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). InvalidTxnState
    /// (22), fence / epoch / abortable codes, 13 / 14 / 9 / 10 / 11 / 2,
    /// and protocol errors are not retried. [`crate::TransactionalProducer::begin`]
    /// inherits via this method.
    pub async fn begin_transaction(&self) -> Result<()> {
        if self.config.transactional_id.is_none() {
            return Err(Error::InvalidArgument(
                "transactional_id not configured".into(),
            ));
        }
        self.ensure_producer_id().await?;
        let (producer_id, producer_epoch) = {
            let state = self.idempotent.lock().await;
            (state.producer_id, state.epoch)
        };
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::BeginTxn {
                    producer_id,
                    producer_epoch,
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::BeginTxn { error_code } => {
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "begin_txn")?;
                    let mut state = self.idempotent.lock().await;
                    state.seq_at_begin = state.next_seq.clone();
                    state.in_transaction = true;
                    return Ok(());
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for begin_txn: {other:?}"
                    )))
                }
            }
        }
    }

    /// Commit the open transaction (Phase 18).
    ///
    /// Returns final log offsets for each buffered produce batch.
    pub async fn commit_transaction(
        &self,
        offsets: Vec<TxnOffsetCommit>,
    ) -> Result<Vec<TxnProduceResult>> {
        self.end_transaction(true, offsets).await
    }

    /// Abort the open transaction (Phase 18).
    pub async fn abort_transaction(&self) -> Result<()> {
        let _ = self.end_transaction(false, Vec::new()).await?;
        Ok(())
    }

    async fn end_transaction(
        &self,
        committed: bool,
        offsets: Vec<TxnOffsetCommit>,
    ) -> Result<Vec<TxnProduceResult>> {
        let (producer_id, producer_epoch) = {
            let state = self.idempotent.lock().await;
            if !state.initialized {
                return Err(Error::InvalidArgument("producer id not initialized".into()));
            }
            (state.producer_id, state.epoch)
        };
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::EndTxn {
                    producer_id,
                    producer_epoch,
                    committed,
                    offsets: offsets.clone(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::EndTxn {
                    error_code,
                    results,
                } => {
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "end_txn")?;
                    let mut state = self.idempotent.lock().await;
                    state.in_transaction = false;
                    if !committed {
                        // Broker discarded pending sequences; rewind client counters.
                        state.next_seq = state.seq_at_begin.clone();
                    }
                    state.seq_at_begin.clear();
                    return Ok(results);
                }
                Response::Error { code, message } => {
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for end_txn: {other:?}"
                    )))
                }
            }
        }
    }

    /// Resolve partition via metadata (key murmur2 or round-robin using partition 0).
    async fn resolve_partition(&self, topic: &str, key: Option<&[u8]>) -> Result<u32> {
        let meta = self.metadata().await?;
        let tinfo = meta
            .topics
            .iter()
            .find(|t| t.name == topic)
            .ok_or_else(|| Error::NotFound(format!("topic '{topic}' not found in metadata")))?;
        let n = tinfo.partitions.len() as u32;
        if n == 0 {
            return Err(Error::NotFound(format!(
                "topic '{topic}' has no partitions"
            )));
        }
        let p = match key {
            Some(k) => volant_broker_partition_for_key(k, n),
            None => {
                // Stable default: partition 0 when RR state is not shared client-side.
                0
            }
        };
        Ok(p)
    }

    /// Default Fetch `max_bytes` (4 MiB). Default for
    /// [`ClientConfig::fetch_max_bytes`].
    pub(crate) const DEFAULT_FETCH_MAX_BYTES: u32 = 4 * 1024 * 1024;

    /// Fetch records from a partition.
    ///
    /// On `NotLeaderForPartition`, reconnects to the leader and retries.
    /// `max_bytes` comes from [`ClientConfig::fetch_max_bytes`] (default 4 MiB);
    /// use [`Self::fetch_opts`] to set it per call.
    /// For all [`ClientConfig`] fetch knobs, use [`Self::fetch_default`].
    pub async fn fetch(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
    ) -> Result<FetchResult> {
        self.fetch_opts(
            topic,
            partition,
            from,
            max_messages,
            max_wait_ms,
            self.config.fetch_max_bytes,
        )
        .await
    }

    /// Fetch using [`ClientConfig`] `fetch_max_messages` / `fetch_max_wait_ms` /
    /// `fetch_max_bytes` (defaults 128 / 0 / 4 MiB).
    ///
    /// [`Self::fetch`] still requires explicit `max_messages` / `max_wait_ms`
    /// and uses [`ClientConfig::fetch_max_bytes`]. GroupConsumer poll knobs
    /// stay historical (v0.76; 100 / 4 MiB).
    pub async fn fetch_default(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
    ) -> Result<FetchResult> {
        self.fetch_opts(
            topic,
            partition,
            from,
            self.config.fetch_max_messages,
            self.config.fetch_max_wait_ms,
            self.config.fetch_max_bytes,
        )
        .await
    }

    /// Fetch records from a partition with an explicit `max_bytes`.
    ///
    /// Same leader-redirect behaviour as [`Self::fetch`].
    pub async fn fetch_opts(
        &self,
        topic: &str,
        partition: u32,
        from: Offset,
        max_messages: u32,
        max_wait_ms: u32,
        max_bytes: u32,
    ) -> Result<FetchResult> {
        let max_attempts = 1 + self.config.max_redirects;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let resp = self
                .round_trip(Request::Fetch {
                    topic: topic.to_owned(),
                    partition,
                    from_offset: from.raw(),
                    max_messages,
                    max_bytes,
                    max_wait_ms,
                })
                .await?;

            match resp {
                Response::Fetch {
                    topic: t,
                    partition: p,
                    high_watermark,
                    error_code,
                    records,
                } => {
                    if error_code == ErrorCode::NotLeaderForPartition as u16
                        && attempt < max_attempts
                    {
                        self.redirect_to_leader(&t, p).await?;
                        continue;
                    }
                    check_ok(error_code, "fetch")?;
                    return Ok(FetchResult {
                        topic: t,
                        partition: p,
                        high_watermark,
                        records,
                    });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotLeaderForPartition as u16 && attempt < max_attempts {
                        self.redirect_to_leader(topic, partition).await?;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for fetch: {other:?}"
                    )))
                }
            }
        }
    }

    /// Resolve leader for `topic`/`partition` via Metadata and reconnect.
    async fn redirect_to_leader(&self, topic: &str, partition: u32) -> Result<()> {
        let meta = self.metadata().await?;
        let leader_id = meta
            .topics
            .iter()
            .find(|t| t.name == topic)
            .and_then(|t| t.partitions.iter().find(|p| p.partition_id == partition))
            .map(|p| p.leader)
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "metadata missing topic={topic} partition={partition} for redirect"
                ))
            })?;
        let broker = meta
            .brokers
            .iter()
            .find(|b| b.node_id == leader_id)
            .ok_or_else(|| {
                Error::NotFound(format!("metadata missing broker node_id={leader_id}"))
            })?;
        let addr = format!("{}:{}", broker.host, broker.port);
        let current = self.current_addr.lock().await.clone();
        if current == addr {
            debug!(%addr, "already on leader; skipping reconnect");
            return Ok(());
        }
        debug!(from = %current, to = %addr, leader_id, "redirecting to partition leader");
        self.reconnect(&addr).await
    }

    /// Metadata → reconnect to the controller (v0.79).
    ///
    /// If `controller_id` is known (parsed from `controller_id=N` in a 14 Error
    /// message, or Metadata's v0.77 trailer when non-zero), look that node up
    /// in Metadata brokers, then [`Self::list_members_rpc`] if Metadata has no
    /// matching id. Otherwise pick the first advertised broker whose host:port
    /// is not this connection.
    ///
    /// Hunt uses [`Self::metadata_rpc`] (no 14 wrap) so this helper and
    /// `metadata` / `metadata_topics` are not mutually recursive. Id miss
    /// still uses [`Self::list_members_rpc`].
    ///
    /// Returns `true` when the caller should retry. Returns `false` on no other
    /// broker / lookup miss / empty host / reconnect fail — caller must surface
    /// the original error 14.
    async fn redirect_to_controller(&self, controller_id: Option<u32>) -> bool {
        let meta = match self.metadata_rpc(Vec::new()).await {
            Ok(m) => m,
            Err(_) => return false,
        };
        let current = self.current_addr().await;
        let controller_id = controller_id.or_else(|| {
            if meta.controller_id != 0 {
                Some(meta.controller_id)
            } else {
                None
            }
        });
        let (host, port) = if let Some(id) = controller_id {
            if let Some(b) = meta.brokers.iter().find(|b| b.node_id == id) {
                (b.host.clone(), b.port)
            } else {
                // Hunt uses the no-14 path so this helper and
                // `list_members` are not mutually recursive async fns.
                let members = match self.list_members_rpc().await {
                    Ok(m) => m,
                    Err(_) => return false,
                };
                match members.brokers.iter().find(|b| b.id == id) {
                    Some(b) => (b.host.clone(), b.port),
                    None => return false,
                }
            }
        } else {
            match meta
                .brokers
                .iter()
                .find(|b| !b.host.is_empty() && format!("{}:{}", b.host, b.port) != current)
            {
                Some(b) => (b.host.clone(), b.port),
                None => return false,
            }
        };
        if host.is_empty() {
            return false;
        }
        let addr = format!("{host}:{port}");
        if addr == current {
            return true;
        }
        debug!(from = %current, to = %addr, "redirecting to controller");
        self.reconnect(&addr).await.is_ok()
    }

    /// Round-trip a controller-gated admin RPC.
    ///
    /// Error **14** (`NotController`) uses [`ClientConfig::max_redirects`]
    /// via [`Self::redirect_to_controller`] (independent of retry). Transient
    /// 6 / 7 / 15 / 16 and [`Error::Io`] retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). 13 / 9 / 10 /
    /// 11 / 2 / 21 / InvalidTxnState (22) and protocol are not retried.
    async fn admin_round_trip(&self, req: Request) -> Result<Response> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        loop {
            let resp = match self.round_trip(req.clone()).await {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (code, hint) = match &resp {
                Response::Error { code, message } => (*code, parse_controller_id(message)),
                Response::CreateTopic { error_code, .. }
                | Response::DeleteTopic { error_code, .. }
                | Response::CreatePartitions { error_code, .. }
                | Response::ReassignPartitions { error_code, .. }
                | Response::CreateAcls { error_code }
                | Response::DeleteAcls { error_code, .. }
                | Response::CreateScramUser { error_code }
                | Response::DeleteScramUser { error_code }
                | Response::ListScramUsers { error_code, .. }
                | Response::ListAcls { error_code, .. }
                | Response::AddBroker { error_code, .. }
                | Response::RemoveBroker { error_code, .. }
                | Response::DescribeConfigs { error_code, .. }
                | Response::AlterConfigs { error_code, .. } => (*error_code, None),
                _ => return Ok(resp),
            };
            if code == ErrorCode::NotController as u16
                && redirects < max_redirects
                && self.redirect_to_controller(hint).await
            {
                redirects += 1;
                continue;
            }
            if is_transient_error_code(code) && retry_attempt < max_retries {
                retry_attempt += 1;
                tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                continue;
            }
            return Ok(resp);
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
        self.join_group_with_instance(group_id, member_id, session_timeout_ms, topics, "")
            .await
    }

    /// Join with optional static membership (`group_instance_id`, Phase 12).
    pub async fn join_group_with_instance(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: u32,
        topics: Vec<String>,
        group_instance_id: &str,
    ) -> Result<JoinGroupResult> {
        let resp = self
            .round_trip(Request::JoinGroup {
                group_id: group_id.to_owned(),
                member_id: member_id.to_owned(),
                session_timeout_ms,
                topics,
                group_instance_id: group_instance_id.to_owned(),
            })
            .await?;
        match resp {
            Response::JoinGroup {
                error_code,
                generation,
                member_id,
                assignment,
                revoked,
            } => {
                check_ok(error_code, "join_group")?;
                Ok(JoinGroupResult {
                    generation,
                    member_id,
                    assignment,
                    revoked,
                })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for join_group: {other:?}"
            ))),
        }
    }

    /// Heartbeat for group membership.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error **14**
    /// (`NotController`) redirects via [`ClientConfig::max_redirects`]
    /// (default 1; `0` does not redirect) and does not increment
    /// `retry_attempt`. Rebalance codes 9 / 10 / 11 are not retried so
    /// [`crate::GroupConsumer`] can rejoin. 13 / 2 / 17 / 18 / 21 / 22
    /// and protocol errors are not retried or redirected. Non-zero
    /// typed codes are still returned as [`HeartbeatResult`] (no
    /// `check_ok`). [`crate::GroupConsumer`] poll / background
    /// heartbeat inherit.
    pub async fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
    ) -> Result<HeartbeatResult> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::Heartbeat {
                    group_id: group_id.to_owned(),
                    member_id: member_id.to_owned(),
                    generation,
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::Heartbeat { error_code } => {
                    if error_code == ErrorCode::NotController as u16
                        && redirects < max_redirects
                        && self.redirect_to_controller(None).await
                    {
                        redirects += 1;
                        continue;
                    }
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Ok(HeartbeatResult { error_code });
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::NotController as u16
                        && redirects < max_redirects
                        && self
                            .redirect_to_controller(parse_controller_id(&message))
                            .await
                    {
                        redirects += 1;
                        continue;
                    }
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for heartbeat: {other:?}"
                    )))
                }
            }
        }
    }

    /// Leave a consumer group.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error **14**
    /// (`NotController`) redirects via [`ClientConfig::max_redirects`]
    /// (default 1; `0` does not redirect) and does not increment
    /// `retry_attempt`. Error 10 (`UnknownMemberId`) is treated as
    /// success (already left) before 14 / transient handling. Rebalance
    /// 9 / 11, 13, NotFound 2, 17 / 18, 21, 22, and protocol errors are
    /// not retried or redirected. [`crate::GroupConsumer::leave`] inherits
    /// via this method.
    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        loop {
            let resp = match self
                .round_trip(Request::LeaveGroup {
                    group_id: group_id.to_owned(),
                    member_id: member_id.to_owned(),
                })
                .await
            {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            match resp {
                Response::LeaveGroup { error_code } => {
                    if error_code == ErrorCode::UnknownMemberId as u16 {
                        return Ok(());
                    }
                    if error_code == ErrorCode::NotController as u16
                        && redirects < max_redirects
                        && self.redirect_to_controller(None).await
                    {
                        redirects += 1;
                        continue;
                    }
                    if is_transient_error_code(error_code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    check_ok(error_code, "leave_group")?;
                    return Ok(());
                }
                Response::Error { code, message } => {
                    if code == ErrorCode::UnknownMemberId as u16 {
                        return Ok(());
                    }
                    if code == ErrorCode::NotController as u16
                        && redirects < max_redirects
                        && self
                            .redirect_to_controller(parse_controller_id(&message))
                            .await
                    {
                        redirects += 1;
                        continue;
                    }
                    if is_transient_error_code(code) && retry_attempt < max_retries {
                        retry_attempt += 1;
                        tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms))
                            .await;
                        continue;
                    }
                    return Err(error_from_code(code, message));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected response for leave_group: {other:?}"
                    )))
                }
            }
        }
    }

    /// Describe a live consumer group (Phase 11).
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error **14**
    /// (`NotController`) redirects via [`ClientConfig::max_redirects`]
    /// (default 1; `0` does not redirect). Error 2 (no live members),
    /// 9 / 10 / 11, 13, 17 / 18, 21, 22, and protocol errors are not
    /// retried or redirected. Range assignor inherits via this method.
    pub async fn describe_group(&self, group_id: &str) -> Result<DescribeGroupResult> {
        let resp = self
            .describe_list_groups_round_trip(Request::DescribeGroup {
                group_id: group_id.to_owned(),
            })
            .await?;
        match resp {
            Response::DescribeGroup {
                error_code,
                group_id,
                generation,
                members,
            } => {
                check_ok(error_code, "describe_group")?;
                Ok(DescribeGroupResult {
                    group_id,
                    generation,
                    members,
                })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for describe_group: {other:?}"
            ))),
        }
    }

    /// List known consumer groups (Phase 12).
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Error **14**
    /// (`NotController`) redirects via [`ClientConfig::max_redirects`]
    /// (default 1; `0` does not redirect). Error 2, 9 / 10 / 11, 13,
    /// 17 / 18, 21, 22, and protocol errors are not retried or redirected.
    pub async fn list_groups(&self) -> Result<Vec<GroupListing>> {
        let resp = self
            .describe_list_groups_round_trip(Request::ListGroups)
            .await?;
        match resp {
            Response::ListGroups { error_code, groups } => {
                check_ok(error_code, "list_groups")?;
                Ok(groups)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for list_groups: {other:?}"
            ))),
        }
    }

    /// DescribeGroup / ListGroups share produce/heartbeat
    /// [`ClientConfig::max_retries`]. Transient 6 / 7 / 15 / 16 and
    /// [`Error::Io`] are retried; 13 / 9 / 10 / 11 / 2 / 17 / 18 / 21 /
    /// 22 and protocol errors are not. Error **14** (`NotController`)
    /// uses [`ClientConfig::max_redirects`] via
    /// [`Self::redirect_to_controller`] (independent of retry).
    /// `max_redirects=0` does not redirect.
    async fn describe_list_groups_round_trip(&self, req: Request) -> Result<Response> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        loop {
            let resp = match self.round_trip(req.clone()).await {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (code, hint) = match &resp {
                Response::Error { code, message } => (*code, parse_controller_id(message)),
                Response::DescribeGroup { error_code, .. }
                | Response::ListGroups { error_code, .. } => (*error_code, None),
                _ => return Ok(resp),
            };
            if code == ErrorCode::NotController as u16
                && redirects < max_redirects
                && self.redirect_to_controller(hint).await
            {
                redirects += 1;
                continue;
            }
            if is_transient_error_code(code) && retry_attempt < max_retries {
                retry_attempt += 1;
                tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                continue;
            }
            return Ok(resp);
        }
    }

    /// Delete committed offsets for a group (Phase 12).
    ///
    /// Empty `entries` deletes all offsets for the group.
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0).
    /// Error **14** (`NotController`) redirects via
    /// [`ClientConfig::max_redirects`] (default 1; `0` does not redirect).
    pub async fn delete_offsets(
        &self,
        group_id: &str,
        entries: Vec<OffsetEntry>,
    ) -> Result<DeleteOffsetsResult> {
        let resp = self
            .offset_admin_round_trip(Request::DeleteOffsets {
                group_id: group_id.to_owned(),
                entries,
            })
            .await?;
        match resp {
            Response::DeleteOffsets {
                error_code,
                deleted_count,
            } => {
                check_ok(error_code, "delete_offsets")?;
                Ok(DeleteOffsetsResult { deleted_count })
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for delete_offsets: {other:?}"
            ))),
        }
    }

    /// Delete every committed offset for `group_id` (empty wire entries).
    pub async fn delete_offsets_all(&self, group_id: &str) -> Result<DeleteOffsetsResult> {
        self.delete_offsets(group_id, Vec::new()).await
    }

    /// Delete one committed offset.
    pub async fn delete_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<DeleteOffsetsResult> {
        self.delete_offsets(
            group_id,
            vec![OffsetEntry {
                topic: topic.to_owned(),
                partition,
            }],
        )
        .await
    }

    /// Create ACL bindings (Phase 20). Enables enforcement on the broker.
    pub async fn create_acls(&self, entries: Vec<volant_protocol::AclBinding>) -> Result<()> {
        let resp = self
            .admin_round_trip(Request::CreateAcls { entries })
            .await?;
        match resp {
            Response::CreateAcls { error_code } => check_ok(error_code, "create_acls"),
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for create_acls: {other:?}"
            ))),
        }
    }

    /// Delete exact-matching ACL bindings (Phase 20).
    pub async fn delete_acls(&self, entries: Vec<volant_protocol::AclBinding>) -> Result<u32> {
        let resp = self
            .admin_round_trip(Request::DeleteAcls { entries })
            .await?;
        match resp {
            Response::DeleteAcls {
                error_code,
                removed,
            } => {
                check_ok(error_code, "delete_acls")?;
                Ok(removed)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for delete_acls: {other:?}"
            ))),
        }
    }

    /// List ACL bindings with optional filters (Phase 20).
    ///
    /// Empty `principal` / `resource` = any. `resource_type = 255` = any type.
    pub async fn list_acls(
        &self,
        principal: &str,
        resource_type: u8,
        resource: &str,
    ) -> Result<Vec<volant_protocol::AclBinding>> {
        let resp = self
            .admin_round_trip(Request::ListAcls {
                principal: principal.to_owned(),
                resource_type,
                resource: resource.to_owned(),
            })
            .await?;
        match resp {
            Response::ListAcls {
                error_code,
                entries,
            } => {
                check_ok(error_code, "list_acls")?;
                Ok(entries)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for list_acls: {other:?}"
            ))),
        }
    }

    /// List every ACL binding (empty filters: any principal / type / resource).
    /// Same as `list_acls("", 255, "")`.
    pub async fn list_acls_all(&self) -> Result<Vec<volant_protocol::AclBinding>> {
        self.list_acls("", 255, "").await
    }

    /// Add a broker endpoint to the membership overlay (v0.10).
    pub async fn add_broker(
        &self,
        id: u32,
        host: &str,
        port: u16,
        rack: Option<&str>,
    ) -> Result<u64> {
        let resp = self
            .admin_round_trip(Request::AddBroker {
                id,
                host: host.to_owned(),
                port,
                rack: rack.map(str::to_owned),
            })
            .await?;
        match resp {
            Response::AddBroker {
                error_code,
                generation,
            } => {
                check_ok(error_code, "add_broker")?;
                Ok(generation)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for add_broker: {other:?}"
            ))),
        }
    }

    /// Remove a broker from the membership overlay (v0.10).
    pub async fn remove_broker(&self, id: u32) -> Result<u64> {
        let resp = self.admin_round_trip(Request::RemoveBroker { id }).await?;
        match resp {
            Response::RemoveBroker {
                error_code,
                generation,
            } => {
                check_ok(error_code, "remove_broker")?;
                Ok(generation)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for remove_broker: {other:?}"
            ))),
        }
    }

    /// Reassign replicas for a topic (or one partition) (v0.18).
    ///
    /// `partition = None` updates every partition. Empty `replicas` asks the
    /// controller to auto-place with the current membership.
    pub async fn reassign_partitions(
        &self,
        topic: &str,
        partition: Option<u32>,
        replicas: &[u32],
    ) -> Result<u32> {
        let resp = self
            .admin_round_trip(Request::ReassignPartitions {
                topic: topic.to_owned(),
                partition: partition.unwrap_or(volant_protocol::REASSIGN_ALL_PARTITIONS),
                replicas: replicas.to_vec(),
            })
            .await?;
        match resp {
            Response::ReassignPartitions {
                error_code,
                generation,
            } => {
                check_ok(error_code, "reassign_partitions")?;
                Ok(generation)
            }
            Response::Error { code, message } => Err(error_from_code(code, message)),
            other => Err(Error::Protocol(format!(
                "unexpected response for reassign_partitions: {other:?}"
            ))),
        }
    }

    /// List configured + live membership (v0.10).
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0). Typed
    /// error_code 2 / 9 / 10 / 11 / 13 and protocol errors are
    /// not retried. Error **14** (`NotController`) uses
    /// [`ClientConfig::max_redirects`] via [`Self::redirect_to_controller`]
    /// (independent of retry). `max_redirects=0` does not redirect.
    /// 13 / 2 / 9 / 10 / 11 / 17 / 18 / 21 / 22 and protocol are not
    /// redirected. [`Self::metadata`] is not wrapped here.
    pub async fn list_members(&self) -> Result<MembershipList> {
        let max_redirects = self.config.max_redirects;
        let mut redirect_attempt = 0u32;
        loop {
            let resp = self
                .metadata_list_members_round_trip(Request::ListMembers)
                .await?;
            let (code, hint) = match &resp {
                Response::Error { code, message } => (*code, parse_controller_id(message)),
                Response::ListMembers { error_code, .. } => (*error_code, None),
                _ => return membership_list_from_response(resp),
            };
            if code == ErrorCode::NotController as u16
                && redirect_attempt + 1 < 1 + max_redirects
                && self.redirect_to_controller(hint).await
            {
                redirect_attempt += 1;
                continue;
            }
            return membership_list_from_response(resp);
        }
    }

    /// ListMembers without the v0.120 error-14 wrap. Used by
    /// [`Self::redirect_to_controller`] so hunt and `list_members` are
    /// not mutually recursive. Transient retry is still v0.96.
    async fn list_members_rpc(&self) -> Result<MembershipList> {
        let resp = self
            .metadata_list_members_round_trip(Request::ListMembers)
            .await?;
        membership_list_from_response(resp)
    }

    /// Metadata / ListMembers share produce/heartbeat
    /// [`ClientConfig::max_retries`]. Transient 6 / 7 / 15 / 16 and
    /// [`Error::Io`] are retried; 13 / 14 / 9 / 10 / 11 / 2 and protocol
    /// errors are not. Native Metadata has no top-level error_code —
    /// only [`Response::Error`] / transport are retry signals.
    async fn metadata_list_members_round_trip(&self, req: Request) -> Result<Response> {
        let max_retries = self.config.max_retries;
        let mut retry_attempt = 0u32;
        loop {
            let resp = match self.round_trip(req.clone()).await {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let code = match &resp {
                Response::ListMembers { error_code, .. }
                | Response::Error {
                    code: error_code, ..
                } => *error_code,
                _ => return Ok(resp),
            };
            if is_transient_error_code(code) && retry_attempt < max_retries {
                retry_attempt += 1;
                tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                continue;
            }
            return Ok(resp);
        }
    }

    /// Commit offsets for a consumer group.
    ///
    /// Pass `generation = 0` for admin/CLI commits that skip generation checks.
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0).
    pub async fn commit_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        entries: Vec<OffsetCommitEntry>,
    ) -> Result<()> {
        let resp = self
            .offset_admin_round_trip(Request::OffsetCommit {
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

    /// Admin path: empty member, generation 0, empty metadata.
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.commit_offset_meta(group_id, topic, partition, offset, "")
            .await
    }

    /// Admin path with per-entry metadata.
    pub async fn commit_offset_meta(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        metadata: &str,
    ) -> Result<()> {
        self.commit_offsets(
            group_id,
            "",
            0,
            vec![OffsetCommitEntry {
                topic: topic.to_owned(),
                partition,
                offset,
                metadata: metadata.to_owned(),
            }],
        )
        .await
    }

    /// One entry with caller member_id + generation (empty metadata).
    pub async fn commit_offset_member(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        member_id: &str,
        generation: u32,
    ) -> Result<()> {
        self.commit_offset_member_meta(
            group_id, topic, partition, offset, member_id, generation, "",
        )
        .await
    }

    /// One entry with member + generation + metadata.
    pub async fn commit_offset_member_meta(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
        member_id: &str,
        generation: u32,
        metadata: &str,
    ) -> Result<()> {
        self.commit_offsets(
            group_id,
            member_id,
            generation,
            vec![OffsetCommitEntry {
                topic: topic.to_owned(),
                partition,
                offset,
                metadata: metadata.to_owned(),
            }],
        )
        .await
    }

    /// Fetch committed offsets. Empty `entries` returns all offsets for the group.
    ///
    /// Transient broker/transport errors retry up to
    /// [`ClientConfig::max_retries`] extra times (default 0).
    pub async fn fetch_offsets(
        &self,
        group_id: &str,
        entries: Vec<OffsetEntry>,
    ) -> Result<Vec<OffsetFetchEntry>> {
        let resp = self
            .offset_admin_round_trip(Request::OffsetFetch {
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

    /// Fetch committed offsets for `topic`, including per-entry metadata.
    /// Calls `fetch_offsets(group_id, vec![])` (all group offsets) and
    /// keeps rows whose topic matches.
    pub async fn fetch_offsets_for_topic(
        &self,
        group_id: &str,
        topic: &str,
    ) -> Result<Vec<OffsetFetchEntry>> {
        let entries = self.fetch_offsets(group_id, vec![]).await?;
        Ok(entries.into_iter().filter(|e| e.topic == topic).collect())
    }

    /// Fetch every committed offset for `group` (empty wire entries).
    /// Same as `fetch_offsets(group_id, vec![])`.
    pub async fn fetch_offsets_all(&self, group_id: &str) -> Result<Vec<OffsetFetchEntry>> {
        self.fetch_offsets(group_id, Vec::new()).await
    }

    /// OffsetCommit / OffsetFetch / DeleteOffsets share produce/heartbeat
    /// [`ClientConfig::max_retries`]. Transient 6 / 7 / 15 / 16 and
    /// [`Error::Io`] are retried; 13 / 9 / 10 / 11 / 2 and protocol
    /// errors are not. Error **14** (`NotController`) uses
    /// [`ClientConfig::max_redirects`] via [`Self::redirect_to_controller`]
    /// (same budget as `admin_round_trip`). `max_redirects=0` does not
    /// redirect.
    async fn offset_admin_round_trip(&self, req: Request) -> Result<Response> {
        let max_retries = self.config.max_retries;
        let max_redirects = self.config.max_redirects;
        let mut retry_attempt = 0u32;
        let mut redirects = 0u32;
        loop {
            let resp = match self.round_trip(req.clone()).await {
                Ok(r) => r,
                Err(e) if is_transient_transport(&e) && retry_attempt < max_retries => {
                    retry_attempt += 1;
                    tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let (code, hint) = match &resp {
                Response::Error { code, message } => (*code, parse_controller_id(message)),
                Response::OffsetCommit { error_code }
                | Response::OffsetFetch { error_code, .. }
                | Response::DeleteOffsets { error_code, .. } => (*error_code, None),
                _ => return Ok(resp),
            };
            if code == ErrorCode::NotController as u16
                && redirects < max_redirects
                && self.redirect_to_controller(hint).await
            {
                redirects += 1;
                continue;
            }
            if is_transient_error_code(code) && retry_attempt < max_retries {
                retry_attempt += 1;
                tokio::time::sleep(Duration::from_millis(self.config.retry_backoff_ms)).await;
                continue;
            }
            return Ok(resp);
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

fn metadata_from_response(resp: Response) -> Result<Metadata> {
    match resp {
        Response::Metadata {
            brokers,
            topics,
            controller_id,
        } => Ok(Metadata {
            brokers,
            topics,
            controller_id,
        }),
        Response::Error { code, message } => Err(error_from_code(code, message)),
        other => Err(Error::Protocol(format!(
            "unexpected response for metadata: {other:?}"
        ))),
    }
}

fn membership_list_from_response(resp: Response) -> Result<MembershipList> {
    match resp {
        Response::ListMembers {
            error_code,
            generation,
            brokers,
            live,
        } => {
            check_ok(error_code, "list_members")?;
            Ok(MembershipList {
                generation,
                brokers,
                live,
            })
        }
        Response::Error { code, message } => Err(error_from_code(code, message)),
        other => Err(Error::Protocol(format!(
            "unexpected response for list_members: {other:?}"
        ))),
    }
}

/// Parse `controller_id=N` from a NotController Error message. Ignores junk.
fn parse_controller_id(message: &str) -> Option<u32> {
    let rest = message.split("controller_id=").nth(1)?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
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
        | ErrorCode::BrokerNotAvailable
        | ErrorCode::AuthenticationFailed
        | ErrorCode::AuthenticationRequired
        | ErrorCode::InvalidProducerEpoch
        | ErrorCode::OutOfOrderSequence
        | ErrorCode::UnknownProducerId
        | ErrorCode::InvalidTxnState
        | ErrorCode::TransactionAbortable
        | ErrorCode::AuthorizationFailed => Error::Protocol(message),
        ErrorCode::Ok | ErrorCode::Unknown => Error::Protocol(message),
    }
}

fn is_transient_error_code(code: u16) -> bool {
    matches!(
        ErrorCode::from_u16(code),
        ErrorCode::Timeout
            | ErrorCode::NotEnoughReplicas
            | ErrorCode::BrokerNotAvailable
            | ErrorCode::Io
    )
}

fn is_transient_transport(err: &Error) -> bool {
    matches!(err, Error::Io(_))
}

/// Kafka-compatible murmur2 partition (same algorithm as `volant_broker::partition_for_key`).
fn volant_broker_partition_for_key(key: &[u8], num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    (client_murmur2(key) & 0x7fff_ffff) % num_partitions
}

fn client_murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let length = data.len() as u32;
    let mut h: u32 = SEED ^ length;
    let length4 = data.len() / 4;

    for i in 0..length4 {
        let i4 = i * 4;
        let mut k = u32::from(data[i4])
            | (u32::from(data[i4 + 1]) << 8)
            | (u32::from(data[i4 + 2]) << 16)
            | (u32::from(data[i4 + 3]) << 24);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let rem = data.len() % 4;
    let offset = data.len() & !3;
    if rem == 3 {
        h ^= u32::from(data[offset + 2]) << 16;
    }
    if rem >= 2 {
        h ^= u32::from(data[offset + 1]) << 8;
    }
    if rem >= 1 {
        h ^= u32::from(data[offset]);
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
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
