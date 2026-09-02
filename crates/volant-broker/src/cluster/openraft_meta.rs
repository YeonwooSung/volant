//! Opt-in openraft metadata leader election (v0.11).
//!
//! Election only: assignment.json / Phase 154 log apply are unchanged.
//! InstallSnapshot is not implemented (snapshot policy never fires).

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogId, RaftLogReader,
    RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::warn;
use volant_core::{Error, Result};
use volant_protocol::{Request, Response};

use crate::broker::Broker;
use crate::net::inter_broker_rpc;

openraft::declare_raft_types!(
    /// Metadata-election type config (broker id = node id).
    pub TypeConfig:
        D = MetaRequest,
        R = MetaResponse,
        NodeId = u32,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime
);

/// Client log payload. Election-only MVP uses `Noop`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum MetaRequest {
    /// Empty command (election / heartbeat filler).
    #[default]
    Noop,
}

/// Apply result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetaResponse {
    /// Always true for `Noop`.
    pub ok: bool,
}

/// Handle stored on [`Broker`]. Dropping it stops the raft core.
pub struct OpenraftMetaHandle {
    raft: Raft<TypeConfig>,
}

impl fmt::Debug for OpenraftMetaHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenraftMetaHandle").finish_non_exhaustive()
    }
}

/// Drops the local raft node when the serve_listener / bg task ends.
pub struct OpenraftGuard(pub Arc<Broker>);

impl Drop for OpenraftGuard {
    fn drop(&mut self) {
        self.0.drop_openraft_metadata();
    }
}

/// Cached leader / term (also written to Prometheus gauges).
#[derive(Debug)]
pub struct OpenraftMetricsCache {
    /// Leader broker id (`0` = unknown).
    pub leader_id: AtomicU32,
    /// Current term.
    pub term: AtomicU64,
}

impl Default for OpenraftMetricsCache {
    fn default() -> Self {
        Self {
            leader_id: AtomicU32::new(0),
            term: AtomicU64::new(0),
        }
    }
}

#[derive(Clone, Default)]
struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Default)]
struct LogStoreInner {
    last_purged: Option<LogId<u32>>,
    committed: Option<LogId<u32>>,
    vote: Option<Vote<u32>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<u32>> {
        let inner = self.inner.lock();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<u32>> {
        let inner = self.inner.lock();
        let last = inner.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last.or(inner.last_purged),
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u32>>,
    ) -> std::result::Result<(), StorageError<u32>> {
        self.inner.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u32>>, StorageError<u32>> {
        Ok(self.inner.lock().committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u32>) -> std::result::Result<(), StorageError<u32>> {
        self.inner.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u32>>, StorageError<u32>> {
        Ok(self.inner.lock().vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.lock();
            for e in entries {
                inner.log.insert(e.get_log_id().index, e);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u32>) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u32>) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index.saturating_add(1)));
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[derive(Clone, Default)]
struct StateMachine {
    inner: Arc<Mutex<SmInner>>,
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogId<u32>>,
    last_membership: StoredMembership<u32, BasicNode>,
    snapshot: Option<(SnapshotMeta<u32, BasicNode>, Vec<u8>)>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<u32>> {
        let inner = self.inner.lock();
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id: "v11-empty".into(),
        };
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u32>>, StoredMembership<u32, BasicNode>),
        StorageError<u32>,
    > {
        let inner = self.inner.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<MetaResponse>, StorageError<u32>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut out = Vec::new();
        let mut inner = self.inner.lock();
        for e in entries {
            inner.last_applied = Some(*e.get_log_id());
            if let EntryPayload::Membership(ref m) = e.payload {
                inner.last_membership = StoredMembership::new(Some(*e.get_log_id()), m.clone());
            }
            out.push(MetaResponse { ok: true });
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u32>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u32, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), snapshot.into_inner()));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<u32>> {
        let inner = self.inner.lock();
        Ok(inner.snapshot.as_ref().map(|(meta, data)| Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(data.clone())),
        }))
    }
}

struct MetaNetwork {
    broker: Arc<Broker>,
    target: u32,
}

impl RaftNetwork<TypeConfig> for MetaNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>>
    {
        let payload = serde_json::to_vec(&rpc).map_err(|e| unreachable_err(&e))?;
        let resp = rpc_peer(
            &self.broker,
            self.target,
            Request::OpenraftAppend {
                payload: Bytes::from(payload),
            },
        )
        .await?;
        match resp {
            Response::OpenraftAppend { payload } => {
                serde_json::from_slice(&payload).map_err(|e| unreachable_err(&e))
            }
            other => Err(unreachable_err(&format!("unexpected append ack {other:?}"))),
        }
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u32>,
        RPCError<u32, BasicNode, RaftError<u32, openraft::error::InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::new(
                std::io::ErrorKind::Other,
                "InstallSnapshot is not implemented in v0.11",
            ),
        )))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u32>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u32>, RPCError<u32, BasicNode, RaftError<u32>>> {
        let payload = serde_json::to_vec(&rpc).map_err(|e| unreachable_err(&e))?;
        let resp = rpc_peer(
            &self.broker,
            self.target,
            Request::OpenraftVote {
                payload: Bytes::from(payload),
            },
        )
        .await?;
        match resp {
            Response::OpenraftVote { payload } => {
                serde_json::from_slice(&payload).map_err(|e| unreachable_err(&e))
            }
            other => Err(unreachable_err(&format!("unexpected vote ack {other:?}"))),
        }
    }
}

struct MetaNetworkFactory {
    broker: Arc<Broker>,
}

impl RaftNetworkFactory<TypeConfig> for MetaNetworkFactory {
    type Network = MetaNetwork;

    async fn new_client(&mut self, target: u32, _node: &BasicNode) -> Self::Network {
        MetaNetwork {
            broker: Arc::clone(&self.broker),
            target,
        }
    }
}

fn unreachable_err(err: &dyn fmt::Display) -> RPCError<u32, BasicNode, RaftError<u32>> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::Other,
        err.to_string(),
    )))
}

async fn rpc_peer(
    broker: &Broker,
    target: u32,
    req: Request,
) -> std::result::Result<Response, RPCError<u32, BasicNode, RaftError<u32>>> {
    let addr = broker
        .broker_addr(target)
        .ok_or_else(|| unreachable_err(&format!("no addr for broker {target}")))?;
    inter_broker_rpc(broker, &addr, &req)
        .await
        .map_err(|e| unreachable_err(&e))
}

fn membership_nodes(broker: &Broker) -> BTreeMap<u32, BasicNode> {
    let mut nodes = BTreeMap::new();
    if let Some(cfg) = broker.cluster_config() {
        for b in cfg.brokers {
            nodes.insert(
                b.id,
                BasicNode {
                    addr: format!("{}:{}", b.host, b.port),
                },
            );
        }
    }
    nodes
}

fn raft_config() -> Config {
    Config {
        cluster_name: "volant-openraft-meta".into(),
        election_timeout_min: 200,
        election_timeout_max: 400,
        heartbeat_interval: 50,
        install_snapshot_timeout: 400,
        max_payload_entries: 64,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(u64::MAX),
        enable_tick: true,
        enable_heartbeat: true,
        enable_elect: true,
        ..Default::default()
    }
}

impl Broker {
    /// Whether `VOLANT_OPENRAFT_METADATA` is on for this process.
    pub fn openraft_metadata_enabled(&self) -> bool {
        self.openraft_metadata_enabled.load(Ordering::Relaxed)
    }

    /// Current openraft leader id (`None` if flag off or no leader yet).
    pub fn openraft_leader_id(&self) -> Option<u32> {
        if !self.openraft_metadata_enabled() {
            return None;
        }
        if let Some(h) = self.openraft_meta.lock().as_ref() {
            if let Some(id) = h.raft.metrics().borrow().current_leader {
                return Some(id);
            }
        }
        let cached = self.openraft_metrics.leader_id.load(Ordering::Relaxed);
        if cached == 0 {
            None
        } else {
            Some(cached)
        }
    }

    /// Current openraft term (`0` if the group has not started).
    pub fn openraft_term(&self) -> u64 {
        if let Some(h) = self.openraft_meta.lock().as_ref() {
            return h.raft.metrics().borrow().current_term;
        }
        self.openraft_metrics.term.load(Ordering::Relaxed)
    }

    /// Start the local openraft node (idempotent).
    pub async fn boot_openraft_metadata(self: &Arc<Self>) -> Result<()> {
        if !self.openraft_metadata_enabled() {
            return Ok(());
        }
        if self.cluster_config().is_none() {
            return Ok(());
        }
        {
            let g = self.openraft_meta.lock();
            if g.is_some() {
                return Ok(());
            }
        }
        let nodes = membership_nodes(self);
        if nodes.len() < 2 {
            return Ok(());
        }
        let config = Arc::new(
            raft_config()
                .validate()
                .map_err(|e| Error::InvalidArgument(e.to_string()))?,
        );
        let network = MetaNetworkFactory {
            broker: Arc::clone(self),
        };
        let log_store = LogStore::default();
        let state_machine = StateMachine::default();
        let raft = Raft::new(self.node_id(), config, network, log_store, state_machine)
            .await
            .map_err(|e| Error::InvalidArgument(format!("openraft new: {e}")))?;
        *self.openraft_meta.lock() = Some(OpenraftMetaHandle { raft: raft.clone() });

        // Lowest configured id initializes. Others retry if no leader appears
        // (initializer may have been slow to bind).
        let lowest = nodes.keys().copied().next().unwrap_or(self.node_id());
        if self.node_id() == lowest {
            // Give peer accept loops a moment to bind.
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            if let Err(e) = raft.initialize(nodes.clone()).await {
                warn!(error = %e, "openraft initialize (lowest id)");
            }
        } else {
            let raft_retry = raft.clone();
            let nodes_retry = nodes;
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                if raft_retry.metrics().borrow().current_leader.is_none() {
                    if let Err(e) = raft_retry.initialize(nodes_retry).await {
                        warn!(error = %e, "openraft initialize retry");
                    }
                }
            });
        }
        Ok(())
    }

    /// Drop the local raft node (isolate / shutdown).
    pub fn drop_openraft_metadata(&self) {
        let _ = self.openraft_meta.lock().take();
        self.openraft_metrics.leader_id.store(0, Ordering::Relaxed);
    }

    /// Refresh cached leader / term from the live raft handle.
    pub fn refresh_openraft_metrics(&self) {
        let m = {
            let g = self.openraft_meta.lock();
            let Some(h) = g.as_ref() else {
                return;
            };
            h.raft.metrics().borrow().clone()
        };
        self.openraft_metrics
            .leader_id
            .store(m.current_leader.unwrap_or(0), Ordering::Relaxed);
        self.openraft_metrics
            .term
            .store(m.current_term, Ordering::Relaxed);
    }

    /// Handle inbound AppendEntries (opcode 108).
    pub async fn handle_openraft_append(&self, payload: &[u8]) -> Result<Bytes> {
        let req: AppendEntriesRequest<TypeConfig> = serde_json::from_slice(payload)
            .map_err(|e| Error::Protocol(format!("openraft append decode: {e}")))?;
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        let resp = raft
            .append_entries(req)
            .await
            .map_err(|e| Error::Protocol(format!("openraft append: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Error::Protocol(format!("openraft append encode: {e}")))?;
        Ok(Bytes::from(bytes))
    }

    /// Handle inbound RequestVote (opcode 110).
    pub async fn handle_openraft_vote(&self, payload: &[u8]) -> Result<Bytes> {
        let req: VoteRequest<u32> = serde_json::from_slice(payload)
            .map_err(|e| Error::Protocol(format!("openraft vote decode: {e}")))?;
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        let resp = raft
            .vote(req)
            .await
            .map_err(|e| Error::Protocol(format!("openraft vote: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Error::Protocol(format!("openraft vote encode: {e}")))?;
        Ok(Bytes::from(bytes))
    }
}

/// Parse `VOLANT_OPENRAFT_METADATA`. Default **off**.
pub fn default_openraft_metadata_enabled() -> bool {
    match std::env::var("VOLANT_OPENRAFT_METADATA") {
        Ok(s) => {
            let t = s.trim();
            t == "1"
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("yes")
                || t.eq_ignore_ascii_case("on")
        }
        Err(_) => false,
    }
}
