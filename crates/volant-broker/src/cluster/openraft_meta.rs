//! Opt-in openraft metadata election (v0.11) + assignment apply (v0.16)
//! + InstallSnapshot (v0.17) + durable log / hard state (v0.21)
//! + snapshot assignment apply (v0.22) + joint membership (v0.26).
//!
//! When `VOLANT_OPENRAFT_METADATA=1`, the leader replicates
//! [`MetaRequest::SetAssignment`] via opcodes 108/109. Apply writes
//! `assignment.json` and installs cluster state. Snapshots use opcodes
//! 112/113; a non-empty snapshot `assignment` is applied the same way.
//! Vote, log, and last snapshot persist under `{data_dir}/__openraft/`
//! (JSON files; not Rocks). Flag off does not create that directory.
//! Homemade 154 is unchanged.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, ChangeMembers, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogId,
    RaftLogReader, RaftNetwork, RaftNetworkFactory, RaftSnapshotBuilder, SnapshotMeta,
    SnapshotPolicy, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::warn;
use volant_core::{Error, Result};
use volant_protocol::{ClusterTopicState, Request, Response};

use crate::broker::Broker;
use crate::cluster::state::AssignmentSnapshot;
use crate::net::inter_broker_rpc;

/// On-disk directory under `data_dir` for the opt-in openraft store (v0.21).
pub const OPENRAFT_DIR: &str = "__openraft";
/// Durable vote / committed / last_purged file name.
pub const OPENRAFT_HARD_STATE_FILE: &str = "hard_state.json";
/// Durable log entries file name.
pub const OPENRAFT_LOG_FILE: &str = "log.json";
/// Last snapshot meta + payload (and last_applied checkpoint).
pub const OPENRAFT_SNAPSHOT_FILE: &str = "snapshot.json";

/// How long the mutating leader waits for `client_write` (commit + local apply).
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Client log payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum MetaRequest {
    /// Empty command (election / heartbeat filler).
    #[default]
    Noop,
    /// Full assignment snapshot (CreateTopic / DeleteTopic / CreatePartitions).
    SetAssignment {
        /// Assignment generation for this snapshot.
        generation: u32,
        /// Wire topics (leaders / replicas / ISR).
        topics: Vec<ClusterTopicState>,
    },
}

/// Apply result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetaResponse {
    /// True when apply succeeded (or the entry was `Noop` / membership).
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
    /// `{data_dir}/__openraft` when the flag is on; `None` = memory-only.
    dir: Option<PathBuf>,
}

#[derive(Default)]
struct LogStoreInner {
    last_purged: Option<LogId<u32>>,
    committed: Option<LogId<u32>>,
    vote: Option<Vote<u32>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

/// On-disk hard state (vote + commit pointers).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenraftHardStateFile {
    #[serde(default)]
    vote: Option<Vote<u32>>,
    #[serde(default)]
    committed: Option<LogId<u32>>,
    #[serde(default)]
    last_purged: Option<LogId<u32>>,
}

/// On-disk log (JSON array of openraft entries).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenraftLogFile {
    #[serde(default)]
    entries: Vec<Entry<TypeConfig>>,
}

impl LogStore {
    fn open(dir: &Path) -> Self {
        let hard = load_json::<OpenraftHardStateFile>(&dir.join(OPENRAFT_HARD_STATE_FILE))
            .unwrap_or_default();
        let file = load_json::<OpenraftLogFile>(&dir.join(OPENRAFT_LOG_FILE)).unwrap_or_default();
        let mut log = BTreeMap::new();
        for e in file.entries {
            log.insert(e.get_log_id().index, e);
        }
        Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                last_purged: hard.last_purged,
                committed: hard.committed,
                vote: hard.vote,
                log,
            })),
            dir: Some(dir.to_path_buf()),
        }
    }

    fn persist_hard(&self, inner: &LogStoreInner) -> std::io::Result<()> {
        let Some(dir) = self.dir.as_ref() else {
            return Ok(());
        };
        let file = OpenraftHardStateFile {
            vote: inner.vote,
            committed: inner.committed,
            last_purged: inner.last_purged,
        };
        atomic_write_json(&dir.join(OPENRAFT_HARD_STATE_FILE), &file)
    }

    fn persist_log(&self, inner: &LogStoreInner) -> std::io::Result<()> {
        let Some(dir) = self.dir.as_ref() else {
            return Ok(());
        };
        let file = OpenraftLogFile {
            entries: inner.log.values().cloned().collect(),
        };
        atomic_write_json(&dir.join(OPENRAFT_LOG_FILE), &file)
    }
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
        let mut inner = self.inner.lock();
        inner.committed = committed;
        self.persist_hard(&inner)
            .map_err(|e| StorageError::from(StorageIOError::write_logs(&e)))?;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u32>>, StorageError<u32>> {
        Ok(self.inner.lock().committed)
    }

    async fn save_vote(&mut self, vote: &Vote<u32>) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.vote = Some(*vote);
        self.persist_hard(&inner)
            .map_err(|e| StorageError::from(StorageIOError::write_vote(&e)))?;
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
            self.persist_log(&inner)
                .map_err(|e| StorageError::from(StorageIOError::write_logs(&e)))?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u32>) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.log.split_off(&log_id.index);
        self.persist_log(&inner)
            .map_err(|e| StorageError::from(StorageIOError::write_logs(&e)))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u32>) -> std::result::Result<(), StorageError<u32>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index.saturating_add(1)));
        self.persist_hard(&inner)
            .map_err(|e| StorageError::from(StorageIOError::write_logs(&e)))?;
        self.persist_log(&inner)
            .map_err(|e| StorageError::from(StorageIOError::write_logs(&e)))?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

/// JSON bytes stored in an openraft snapshot (v0.17).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaSnapshotPayload {
    last_applied: Option<LogId<u32>>,
    membership: StoredMembership<u32, BasicNode>,
    assignment: AssignmentSnapshot,
}

/// On-disk last snapshot + last_applied checkpoint (v0.21).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct OpenraftSnapshotFile {
    #[serde(default)]
    last_applied: Option<LogId<u32>>,
    #[serde(default)]
    last_membership: StoredMembership<u32, BasicNode>,
    #[serde(default)]
    snapshot_seq: u64,
    #[serde(default)]
    snapshot: Option<OpenraftSnapshotBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenraftSnapshotBlob {
    meta: SnapshotMeta<u32, BasicNode>,
    /// Raw JSON of [`MetaSnapshotPayload`].
    payload: MetaSnapshotPayload,
}

#[derive(Clone, Default)]
struct StateMachine {
    inner: Arc<Mutex<SmInner>>,
    /// Weak so apply can install assignment without a Broker↔Raft cycle.
    broker: Option<Weak<Broker>>,
    /// `{data_dir}/__openraft` when the flag is on; `None` = memory-only.
    dir: Option<PathBuf>,
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogId<u32>>,
    last_membership: StoredMembership<u32, BasicNode>,
    snapshot: Option<(SnapshotMeta<u32, BasicNode>, Vec<u8>)>,
    snapshot_seq: u64,
}

impl StateMachine {
    fn open(dir: &Path, broker: &Arc<Broker>) -> Self {
        let file = load_json::<OpenraftSnapshotFile>(&dir.join(OPENRAFT_SNAPSHOT_FILE))
            .unwrap_or_default();
        let mut inner = SmInner {
            last_applied: file.last_applied,
            last_membership: file.last_membership,
            snapshot: None,
            snapshot_seq: file.snapshot_seq,
        };
        if let Some(blob) = file.snapshot {
            if inner.last_applied.is_none() {
                inner.last_applied = blob.meta.last_log_id;
            }
            if inner.last_membership == StoredMembership::default() {
                inner.last_membership = blob.meta.last_membership.clone();
            }
            let bytes = serde_json::to_vec(&blob.payload).unwrap_or_else(|_| b"{}".to_vec());
            inner.snapshot = Some((blob.meta, bytes));
        }
        Self {
            inner: Arc::new(Mutex::new(inner)),
            broker: Some(Arc::downgrade(broker)),
            dir: Some(dir.to_path_buf()),
        }
    }

    /// Memory-only SM bound to `broker` (v0.22 install tests; no `__openraft/` write).
    fn with_broker(broker: &Arc<Broker>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SmInner::default())),
            broker: Some(Arc::downgrade(broker)),
            dir: None,
        }
    }

    fn live_assignment(&self) -> AssignmentSnapshot {
        self.broker
            .as_ref()
            .and_then(|w| w.upgrade())
            .and_then(|b| b.clone_live_assignment())
            .unwrap_or_default()
    }

    fn persist_sm(&self, inner: &SmInner) {
        let Some(dir) = self.dir.as_ref() else {
            return;
        };
        let snapshot = inner.snapshot.as_ref().and_then(|(meta, bytes)| {
            let payload = serde_json::from_slice::<MetaSnapshotPayload>(bytes).ok()?;
            Some(OpenraftSnapshotBlob {
                meta: meta.clone(),
                payload,
            })
        });
        let file = OpenraftSnapshotFile {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_seq: inner.snapshot_seq,
            snapshot,
        };
        if let Err(e) = atomic_write_json(&dir.join(OPENRAFT_SNAPSHOT_FILE), &file) {
            warn!(
                path = %dir.join(OPENRAFT_SNAPSHOT_FILE).display(),
                error = %e,
                "openraft snapshot persist failed"
            );
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<u32>> {
        let assignment = self.live_assignment();
        let mut inner = self.inner.lock();
        let payload = MetaSnapshotPayload {
            last_applied: inner.last_applied,
            membership: inner.last_membership.clone(),
            assignment,
        };
        let bytes = serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec());
        let seq = inner.snapshot_seq;
        inner.snapshot_seq = seq.saturating_add(1);
        let index = inner.last_applied.map(|id| id.index).unwrap_or(0);
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id: format!("v17-{index}-{seq}"),
        };
        inner.snapshot = Some((meta.clone(), bytes.clone()));
        self.persist_sm(&inner);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
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
        let broker = self.broker.as_ref().and_then(|w| w.upgrade());
        for e in entries {
            {
                let mut inner = self.inner.lock();
                inner.last_applied = Some(*e.get_log_id());
                if let EntryPayload::Membership(ref m) = e.payload {
                    inner.last_membership = StoredMembership::new(Some(*e.get_log_id()), m.clone());
                }
            }
            let ok = match &e.payload {
                EntryPayload::Normal(MetaRequest::SetAssignment { generation, topics }) => {
                    match &broker {
                        Some(b) => {
                            match b.apply_cluster_state(*generation, b.controller_id(), topics) {
                                Ok(()) => true,
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        generation,
                                        "openraft apply SetAssignment failed"
                                    );
                                    false
                                }
                            }
                        }
                        None => {
                            warn!("openraft apply SetAssignment: broker dropped");
                            false
                        }
                    }
                }
                _ => true,
            };
            out.push(MetaResponse { ok });
        }
        {
            let inner = self.inner.lock();
            self.persist_sm(&inner);
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
        let bytes = snapshot.into_inner();
        if let Ok(payload) = serde_json::from_slice::<MetaSnapshotPayload>(&bytes) {
            // last_applied / membership come from SnapshotMeta. A non-empty
            // assignment is applied via the same path as SetAssignment (v0.16).
            // Empty topics is a no-op so we never wipe a live assignment.json.
            // Apply errors are logged; raft meta still installs.
            if !payload.assignment.topics.is_empty() {
                match self.broker.as_ref().and_then(|w| w.upgrade()) {
                    Some(b) => {
                        let generation = payload.assignment.generation;
                        let topics = payload.assignment.to_wire_topics();
                        if let Err(err) =
                            b.apply_cluster_state(generation, b.controller_id(), &topics)
                        {
                            warn!(
                                error = %err,
                                generation,
                                "openraft install_snapshot apply assignment failed"
                            );
                        }
                    }
                    None => {
                        warn!("openraft install_snapshot: broker dropped; assignment not applied");
                    }
                }
            }
        }
        let mut inner = self.inner.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some((meta.clone(), bytes));
        self.persist_sm(&inner);
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
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u32>,
        RPCError<u32, BasicNode, RaftError<u32, InstallSnapshotError>>,
    > {
        let payload = serde_json::to_vec(&rpc).map_err(|e| unreachable_install_err(&e))?;
        let resp = rpc_peer_install(
            &self.broker,
            self.target,
            Request::OpenraftInstallSnapshot {
                payload: Bytes::from(payload),
            },
        )
        .await?;
        match resp {
            Response::OpenraftInstallSnapshot { payload } => {
                serde_json::from_slice(&payload).map_err(|e| unreachable_install_err(&e))
            }
            other => Err(unreachable_install_err(&format!(
                "unexpected install-snapshot ack {other:?}"
            ))),
        }
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

fn unreachable_install_err(
    err: &dyn fmt::Display,
) -> RPCError<u32, BasicNode, RaftError<u32, InstallSnapshotError>> {
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

async fn rpc_peer_install(
    broker: &Broker,
    target: u32,
    req: Request,
) -> std::result::Result<Response, RPCError<u32, BasicNode, RaftError<u32, InstallSnapshotError>>> {
    let addr = broker
        .broker_addr(target)
        .ok_or_else(|| unreachable_install_err(&format!("no addr for broker {target}")))?;
    inter_broker_rpc(broker, &addr, &req)
        .await
        .map_err(|e| unreachable_install_err(&e))
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

/// Snapshot interval from `VOLANT_OPENRAFT_SNAPSHOT_LOGS`.
///
/// * unset → every **1000** applied logs (conservative production default)
/// * `0` / `never` / `off` → never (manual `trigger().snapshot()` only)
/// * `N` ≥ 1 → every N logs (tests use `1` or `5`)
pub fn openraft_snapshot_logs_since_last() -> Option<u64> {
    match std::env::var("VOLANT_OPENRAFT_SNAPSHOT_LOGS") {
        Ok(s) => {
            let t = s.trim();
            if t.is_empty() {
                return Some(DEFAULT_OPENRAFT_SNAPSHOT_LOGS);
            }
            if t == "0"
                || t.eq_ignore_ascii_case("never")
                || t.eq_ignore_ascii_case("off")
                || t.eq_ignore_ascii_case("false")
            {
                return None;
            }
            Some(
                t.parse::<u64>()
                    .unwrap_or(DEFAULT_OPENRAFT_SNAPSHOT_LOGS)
                    .max(1),
            )
        }
        Err(_) => Some(DEFAULT_OPENRAFT_SNAPSHOT_LOGS),
    }
}

/// Default production snapshot interval (applied logs since last snapshot).
pub const DEFAULT_OPENRAFT_SNAPSHOT_LOGS: u64 = 1000;

fn raft_config() -> Config {
    let env_set = std::env::var("VOLANT_OPENRAFT_SNAPSHOT_LOGS").is_ok();
    let (snapshot_policy, max_in_snapshot_log_to_keep, replication_lag_threshold) =
        match openraft_snapshot_logs_since_last() {
            None => (SnapshotPolicy::Never, 1000, 5000),
            Some(n) if env_set => (SnapshotPolicy::LogsSinceLast(n), 0, n.max(1)),
            Some(n) => (SnapshotPolicy::LogsSinceLast(n), 1000, 5000),
        };
    Config {
        cluster_name: "volant-openraft-meta".into(),
        election_timeout_min: 200,
        election_timeout_max: 400,
        heartbeat_interval: 50,
        install_snapshot_timeout: 400,
        max_payload_entries: 64,
        snapshot_policy,
        max_in_snapshot_log_to_keep,
        replication_lag_threshold,
        purge_batch_size: 1,
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

    /// Effective openraft voter ids (sorted). Empty when raft is not started.
    pub fn openraft_voter_ids(&self) -> Vec<u32> {
        let g = self.openraft_meta.lock();
        let Some(h) = g.as_ref() else {
            return Vec::new();
        };
        let mut ids: Vec<u32> = h
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Last intended voter set after overlay add/remove (v0.26 test hook).
    pub fn test_last_openraft_membership_target(&self) -> Option<Vec<u32>> {
        self.openraft_last_membership_target.lock().clone()
    }

    /// Record configured broker ids as the intended openraft voter set.
    ///
    /// No-op when the openraft flag is off (v0.10 overlay-only).
    pub fn note_openraft_membership_target(&self) {
        if !self.openraft_metadata_enabled() {
            return;
        }
        let ids = self
            .cluster_config()
            .map(|c| c.broker_ids())
            .unwrap_or_default();
        *self.openraft_last_membership_target.lock() = Some(ids);
    }

    /// Propose openraft joint membership to the configured broker ids.
    ///
    /// Only the openraft leader calls `change_membership`. Overlay add/remove
    /// is already persisted; a raft failure is logged and does **not** roll
    /// back `{data_dir}/cluster/membership.json`.
    ///
    /// Returns `true` when the flag is off, this node is not the leader, the
    /// voter set already matches, or the wait succeeds.
    pub async fn change_openraft_membership(&self) -> bool {
        if !self.openraft_metadata_enabled() {
            return true;
        }
        if self.cluster_config().is_none() {
            return true;
        }
        self.note_openraft_membership_target();
        if !self.is_controller() {
            return true;
        }
        let nodes = membership_nodes(self);
        if nodes.is_empty() {
            return true;
        }
        let target: Vec<u32> = nodes.keys().copied().collect();
        let current = self.openraft_voter_ids();
        if current == target {
            return true;
        }
        let raft = {
            let g = self.openraft_meta.lock();
            match g.as_ref() {
                Some(h) => h.raft.clone(),
                None => {
                    warn!("openraft change_membership: raft not started");
                    return false;
                }
            }
        };
        // openraft 0.9 ReplaceAllVoters requires a Node record for every new
        // voter. AddNodes first (learner), then replace voters (joint).
        let add = ChangeMembers::AddNodes(nodes);
        match tokio::time::timeout(CLIENT_WRITE_TIMEOUT, raft.change_membership(add, true)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                warn!(error = %e, "openraft AddNodes (learner) failed");
            }
            Err(_) => {
                warn!(
                    timeout_secs = CLIENT_WRITE_TIMEOUT.as_secs(),
                    "openraft AddNodes (learner) timed out"
                );
            }
        }
        let ids: BTreeSet<u32> = target.iter().copied().collect();
        match tokio::time::timeout(CLIENT_WRITE_TIMEOUT, raft.change_membership(ids, false)).await {
            Ok(Ok(_)) => true,
            Ok(Err(e)) => {
                warn!(error = %e, "openraft change_membership failed");
                false
            }
            Err(_) => {
                warn!(
                    timeout_secs = CLIENT_WRITE_TIMEOUT.as_secs(),
                    "openraft change_membership timed out"
                );
                false
            }
        }
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
        let dir = match self.cluster_state() {
            Some(c) => c.data_dir.join(OPENRAFT_DIR),
            None => return Ok(()),
        };
        let log_store = LogStore::open(&dir);
        let state_machine = StateMachine::open(&dir, self);
        let raft = Raft::new(self.node_id(), config, network, log_store, state_machine)
            .await
            .map_err(|e| Error::InvalidArgument(format!("openraft new: {e}")))?;
        *self.openraft_meta.lock() = Some(OpenraftMetaHandle { raft: raft.clone() });

        // Restored vote/log already counts as initialized; skip a second
        // initialize() so restart does not rewrite membership.
        let already = raft.is_initialized().await.unwrap_or(false);
        if already {
            return Ok(());
        }

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

    /// Replicate the live assignment as [`MetaRequest::SetAssignment`] and wait
    /// for local apply (timeout). Returns `true` on success.
    ///
    /// No-op success when the flag is off or this process is not clustered.
    /// Fail (and do not call `client_write`) when this node is not the
    /// openraft leader or the raft node is not started.
    pub async fn client_write_set_assignment(&self) -> bool {
        if !self.openraft_metadata_enabled() {
            return true;
        }
        if self.cluster_config().is_none() {
            return true;
        }
        if !self.is_controller() {
            warn!(
                node = self.node_id(),
                leader = self.controller_id(),
                "openraft client_write skipped; not leader"
            );
            return false;
        }
        let raft = {
            let g = self.openraft_meta.lock();
            match g.as_ref() {
                Some(h) => h.raft.clone(),
                None => {
                    warn!("openraft client_write: raft not started");
                    return false;
                }
            }
        };
        let Some(asg) = self.clone_live_assignment() else {
            return true;
        };
        let req = MetaRequest::SetAssignment {
            generation: asg.generation,
            topics: asg.to_wire_topics(),
        };
        match tokio::time::timeout(CLIENT_WRITE_TIMEOUT, raft.client_write(req)).await {
            Ok(Ok(resp)) => resp.data.ok,
            Ok(Err(e)) => {
                warn!(error = %e, "openraft client_write SetAssignment failed");
                false
            }
            Err(_) => {
                warn!(
                    timeout_secs = CLIENT_WRITE_TIMEOUT.as_secs(),
                    "openraft client_write SetAssignment timed out"
                );
                false
            }
        }
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

    /// Handle inbound InstallSnapshot (opcode 112).
    pub async fn handle_openraft_install_snapshot(&self, payload: &[u8]) -> Result<Bytes> {
        self.openraft_install_snapshot_rx
            .fetch_add(1, Ordering::Relaxed);
        let req: InstallSnapshotRequest<TypeConfig> = serde_json::from_slice(payload)
            .map_err(|e| Error::Protocol(format!("openraft install snapshot decode: {e}")))?;
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        let resp = raft
            .install_snapshot(req)
            .await
            .map_err(|e| Error::Protocol(format!("openraft install snapshot: {e}")))?;
        let bytes = serde_json::to_vec(&resp)
            .map_err(|e| Error::Protocol(format!("openraft install snapshot encode: {e}")))?;
        Ok(Bytes::from(bytes))
    }

    /// Inbound InstallSnapshot (opcode 112) count (test hook).
    pub fn test_openraft_install_snapshot_rx(&self) -> u64 {
        self.openraft_install_snapshot_rx.load(Ordering::Relaxed)
    }

    /// Last purged log index (`None` if the prefix has not been truncated).
    pub fn test_openraft_last_purged_index(&self) -> Option<u64> {
        let g = self.openraft_meta.lock();
        g.as_ref()?
            .raft
            .metrics()
            .borrow()
            .purged
            .map(|id| id.index)
    }

    /// Last applied log index (`None` if nothing has been applied).
    pub fn test_openraft_last_applied_index(&self) -> Option<u64> {
        let g = self.openraft_meta.lock();
        g.as_ref()?
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
    }

    /// Current in-memory snapshot (`snapshot_id`, last log index, JSON bytes).
    pub async fn test_openraft_current_snapshot(&self) -> Option<(String, Option<u64>, Vec<u8>)> {
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()?.raft.clone()
        };
        let snap = raft.get_snapshot().await.ok()??;
        let bytes = snap.snapshot.get_ref().clone();
        Some((
            snap.meta.snapshot_id,
            snap.meta.last_log_id.map(|id| id.index),
            bytes,
        ))
    }

    /// Submit a `Noop` client write (must run on the leader).
    pub async fn test_openraft_client_write_noop(&self) -> Result<()> {
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        raft.client_write(MetaRequest::Noop)
            .await
            .map_err(|e| Error::Protocol(format!("openraft client write: {e}")))?;
        Ok(())
    }

    /// Ask openraft to build a snapshot now (returns immediately).
    pub async fn test_openraft_trigger_snapshot(&self) -> Result<()> {
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        raft.trigger()
            .snapshot()
            .await
            .map_err(|e| Error::Protocol(format!("openraft trigger snapshot: {e}")))?;
        Ok(())
    }

    /// Ask openraft to purge logs up through last-applied (returns immediately).
    pub async fn test_openraft_trigger_purge(&self) -> Result<()> {
        let raft = {
            let g = self.openraft_meta.lock();
            g.as_ref()
                .map(|h| h.raft.clone())
                .ok_or_else(|| Error::Protocol("openraft not started".into()))?
        };
        let upto = raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0);
        raft.trigger()
            .purge_log(upto)
            .await
            .map_err(|e| Error::Protocol(format!("openraft trigger purge: {e}")))?;
        Ok(())
    }

    /// JSON `MetaSnapshotPayload` bytes (`last_applied` / membership empty).
    pub fn test_openraft_snapshot_bytes(assignment: &AssignmentSnapshot) -> Vec<u8> {
        let payload = MetaSnapshotPayload {
            last_applied: None,
            membership: StoredMembership::default(),
            assignment: assignment.clone(),
        };
        serde_json::to_vec(&payload).unwrap_or_else(|_| b"{}".to_vec())
    }

    /// Drive [`RaftStateMachine::install_snapshot`] on this broker (no raft RPC).
    pub async fn test_openraft_sm_install_snapshot(self: &Arc<Self>, bytes: Vec<u8>) {
        let mut sm = StateMachine::with_broker(self);
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "v22-test".into(),
        };
        sm.install_snapshot(&meta, Box::new(Cursor::new(bytes)))
            .await
            .expect("install_snapshot");
    }

    /// JSON body of a dummy InstallSnapshotRequest (vote term 0) for opcode tests.
    pub fn test_openraft_probe_install_snapshot_payload() -> Bytes {
        let rpc = InstallSnapshotRequest::<TypeConfig> {
            vote: Vote::new(0, 1),
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "v17-probe".into(),
            },
            offset: 0,
            data: b"{}".to_vec(),
            done: true,
        };
        Bytes::from(serde_json::to_vec(&rpc).unwrap_or_else(|_| b"{}".to_vec()))
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

fn load_json<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let mut f = File::open(path).ok()?;
    let mut s = String::new();
    f.read_to_string(&mut s).ok()?;
    serde_json::from_str(&s).ok()
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&json)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}
