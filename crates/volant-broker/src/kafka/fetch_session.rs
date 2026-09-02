//! Fetch session state (Phase 88 + 91 omit-unchanged + Phase 95 limits + Phase 115 durable
//! + Phase 119 multi-broker owner encoding / handoff + Phase 138 peer mirror + Phase 139
//! mirror polish + Phase 143 promote claim fence + Phase 146 incremental MirrorPut +
//! Phase 147 serve-from-mirror).
//!
//! Tracks topic partitions, last-seen fetch params, last-returned HWM/LSO (Phase 91),
//! idle TTL / max concurrent sessions with lazy LRU eviction (Phase 95), and optional
//! durability under `{data_dir}/__fetch_sessions/state.json` (Phase 115).
//!
//! Sessions remain **owned by one broker**. In cluster mode, `session_id` embeds the
//! owner `node_id` so a peer can transparent-forward Kafka Fetch (Phase 119). Single-node
//! keeps sequential ids.
//!
//! Phase 138/139: primary owners push best-effort JSON snapshots to peers
//! (`SessionMirrorOp`); peers keep a **foreign mirror** map. Puts are coalesced (one
//! slot per session), Puts may be debounced, Deletes flush immediately, and
//! `mirror_gen` fences stale applies / promote supersede. Optional durable mirrors
//! live under `{data_dir}/__fetch_session_mirrors/state.json`.
//!
//! Phase 143: `promoted_by` lowest-id claim fence breaks dual-promote ties when two
//! peers promote the same equal-freshness snapshot; claim travels in MirrorPut JSON.
//!
//! Phase 146: MirrorPut JSON may be `mode=full` (default, complete topics map) or
//! `mode=delta` (topic upserts + `remove_topic_keys`). Fan-out prefers delta against
//! an in-memory `last_mirrored` cache; opcode **90** unchanged.
//!
//! Phase 147: on owner miss, **serve reads from foreign mirror without promoting** into
//! primary (default). Mirror-only mutations stay on the mirror table (no
//! `queue_mirror_put`).
//!
//! v0.25: when two peers both hold an unclaimed (no exclusive `promoted_by`) primary
//! for the same session id, inbound MirrorPut / [`FetchSessionManager::converge_dual_epoch`]
//! keeps the winner and **demotes** the loser to a mirror. Winner order: higher
//! `mirror_gen`, then higher epoch, then lowest non-zero `promoted_by` / owner id
//! (`0` = no claim, loses). Default **on**; `VOLANT_SESSION_DUAL_EPOCH_CONVERGE=0`
//! disables. Phase 147 single owner-miss serve-from-mirror is unchanged. Not Raft.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

// Re-export so external tests / callers can build [`SessionTopic`] maps.
pub use super::topic_id::TopicWireId;

/// Kafka `FetchSession.INITIAL_EPOCH` — create / full fetch.
pub const INITIAL_EPOCH: i32 = 0;
/// Kafka `FetchSession.FINAL_EPOCH` — close session; no new session.
pub const FINAL_EPOCH: i32 = -1;

/// Default idle TTL for fetch sessions (Phase 95).
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;
/// Default max concurrent fetch sessions (Phase 95).
pub const DEFAULT_MAX_SESSIONS: usize = 1000;

/// On-disk directory name under `data_dir` (Phase 115).
pub const FETCH_SESSIONS_DIR: &str = "__fetch_sessions";
/// On-disk snapshot file name (Phase 115).
pub const FETCH_SESSIONS_FILE: &str = "state.json";
/// File format version (Phase 115).
pub const FETCH_SESSIONS_FILE_VERSION: u32 = 1;

/// On-disk directory for optional durable peer mirrors (Phase 139).
pub const FETCH_SESSION_MIRRORS_DIR: &str = "__fetch_session_mirrors";
/// On-disk mirror snapshot file name (Phase 139).
pub const FETCH_SESSION_MIRRORS_FILE: &str = "state.json";

/// Default Put fan-out min interval ms (Phase 139). `0` = immediate after coalesce.
pub const DEFAULT_MIRROR_PUT_MIN_INTERVAL_MS: u64 = 50;

/// Bit shift for owner `node_id` inside cluster `session_id` (Phase 119).
pub const SESSION_OWNER_SHIFT: u32 = 19;
/// Local counter mask (19 bits) for cluster `session_id` (Phase 119).
pub const SESSION_LOCAL_MASK: i32 = (1 << SESSION_OWNER_SHIFT) - 1; // 0x7FFFF
/// Owner `node_id` mask (12 bits) — supports ids 1..4095.
pub const SESSION_OWNER_MASK: u32 = 0xFFF;

/// Dirty op for best-effort peer fan-out of primary session mutations (Phase 138/139).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMirrorOp {
    /// Snapshot put for `session_id` (full or delta; export via
    /// [`FetchSessionManager::export_mirror_put_bytes`] / [`FetchSessionManager::export_session_bytes`]).
    Put(i32),
    /// Remove foreign mirror for `session_id`.
    Delete(i32),
}

/// Default MirrorPut / durable snapshot mode (`"full"`).
fn default_mirror_mode() -> String {
    "full".to_owned()
}

/// Whether a MirrorPut JSON `mode` field is delta (Phase 146).
fn is_delta_mode(mode: &str) -> bool {
    mode.eq_ignore_ascii_case("delta")
}

/// Whether `a` is strictly newer than `b` for mirror fencing (Phase 139).
///
/// Order: `mirror_gen`, then `epoch`, then `last_activity_ms`.
pub fn session_is_newer(a: &FetchSession, b: &FetchSession) -> bool {
    if a.mirror_gen != b.mirror_gen {
        return a.mirror_gen > b.mirror_gen;
    }
    if a.epoch != b.epoch {
        return a.epoch > b.epoch;
    }
    a.last_activity_ms > b.last_activity_ms
}

/// Whether `incoming` should replace `existing` as source of truth (Phase 139 + 143).
///
/// 1. Strictly newer by [`session_is_newer`] (`mirror_gen` / epoch / activity) wins.
/// 2. Strictly older loses.
/// 3. Equal freshness: lowest non-zero `promoted_by` wins (claim fence). Unclaimed
///    (`0`) keeps existing on a pure tie; claimed equal-fresh beats unclaimed.
pub fn session_claim_wins(incoming: &FetchSession, existing: &FetchSession) -> bool {
    if session_is_newer(incoming, existing) {
        return true;
    }
    if session_is_newer(existing, incoming) {
        return false;
    }
    // Equal freshness: lowest non-zero claim wins.
    match (incoming.promoted_by, existing.promoted_by) {
        (0, 0) => false, // keep existing
        (0, _) => false, // unclaimed does not beat claimed
        (_, 0) => true,  // claimed beats unclaimed at equal freshness
        (a, b) => a < b, // both non-zero: lower id wins
    }
}

/// Effective dual-epoch claim id: `promoted_by` if set, else `owner_id`.
///
/// `0` means no claim (loses to any real id).
pub fn dual_epoch_claim_id(promoted_by: u32, owner_id: u32) -> u32 {
    if promoted_by != 0 {
        promoted_by
    } else {
        owner_id
    }
}

/// Whether `incoming` wins a v0.25 dual-epoch compare against `existing`.
///
/// Order (documented, tested):
/// 1. Higher `mirror_gen` wins (beats a higher epoch on the other side).
/// 2. Else higher session `epoch` wins.
/// 3. Else lowest non-zero claim (`promoted_by`, else `owner_id`) wins.
///    `0` is “no claim” and loses to a real id.
/// 4. Else keep local (`existing`) — returns `false`.
///
/// Does **not** use `last_activity_ms` (unlike [`session_is_newer`]).
pub fn session_dual_epoch_wins(
    incoming: &FetchSession,
    incoming_owner: u32,
    existing: &FetchSession,
    existing_owner: u32,
) -> bool {
    if incoming.mirror_gen != existing.mirror_gen {
        return incoming.mirror_gen > existing.mirror_gen;
    }
    if incoming.epoch != existing.epoch {
        return incoming.epoch > existing.epoch;
    }
    let inc = dual_epoch_claim_id(incoming.promoted_by, incoming_owner);
    let ex = dual_epoch_claim_id(existing.promoted_by, existing_owner);
    match (inc, ex) {
        (0, 0) => false,
        (0, _) => false,
        (_, 0) => true,
        (a, b) => a < b,
    }
}

/// Both copies look primary-ish without an exclusive promote claim (v0.25).
///
/// `promoted_by == 0` on both sides: created / served without Phase 143 claim.
/// Claimed copies stay on the Phase 143 replace-or-reject path.
pub fn both_unclaimed_primary_ish(local: &FetchSession, incoming: &FetchSession) -> bool {
    local.promoted_by == 0 && incoming.promoted_by == 0
}

/// Encode a cluster-mode session id from owner broker and local counter.
pub fn encode_session_id(owner_node_id: u32, local: i32) -> i32 {
    debug_assert!(local > 0 && local <= SESSION_LOCAL_MASK);
    let owner = (owner_node_id & SESSION_OWNER_MASK) as i32;
    (owner << SESSION_OWNER_SHIFT) | (local & SESSION_LOCAL_MASK)
}

/// Extract owner broker id from a cluster-encoded session id (`None` if unencoded).
pub fn decode_session_owner(session_id: i32) -> Option<u32> {
    if session_id <= 0 {
        return None;
    }
    let owner = (session_id as u32) >> SESSION_OWNER_SHIFT;
    if owner == 0 {
        None
    } else {
        Some(owner)
    }
}

/// Local counter portion of a session id (full id when unencoded).
pub fn session_local_part(session_id: i32) -> i32 {
    if decode_session_owner(session_id).is_some() {
        session_id & SESSION_LOCAL_MASK
    } else {
        session_id
    }
}

/// Cached partition fetch parameters inside a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPartition {
    /// Last fetch offset the client requested for this partition.
    pub fetch_offset: i64,
    /// Last current_leader_epoch (-1 = none).
    pub current_leader_epoch: i32,
    /// Last last_fetched_epoch (-1 = none); v12+.
    pub last_fetched_epoch: i32,
    /// Last partition_max_bytes.
    pub max_bytes: usize,
    /// High watermark last included in a successful session response (Phase 91).
    pub last_hwm: Option<i64>,
    /// Last stable offset last included in a successful session response (Phase 91).
    pub last_lso: Option<i64>,
}

impl SessionPartition {
    /// New partition params with no prior response cache.
    pub fn new(
        fetch_offset: i64,
        current_leader_epoch: i32,
        last_fetched_epoch: i32,
        max_bytes: usize,
    ) -> Self {
        Self {
            fetch_offset,
            current_leader_epoch,
            last_fetched_epoch,
            max_bytes,
            last_hwm: None,
            last_lso: None,
        }
    }

    /// Whether an empty, successful response with these offsets can be omitted
    /// (Phase 91). Errors and non-empty records always include.
    pub fn should_omit_unchanged(
        &self,
        hwm: i64,
        lso: i64,
        records_empty: bool,
        error: i16,
    ) -> bool {
        if error != 0 || !records_empty {
            return false;
        }
        match (self.last_hwm, self.last_lso) {
            (Some(prev_hwm), Some(prev_lso)) => prev_hwm == hwm && prev_lso == lso,
            _ => false,
        }
    }
}

/// One topic entry inside a fetch session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTopic {
    /// Wire identity to echo on responses (name or TopicId UUID).
    pub wire: TopicWireId,
    /// Resolved topic name (empty when unknown TopicId).
    pub name: String,
    /// Partitions keyed by partition index.
    pub partitions: HashMap<i32, SessionPartition>,
}

/// One active fetch session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchSession {
    /// Next expected `session_epoch` from the client.
    pub epoch: i32,
    /// Topics keyed by a stable map key (name, or `id:<hex>` for unknown UUID).
    pub topics: HashMap<String, SessionTopic>,
    /// Last successful session Fetch activity (unix ms; Phase 95).
    pub last_activity_ms: i64,
    /// Monotonic mutation generation for peer-mirror fencing (Phase 139).
    pub mirror_gen: u64,
    /// Node id that last claim-promoted this session into primary (Phase 143).
    ///
    /// `0` = never claim-promoted (created on original owner via normal create).
    /// Non-zero = broker that won (or last held) the promote claim fence.
    pub promoted_by: u32,
}

/// Fetch session table with idle TTL + max concurrent (Phase 95) + optional
/// durability (Phase 115) + cluster owner encoding (Phase 119) + peer mirror
/// (Phase 138/139) + promote claim fence (Phase 143) + incremental MirrorPut (Phase 146/147)
/// + dual-epoch converge (v0.25).
#[derive(Debug)]
pub struct FetchSessionManager {
    sessions: Mutex<HashMap<i32, FetchSession>>,
    /// Foreign (non-owned) session mirrors — served without promote by default (Phase 147).
    mirrors: Mutex<HashMap<i32, FetchSession>>,
    /// Pending put/delete ops keyed by session_id (coalesced; Phase 138/139).
    pending_mirror: Mutex<HashMap<i32, SessionMirrorOp>>,
    /// Last successfully exported full/applied primary state for delta fan-out (Phase 146).
    last_mirrored: Mutex<HashMap<i32, FetchSession>>,
    /// Next **local** counter (not necessarily the full session_id).
    next_id: AtomicI32,
    /// Idle TTL in ms; `0` disables idle eviction.
    idle_timeout_ms: AtomicU64,
    /// Max concurrent sessions; `0` = unlimited.
    max_sessions: AtomicUsize,
    /// Total sessions removed by idle TTL or LRU pressure.
    evicted_total: AtomicU64,
    /// Sessions removed by idle TTL only (Phase 97; subset of `evicted_total`).
    idle_evicted_total: AtomicU64,
    /// Optional durable snapshot path (Phase 115).
    durable_path: Option<PathBuf>,
    /// Sessions restored from disk at last open (Phase 115).
    restored: AtomicU64,
    /// Failed durable write attempts (Phase 115).
    persist_errors_total: AtomicU64,
    /// Cluster owner node id for session_id encoding; `0` = sequential (single-node).
    owner_node_id: AtomicU32,
    /// Successful multi-broker Fetch forwards initiated by this broker (Phase 119).
    forward_total: AtomicU64,
    /// Failed multi-broker Fetch forward attempts (Phase 119).
    forward_errors_total: AtomicU64,
    /// Successful `apply_mirror_put` installs (Phase 138).
    mirror_puts_applied_total: AtomicU64,
    /// `apply_mirror_delete` calls that removed a mirror (Phase 138).
    mirror_deletes_applied_total: AtomicU64,
    /// Successful `promote_from_mirror` promotions into primary (Phase 138).
    promote_total: AtomicU64,
    /// Put ops dropped by coalesce (Phase 139).
    mirror_puts_coalesced_total: AtomicU64,
    /// Stale mirror puts rejected by fencing (Phase 139).
    mirror_stale_put_rejects_total: AtomicU64,
    /// Promote path where newer mirror superseded primary (Phase 139).
    promote_supersede_total: AtomicU64,
    /// Mirrors restored from durable snapshot at open (Phase 139).
    mirror_restored: AtomicU64,
    /// Dual-promote / claim-lose rejects (Phase 143).
    promote_claim_reject_total: AtomicU64,
    /// Delta MirrorPut payloads sent or applied (Phase 146).
    mirror_delta_puts_total: AtomicU64,
    /// Owner-miss local serve from mirror without promote (Phase 147).
    serve_from_mirror_total: AtomicU64,
    /// Unclaimed dual-primary demotes (v0.25).
    dual_epoch_converge_total: AtomicU64,
    /// When true, owner miss + mirror → serve without promote (Phase 147; default on).
    serve_mirror_without_promote: AtomicBool,
    /// When true, owner miss + mirror → promote into primary (Phase 147; default off).
    promote_on_miss: AtomicBool,
    /// When true, unclaimed dual-primary MirrorPut/helper demotes the loser (v0.25; default on).
    dual_epoch_converge: AtomicBool,
    /// Min interval between Put fan-outs; `0` = immediate (Phase 139).
    mirror_put_min_interval_ms: AtomicU64,
    /// Single-flight arm for debounced Put fan-out (Phase 139).
    mirror_put_debounce_armed: AtomicBool,
    /// Optional durable path for foreign mirrors (Phase 139).
    mirror_durable_path: Option<PathBuf>,
}

impl Default for FetchSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchSessionManager {
    /// Manager with defaults from env (`VOLANT_FETCH_SESSION_*`) — **no durability**.
    ///
    /// Prefer [`Self::open`] for production brokers with a `data_dir`.
    pub fn new() -> Self {
        Self::with_limits(default_idle_timeout_ms(), default_max_sessions())
    }

    /// Manager with explicit limits and no durable path (unit tests).
    pub fn with_limits(idle_timeout_ms: u64, max_sessions: usize) -> Self {
        Self::with_limits_and_owner(idle_timeout_ms, max_sessions, 0)
    }

    /// Manager with limits and optional cluster owner encoding (Phase 119 tests).
    pub fn with_limits_and_owner(
        idle_timeout_ms: u64,
        max_sessions: usize,
        owner_node_id: u32,
    ) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            mirrors: Mutex::new(HashMap::new()),
            pending_mirror: Mutex::new(HashMap::new()),
            last_mirrored: Mutex::new(HashMap::new()),
            next_id: AtomicI32::new(1),
            idle_timeout_ms: AtomicU64::new(idle_timeout_ms),
            max_sessions: AtomicUsize::new(max_sessions),
            evicted_total: AtomicU64::new(0),
            idle_evicted_total: AtomicU64::new(0),
            durable_path: None,
            restored: AtomicU64::new(0),
            persist_errors_total: AtomicU64::new(0),
            owner_node_id: AtomicU32::new(owner_node_id),
            forward_total: AtomicU64::new(0),
            forward_errors_total: AtomicU64::new(0),
            mirror_puts_applied_total: AtomicU64::new(0),
            mirror_deletes_applied_total: AtomicU64::new(0),
            promote_total: AtomicU64::new(0),
            mirror_puts_coalesced_total: AtomicU64::new(0),
            mirror_stale_put_rejects_total: AtomicU64::new(0),
            promote_supersede_total: AtomicU64::new(0),
            mirror_restored: AtomicU64::new(0),
            promote_claim_reject_total: AtomicU64::new(0),
            mirror_delta_puts_total: AtomicU64::new(0),
            serve_from_mirror_total: AtomicU64::new(0),
            dual_epoch_converge_total: AtomicU64::new(0),
            serve_mirror_without_promote: AtomicBool::new(default_serve_mirror_without_promote()),
            promote_on_miss: AtomicBool::new(default_promote_on_miss()),
            dual_epoch_converge: AtomicBool::new(default_dual_epoch_converge()),
            mirror_put_min_interval_ms: AtomicU64::new(default_mirror_put_min_interval_ms()),
            mirror_put_debounce_armed: AtomicBool::new(false),
            mirror_durable_path: None,
        }
    }

    /// Open (or create) a durable session table under `data_dir/__fetch_sessions`.
    ///
    /// Loads existing sessions, applying idle TTL at load time (Phase 115).
    pub fn open(data_dir: impl AsRef<Path>) -> Self {
        Self::open_with_owner(data_dir, 0)
    }

    /// Open durable sessions with cluster owner encoding (Phase 119).
    pub fn open_with_owner(data_dir: impl AsRef<Path>, owner_node_id: u32) -> Self {
        Self::open_with_limits_and_owner(
            data_dir,
            default_idle_timeout_ms(),
            default_max_sessions(),
            owner_node_id,
        )
    }

    /// Open durable sessions with explicit idle/max limits (tests / controlled boot).
    pub fn open_with_limits(
        data_dir: impl AsRef<Path>,
        idle_timeout_ms: u64,
        max_sessions: usize,
    ) -> Self {
        Self::open_with_limits_and_owner(data_dir, idle_timeout_ms, max_sessions, 0)
    }

    /// Open durable sessions with limits + owner encoding (Phase 119).
    ///
    /// When `VOLANT_FETCH_SESSION_MIRROR_DURABLE` is enabled, also loads optional
    /// durable peer mirrors from `__fetch_session_mirrors` (Phase 139).
    pub fn open_with_limits_and_owner(
        data_dir: impl AsRef<Path>,
        idle_timeout_ms: u64,
        max_sessions: usize,
        owner_node_id: u32,
    ) -> Self {
        let mut mgr = Self::with_limits_and_owner(idle_timeout_ms, max_sessions, owner_node_id);
        let dir = data_dir.as_ref().join(FETCH_SESSIONS_DIR);
        let _ = fs::create_dir_all(&dir);
        mgr.durable_path = Some(dir.join(FETCH_SESSIONS_FILE));
        mgr.load_from_disk_at(now_ms());
        if default_mirror_durable_enabled() {
            mgr.enable_mirror_durable(data_dir.as_ref());
        }
        mgr
    }

    /// Enable durable foreign mirrors under `data_dir/__fetch_session_mirrors` (Phase 139).
    ///
    /// Loads existing mirrors with idle TTL filter. Safe for unit tests without env.
    pub fn enable_mirror_durable(&mut self, data_dir: impl AsRef<Path>) {
        let dir = data_dir.as_ref().join(FETCH_SESSION_MIRRORS_DIR);
        let _ = fs::create_dir_all(&dir);
        self.mirror_durable_path = Some(dir.join(FETCH_SESSION_MIRRORS_FILE));
        self.load_mirrors_from_disk_at(now_ms());
    }

    /// Whether foreign mirrors are persisted to disk (Phase 139).
    pub fn is_mirror_durable(&self) -> bool {
        self.mirror_durable_path.is_some()
    }

    /// Cluster owner node id used for session_id encoding (`0` = sequential).
    pub fn owner_node_id(&self) -> u32 {
        self.owner_node_id.load(Ordering::Relaxed)
    }

    /// Set cluster owner for subsequent allocations (Phase 119). Prefer
    /// [`Self::open_with_owner`] at boot so restored `next_id` is consistent.
    pub fn set_owner_node_id(&self, owner_node_id: u32) {
        self.owner_node_id.store(owner_node_id, Ordering::Relaxed);
    }

    /// Whether a live session exists for `session_id` (after prior evictions on ops).
    pub fn contains(&self, session_id: i32) -> bool {
        self.sessions.lock().contains_key(&session_id)
    }

    /// Successful transparent Fetch forwards from this broker (Phase 119).
    pub fn forward_total(&self) -> u64 {
        self.forward_total.load(Ordering::Relaxed)
    }

    /// Failed transparent Fetch forward attempts (Phase 119).
    pub fn forward_errors_total(&self) -> u64 {
        self.forward_errors_total.load(Ordering::Relaxed)
    }

    /// Record a successful multi-broker Fetch forward (Phase 119).
    pub fn record_forward_ok(&self) {
        self.forward_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed multi-broker Fetch forward (Phase 119).
    pub fn record_forward_error(&self) {
        self.forward_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain pending mirror put/delete ops for peer fan-out (Phase 138/139).
    ///
    /// Returns **Deletes first**, then Puts, each group stable-sorted by `session_id`.
    pub fn drain_mirror_ops(&self) -> Vec<SessionMirrorOp> {
        let mut map = std::mem::take(&mut *self.pending_mirror.lock());
        if map.is_empty() {
            return Vec::new();
        }
        let mut ids: Vec<i32> = map.keys().copied().collect();
        ids.sort_unstable();
        let mut deletes = Vec::new();
        let mut puts = Vec::new();
        for id in ids {
            match map.remove(&id) {
                Some(SessionMirrorOp::Delete(sid)) => deletes.push(SessionMirrorOp::Delete(sid)),
                Some(SessionMirrorOp::Put(sid)) => puts.push(SessionMirrorOp::Put(sid)),
                None => {}
            }
        }
        deletes.extend(puts);
        deletes
    }

    /// Whether any mirror ops are pending fan-out (Phase 138).
    pub fn has_pending_mirror_ops(&self) -> bool {
        !self.pending_mirror.lock().is_empty()
    }

    /// Whether a Delete is pending (flushes immediately; Phase 139).
    pub fn has_pending_mirror_delete(&self) -> bool {
        self.pending_mirror
            .lock()
            .values()
            .any(|op| matches!(op, SessionMirrorOp::Delete(_)))
    }

    /// Put fan-out min interval ms (`0` = immediate after coalesce; Phase 139).
    pub fn mirror_put_min_interval_ms(&self) -> u64 {
        self.mirror_put_min_interval_ms.load(Ordering::Relaxed)
    }

    /// Override Put fan-out min interval ms (Phase 139 tests / runtime).
    pub fn set_mirror_put_min_interval_ms(&self, ms: u64) {
        self.mirror_put_min_interval_ms.store(ms, Ordering::Relaxed);
    }

    /// Try to arm single-flight Put debounce. Returns `true` if this caller armed it.
    pub fn try_arm_mirror_put_debounce(&self) -> bool {
        self.mirror_put_debounce_armed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Clear Put debounce arm after a delayed flush (Phase 139).
    pub fn clear_mirror_put_debounce_armed(&self) {
        self.mirror_put_debounce_armed
            .store(false, Ordering::Release);
    }

    /// JSON bytes of one primary session as a **full** snapshot (`mode=full`) for MirrorPut.
    ///
    /// Prefer [`Self::export_mirror_put_bytes`] on the fan-out path so peers receive
    /// deltas when a prior export is cached (Phase 146).
    pub fn export_session_bytes(&self, session_id: i32) -> Option<Vec<u8>> {
        let guard = self.sessions.lock();
        let session = guard.get(&session_id)?;
        let stored = session_to_stored(session_id, session);
        serde_json::to_vec(&stored).ok()
    }

    /// Export a MirrorPut payload as full or delta relative to `prev` (Phase 146).
    ///
    /// - `prev == None` → full snapshot (`mode=full`).
    /// - Topics equal → metadata-only delta (empty upserts / removes).
    /// - Topics differ → delta with upserted topic keys + `remove_topic_keys` for gone keys.
    ///
    /// Always includes current `mirror_gen` / `epoch` / `last_activity_ms` / `promoted_by`.
    pub fn export_session_delta_bytes(
        &self,
        session_id: i32,
        prev: Option<&FetchSession>,
    ) -> Option<Vec<u8>> {
        let guard = self.sessions.lock();
        let session = guard.get(&session_id)?;
        let stored = match prev {
            None => session_to_stored(session_id, session),
            Some(prev_sess) => session_to_delta_stored(session_id, session, prev_sess),
        };
        serde_json::to_vec(&stored).ok()
    }

    /// Export MirrorPut payload for fan-out, preferring delta vs [`Self::last_mirrored`] (Phase 146).
    ///
    /// Returns `(payload_bytes, is_delta)`. On success the caller should
    /// [`Self::note_last_mirrored`] after scheduling the export.
    pub fn export_mirror_put_bytes(&self, session_id: i32) -> Option<(Vec<u8>, bool)> {
        let prev = self.last_mirrored.lock().get(&session_id).cloned();
        let is_delta = prev.is_some();
        let bytes = self.export_session_delta_bytes(session_id, prev.as_ref())?;
        Some((bytes, is_delta))
    }

    /// Record the primary session as last-exported for future deltas (Phase 146).
    ///
    /// Call after a successful MirrorPut export schedule. No-op when primary missing.
    pub fn note_last_mirrored(&self, session_id: i32) {
        let session = {
            let guard = self.sessions.lock();
            match guard.get(&session_id) {
                Some(s) => s.clone(),
                None => return,
            }
        };
        self.last_mirrored.lock().insert(session_id, session);
    }

    /// Drop cached last-exported state for `session_id` (Phase 146).
    pub fn clear_last_mirrored(&self, session_id: i32) {
        self.last_mirrored.lock().remove(&session_id);
    }

    /// Install/replace a foreign mirror from a JSON snapshot (Phase 138/139/143/146/v0.25 Put).
    ///
    /// Fencing: [`session_claim_wins`] — newer `mirror_gen`/epoch/activity, else
    /// lowest non-zero `promoted_by` on equal freshness. Stale or claim-losing puts
    /// do not clobber primary or mirror. When primary exists and the incoming
    /// snapshot wins a **claimed** (Phase 143) compare, primary is replaced.
    ///
    /// v0.25: when local primary and incoming **both** look unclaimed primary-ish
    /// (`promoted_by == 0`) and dual-epoch converge is on, keep the
    /// [`session_dual_epoch_wins`] winner and **demote** the local loser to a
    /// mirror (does not stay owner). Disable with
    /// `VOLANT_SESSION_DUAL_EPOCH_CONVERGE=0`.
    ///
    /// Phase 146: `mode=delta` merges topic upserts into existing primary or mirror
    /// and applies `remove_topic_keys`. Missing base installs upserts as full state.
    /// Absent / `"full"` mode replaces topics wholesale (legacy path).
    pub fn apply_mirror_put(&self, snapshot: &[u8]) -> Result<(), String> {
        let stored: StoredFetchSession =
            serde_json::from_slice(snapshot).map_err(|e| e.to_string())?;
        if stored.id <= 0 {
            return Err("invalid session id".to_owned());
        }
        let is_delta = is_delta_mode(&stored.mode);
        let (id, session) = if is_delta {
            self.resolve_delta_put(stored)
                .ok_or_else(|| "invalid delta session snapshot".to_owned())?
        } else {
            stored_to_session(stored).ok_or_else(|| "invalid session snapshot".to_owned())?
        };

        // Lock order: sessions then mirrors.
        {
            let mut sessions = self.sessions.lock();
            if sessions.contains_key(&id) {
                let primary = sessions.get(&id).expect("contains_key");
                let dual = self.dual_epoch_converge_enabled()
                    && both_unclaimed_primary_ish(primary, &session);
                if dual {
                    let incoming_wins = session_dual_epoch_wins(
                        &session,
                        session.promoted_by,
                        primary,
                        self.owner_node_id(),
                    );
                    if incoming_wins {
                        sessions.remove(&id);
                        self.persist_locked(&sessions);
                        drop(sessions);
                        let mut mirrors = self.mirrors.lock();
                        mirrors.insert(id, session);
                        self.persist_mirrors_locked(&mirrors);
                        drop(mirrors);
                        self.clear_last_mirrored(id);
                        self.dual_epoch_converge_total
                            .fetch_add(1, Ordering::Relaxed);
                        self.mirror_puts_applied_total
                            .fetch_add(1, Ordering::Relaxed);
                        if is_delta {
                            self.mirror_delta_puts_total.fetch_add(1, Ordering::Relaxed);
                        }
                        return Ok(());
                    }
                    self.record_put_reject(sessions.get(&id).expect("contains_key"), &session);
                    return Ok(());
                }
                let wins = session_claim_wins(&session, primary);
                if !wins {
                    self.record_put_reject(sessions.get(&id).expect("contains_key"), &session);
                    return Ok(());
                }
                sessions.insert(id, session);
                self.persist_locked(&sessions);
                drop(sessions);
                let mut mirrors = self.mirrors.lock();
                if mirrors.remove(&id).is_some() {
                    self.persist_mirrors_locked(&mirrors);
                }
                self.mirror_puts_applied_total
                    .fetch_add(1, Ordering::Relaxed);
                if is_delta {
                    self.mirror_delta_puts_total.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }

        let mut mirrors = self.mirrors.lock();
        let wins = mirrors
            .get(&id)
            .map(|existing| session_claim_wins(&session, existing))
            .unwrap_or(true);
        if !wins {
            self.record_put_reject(mirrors.get(&id).expect("get after !wins"), &session);
            return Ok(());
        }
        mirrors.insert(id, session);
        self.persist_mirrors_locked(&mirrors);
        self.mirror_puts_applied_total
            .fetch_add(1, Ordering::Relaxed);
        if is_delta {
            self.mirror_delta_puts_total.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Build the post-merge session for a delta MirrorPut (Phase 146).
    ///
    /// Base = primary if present, else mirror, else empty topics. Upserts merge into
    /// the topics map; `remove_topic_keys` drop keys; metadata fields come from payload.
    fn resolve_delta_put(&self, stored: StoredFetchSession) -> Option<(i32, FetchSession)> {
        let id = stored.id;
        let remove_keys = stored.remove_topic_keys.clone();
        let upserts = stored_topics_to_map(stored.topics)?;
        let epoch = stored.epoch;
        let last_activity_ms = stored.last_activity_ms;
        let mirror_gen = stored.mirror_gen;
        let promoted_by = stored.promoted_by;

        // Prefer primary base, then mirror (lock order: sessions then mirrors).
        let base_topics = {
            let sessions = self.sessions.lock();
            if let Some(primary) = sessions.get(&id) {
                primary.topics.clone()
            } else {
                drop(sessions);
                let mirrors = self.mirrors.lock();
                mirrors
                    .get(&id)
                    .map(|m| m.topics.clone())
                    .unwrap_or_default()
            }
        };

        let mut topics = base_topics;
        for (k, v) in upserts {
            topics.insert(k, v);
        }
        for k in remove_keys {
            topics.remove(&k);
        }

        Some((
            id,
            FetchSession {
                epoch,
                topics,
                last_activity_ms,
                mirror_gen,
                promoted_by,
            },
        ))
    }

    /// Classify a losing put as stale-gen vs claim-fence reject (Phase 139/143).
    fn record_put_reject(&self, existing: &FetchSession, incoming: &FetchSession) {
        if session_is_newer(existing, incoming) {
            self.mirror_stale_put_rejects_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            // Equal freshness (or incoming newer-by-activity but lost? only equal here).
            self.promote_claim_reject_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Remove a foreign mirror (Phase 138 Delete). No-op if missing.
    pub fn apply_mirror_delete(&self, session_id: i32) {
        let mut mirrors = self.mirrors.lock();
        if mirrors.remove(&session_id).is_some() {
            self.persist_mirrors_locked(&mirrors);
            self.mirror_deletes_applied_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Move mirror → primary, or supersede primary when mirror wins claim (Phase 138/139/143).
    ///
    /// Returns `true` if primary ends up holding the id (already present, promoted,
    /// or superseded). Returns `false` when neither primary nor mirror has the id.
    ///
    /// On empty-primary promote, stamps `promoted_by` with local [`Self::owner_node_id`]
    /// when non-zero (Phase 143 claim fence).
    pub fn promote_from_mirror(&self, session_id: i32) -> bool {
        // Lock order: sessions then mirrors.
        let mut sessions = self.sessions.lock();
        let mut mirrors = self.mirrors.lock();
        let mirror = mirrors.remove(&session_id);
        let owner = self.owner_node_id.load(Ordering::Relaxed);

        if sessions.contains_key(&session_id) {
            if let Some(mut m) = mirror {
                let supersede = sessions
                    .get(&session_id)
                    .map(|primary| session_claim_wins(&m, primary))
                    .unwrap_or(false);
                if supersede {
                    // Claim stamp: keep mirror claim if set; else local owner.
                    if m.promoted_by == 0 && owner > 0 {
                        m.promoted_by = owner;
                    }
                    sessions.insert(session_id, m);
                    self.persist_locked(&sessions);
                    self.persist_mirrors_locked(&mirrors);
                    drop(mirrors);
                    drop(sessions);
                    self.promote_supersede_total.fetch_add(1, Ordering::Relaxed);
                    self.promote_total.fetch_add(1, Ordering::Relaxed);
                    self.queue_mirror_put(session_id);
                    return true;
                }
                // Lost claim or older: drop mirror; count claim reject when equal
                // freshness and claim lost (not pure stale gen).
                if let Some(primary) = sessions.get(&session_id) {
                    if !session_is_newer(primary, &m) && !session_is_newer(&m, primary) {
                        self.promote_claim_reject_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.persist_mirrors_locked(&mirrors);
            }
            return true;
        }

        let Some(mut session) = mirror else {
            return false;
        };
        // Empty primary: stamp local owner only when mirror is unclaimed; keep a
        // prior winning claim that arrived via MirrorPut.
        if session.promoted_by == 0 && owner > 0 {
            session.promoted_by = owner;
        }
        sessions.insert(session_id, session);
        self.persist_locked(&sessions);
        self.persist_mirrors_locked(&mirrors);
        drop(mirrors);
        drop(sessions);
        self.promote_total.fetch_add(1, Ordering::Relaxed);
        // New local SoT — re-queue Put for peers (cluster only).
        self.queue_mirror_put(session_id);
        true
    }

    /// Whether a foreign mirror exists for `session_id` (Phase 138).
    pub fn mirror_contains(&self, session_id: i32) -> bool {
        self.mirrors.lock().contains_key(&session_id)
    }

    /// Number of foreign mirrors (Phase 138).
    pub fn mirrored_count(&self) -> usize {
        self.mirrors.lock().len()
    }

    /// Successful mirror put installs applied on this broker (Phase 138).
    pub fn mirror_puts_applied_total(&self) -> u64 {
        self.mirror_puts_applied_total.load(Ordering::Relaxed)
    }

    /// Successful mirror deletes applied on this broker (Phase 138).
    pub fn mirror_deletes_applied_total(&self) -> u64 {
        self.mirror_deletes_applied_total.load(Ordering::Relaxed)
    }

    /// Successful mirror → primary promotions (Phase 138).
    pub fn promote_total(&self) -> u64 {
        self.promote_total.load(Ordering::Relaxed)
    }

    /// Puts dropped by pending-op coalesce (Phase 139).
    pub fn mirror_puts_coalesced_total(&self) -> u64 {
        self.mirror_puts_coalesced_total.load(Ordering::Relaxed)
    }

    /// Stale mirror puts rejected by fencing (Phase 139).
    pub fn mirror_stale_put_rejects_total(&self) -> u64 {
        self.mirror_stale_put_rejects_total.load(Ordering::Relaxed)
    }

    /// Promotions where a newer mirror superseded primary (Phase 139).
    pub fn promote_supersede_total(&self) -> u64 {
        self.promote_supersede_total.load(Ordering::Relaxed)
    }

    /// Mirrors restored from durable snapshot at open (Phase 139).
    pub fn mirror_restored(&self) -> u64 {
        self.mirror_restored.load(Ordering::Relaxed)
    }

    /// Dual-promote / claim-lose rejects (Phase 143).
    pub fn promote_claim_reject_total(&self) -> u64 {
        self.promote_claim_reject_total.load(Ordering::Relaxed)
    }

    /// Delta MirrorPut payloads sent or applied (Phase 146).
    pub fn mirror_delta_puts_total(&self) -> u64 {
        self.mirror_delta_puts_total.load(Ordering::Relaxed)
    }

    /// Record that a delta MirrorPut was scheduled for fan-out (Phase 146).
    pub fn record_mirror_delta_put_sent(&self) {
        self.mirror_delta_puts_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Owner-miss serves that used foreign mirror without promote (Phase 147).
    pub fn serve_from_mirror_total(&self) -> u64 {
        self.serve_from_mirror_total.load(Ordering::Relaxed)
    }

    /// Record a serve-from-mirror decision in `maybe_forward` (Phase 147).
    pub fn record_serve_from_mirror(&self) {
        self.serve_from_mirror_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether owner miss prefers mirror serve without promote (Phase 147).
    pub fn serve_mirror_without_promote(&self) -> bool {
        self.serve_mirror_without_promote.load(Ordering::Relaxed)
    }

    /// Override serve-without-promote knob (Phase 147 tests / runtime).
    pub fn set_serve_mirror_without_promote(&self, enabled: bool) {
        self.serve_mirror_without_promote
            .store(enabled, Ordering::Relaxed);
    }

    /// Whether owner miss forces promote into primary (Phase 147).
    pub fn promote_on_miss(&self) -> bool {
        self.promote_on_miss.load(Ordering::Relaxed)
    }

    /// Override promote-on-miss knob (Phase 147 tests / runtime).
    pub fn set_promote_on_miss(&self, enabled: bool) {
        self.promote_on_miss.store(enabled, Ordering::Relaxed);
    }

    /// Unclaimed dual-primary demotes since process start (v0.25).
    pub fn dual_epoch_converge_total(&self) -> u64 {
        self.dual_epoch_converge_total.load(Ordering::Relaxed)
    }

    /// Whether dual-epoch converge is enabled (v0.25; default on).
    pub fn dual_epoch_converge_enabled(&self) -> bool {
        self.dual_epoch_converge.load(Ordering::Relaxed)
    }

    /// Override dual-epoch converge knob (v0.25 tests / runtime).
    pub fn set_dual_epoch_converge(&self, enabled: bool) {
        self.dual_epoch_converge.store(enabled, Ordering::Relaxed);
    }

    /// Clone the primary session if present (v0.25 tests / helper).
    pub fn primary_session_clone(&self, session_id: i32) -> Option<FetchSession> {
        self.sessions.lock().get(&session_id).cloned()
    }

    /// Insert or replace a primary session without stamping `promoted_by`.
    ///
    /// Used by dual-epoch fixtures and tests so two managers can hold the same
    /// `session_id` as unclaimed primaries. Removes a foreign mirror for `id`.
    pub fn install_primary(&self, session_id: i32, session: FetchSession) {
        {
            let mut sessions = self.sessions.lock();
            sessions.insert(session_id, session);
            self.persist_locked(&sessions);
        }
        let mut mirrors = self.mirrors.lock();
        if mirrors.remove(&session_id).is_some() {
            self.persist_mirrors_locked(&mirrors);
        }
    }

    /// If local primary and `incoming` both look unclaimed primary-ish, keep the
    /// winner and demote the local loser to a mirror of `incoming`.
    ///
    /// Returns `true` when local was demoted. No-op (returns `false`) when the
    /// knob is off, local is missing, either side has an exclusive promote
    /// claim, or local wins / ties.
    pub fn converge_dual_epoch(&self, session_id: i32, incoming: &FetchSession) -> bool {
        if !self.dual_epoch_converge_enabled() {
            return false;
        }
        let Some(local) = self.primary_session_clone(session_id) else {
            return false;
        };
        if !both_unclaimed_primary_ish(&local, incoming) {
            return false;
        }
        if !session_dual_epoch_wins(incoming, incoming.promoted_by, &local, self.owner_node_id()) {
            return false;
        }
        self.demote_primary_to_winner_mirror(session_id, incoming)
    }

    /// Demote local primary to a mirror of `winner`. Increments the converge metric.
    fn demote_primary_to_winner_mirror(&self, session_id: i32, winner: &FetchSession) -> bool {
        let mut sessions = self.sessions.lock();
        if sessions.remove(&session_id).is_none() {
            return false;
        }
        self.persist_locked(&sessions);
        drop(sessions);
        let mut mirrors = self.mirrors.lock();
        mirrors.insert(session_id, winner.clone());
        self.persist_mirrors_locked(&mirrors);
        drop(mirrors);
        self.clear_last_mirrored(session_id);
        self.dual_epoch_converge_total
            .fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Clone foreign mirror session if present (Phase 147).
    pub fn mirror_session_clone(&self, session_id: i32) -> Option<FetchSession> {
        self.mirrors.lock().get(&session_id).cloned()
    }

    /// Primary or foreign mirror holds `session_id` (Phase 147).
    pub fn has_servable_session(&self, session_id: i32) -> bool {
        self.contains(session_id) || self.mirror_contains(session_id)
    }

    /// Owner-miss: serve from mirror without promote (default) or promote (opt-in/legacy).
    ///
    /// Returns `true` when the caller should return `None` so local `encode_fetch` serves.
    /// Returns `false` when neither mirror nor promote can help (caller emits **70**).
    ///
    /// v0.25: this single owner-miss path is unchanged. Dual-epoch converge runs on
    /// inbound MirrorPut / [`Self::converge_dual_epoch`] when a second unclaimed
    /// primary is observed — not here.
    pub fn try_owner_miss_local_serve(&self, session_id: i32) -> bool {
        if !self.mirror_contains(session_id) {
            return false;
        }
        if self.promote_on_miss() {
            return self.promote_from_mirror(session_id);
        }
        if self.serve_mirror_without_promote() {
            self.record_serve_from_mirror();
            return true;
        }
        // SERVE_MIRROR_WITHOUT_PROMOTE=0 legacy → promote.
        self.promote_from_mirror(session_id)
    }

    /// Queue Put for peer fan-out when this broker is a cluster owner (Phase 138/139).
    ///
    /// One slot per `session_id`; a second Put coalesces (counts drop). Put after
    /// Delete wins the slot.
    fn queue_mirror_put(&self, session_id: i32) {
        if self.owner_node_id.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut pending = self.pending_mirror.lock();
        match pending.get(&session_id) {
            Some(SessionMirrorOp::Put(_)) => {
                self.mirror_puts_coalesced_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(SessionMirrorOp::Delete(_)) | None => {
                pending.insert(session_id, SessionMirrorOp::Put(session_id));
            }
        }
    }

    /// Queue Delete for peer fan-out when this broker is a cluster owner (Phase 138/139).
    ///
    /// Delete supersedes a pending Put (counts as a coalesced put drop).
    fn queue_mirror_delete(&self, session_id: i32) {
        if self.owner_node_id.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut pending = self.pending_mirror.lock();
        if matches!(pending.get(&session_id), Some(SessionMirrorOp::Put(_))) {
            self.mirror_puts_coalesced_total
                .fetch_add(1, Ordering::Relaxed);
        }
        pending.insert(session_id, SessionMirrorOp::Delete(session_id));
    }

    /// Current idle timeout in milliseconds (`0` = disabled).
    pub fn idle_timeout_ms(&self) -> u64 {
        self.idle_timeout_ms.load(Ordering::Relaxed)
    }

    /// Override idle timeout (`0` disables idle eviction).
    pub fn set_idle_timeout_ms(&self, ms: u64) {
        self.idle_timeout_ms.store(ms, Ordering::Relaxed);
    }

    /// Current max concurrent sessions (`0` = unlimited).
    pub fn max_sessions(&self) -> usize {
        self.max_sessions.load(Ordering::Relaxed)
    }

    /// Override max concurrent sessions (`0` = unlimited).
    pub fn set_max_sessions(&self, max: usize) {
        self.max_sessions.store(max, Ordering::Relaxed);
    }

    /// Live session count (after any prior eviction on this manager).
    pub fn active_count(&self) -> usize {
        self.sessions.lock().len()
    }

    /// Total idle + LRU evictions since process start.
    pub fn evicted_total(&self) -> u64 {
        self.evicted_total.load(Ordering::Relaxed)
    }

    /// Idle-TTL evictions only (Phase 97; subset of [`Self::evicted_total`]).
    pub fn idle_evicted_total(&self) -> u64 {
        self.idle_evicted_total.load(Ordering::Relaxed)
    }

    /// Sessions restored from durable snapshot at open (Phase 115).
    pub fn restored(&self) -> u64 {
        self.restored.load(Ordering::Relaxed)
    }

    /// Durable persist failures (Phase 115).
    pub fn persist_errors_total(&self) -> u64 {
        self.persist_errors_total.load(Ordering::Relaxed)
    }

    /// Whether this manager writes a durable snapshot (Phase 115).
    pub fn is_durable(&self) -> bool {
        self.durable_path.is_some()
    }

    /// Evict idle sessions using the current wall clock (Phase 97 sweeper).
    ///
    /// Same path as lazy idle eviction on create / begin_incremental.
    /// Returns the number of sessions removed.
    pub fn evict_idle_now(&self) -> usize {
        self.evict_idle_at(now_ms())
    }

    /// Evict idle sessions at an explicit timestamp (tests / sweeper).
    pub fn evict_idle_at(&self, now_ms: i64) -> usize {
        let mut guard = self.sessions.lock();
        let before = guard.len();
        self.evict_idle_locked(&mut guard, now_ms);
        let removed = before.saturating_sub(guard.len());
        if removed > 0 {
            self.persist_locked(&guard);
        }
        removed
    }

    fn alloc_id(&self) -> i32 {
        let owner = self.owner_node_id.load(Ordering::Relaxed);
        // Skip 0 (INVALID_SESSION_ID). Wrap into positive / local range if needed.
        loop {
            let local = self.next_id.fetch_add(1, Ordering::Relaxed);
            if owner == 0 {
                if local > 0 {
                    return local;
                }
            } else if local > 0 && local <= SESSION_LOCAL_MASK {
                return encode_session_id(owner, local);
            }
            // Overflow / non-positive / past local mask: reset and retry.
            let _ = self.next_id.compare_exchange(
                local.wrapping_add(1),
                1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    /// Stable map key for a session topic.
    pub fn topic_key(wire: &TopicWireId, name: &str) -> String {
        match wire {
            TopicWireId::Name(n) => n.clone(),
            TopicWireId::Uuid(u) => {
                if name.is_empty() {
                    format!("id:{}", hex::encode(u))
                } else {
                    name.to_owned()
                }
            }
        }
    }

    /// Close a session (no-op for id 0 / missing). Not counted as eviction.
    ///
    /// Primary close queues MirrorDelete (Phase 138). Mirror-only close removes
    /// the foreign entry in-place without fan-out (Phase 147; not SoT owner).
    pub fn close(&self, session_id: i32) {
        if session_id == 0 {
            return;
        }
        {
            let mut guard = self.sessions.lock();
            if guard.remove(&session_id).is_some() {
                self.persist_locked(&guard);
                drop(guard);
                self.clear_last_mirrored(session_id);
                self.queue_mirror_delete(session_id);
                return;
            }
        }
        let mut mirrors = self.mirrors.lock();
        if mirrors.remove(&session_id).is_some() {
            self.persist_mirrors_locked(&mirrors);
        }
    }

    /// Create a new session with `expected_epoch = 1`. Returns assigned id.
    ///
    /// Lazy-evicts idle sessions, then LRU-evicts if at max capacity.
    pub fn create(&self, topics: HashMap<String, SessionTopic>) -> i32 {
        self.create_at(topics, now_ms())
    }

    /// Create with an explicit timestamp (unit tests).
    pub fn create_at(&self, topics: HashMap<String, SessionTopic>, now_ms: i64) -> i32 {
        let mut guard = self.sessions.lock();
        self.evict_idle_locked(&mut guard, now_ms);
        self.evict_lru_if_full_locked(&mut guard);
        let id = self.alloc_id();
        guard.insert(
            id,
            FetchSession {
                epoch: 1,
                topics,
                last_activity_ms: now_ms,
                mirror_gen: 1,
                promoted_by: 0,
            },
        );
        self.persist_locked(&guard);
        drop(guard);
        self.queue_mirror_put(id);
        id
    }

    /// Validate incremental request epoch and advance expected epoch (**primary only**).
    ///
    /// Lazy-evicts idle sessions first. Returns `Ok(())` or Kafka top-level
    /// error code (70 / 71). Touches activity on success.
    ///
    /// Prefer [`Self::begin_incremental_from_any`] on the encode path (Phase 147).
    pub fn begin_incremental(&self, session_id: i32, epoch: i32) -> Result<(), i16> {
        self.begin_incremental_at(session_id, epoch, now_ms())
    }

    /// Incremental begin with explicit timestamp (unit tests; primary only).
    pub fn begin_incremental_at(
        &self,
        session_id: i32,
        epoch: i32,
        now_ms: i64,
    ) -> Result<(), i16> {
        let mut guard = self.sessions.lock();
        let before = guard.len();
        self.evict_idle_locked(&mut guard, now_ms);
        let idle_removed = guard.len() != before;
        let Some(session) = guard.get_mut(&session_id) else {
            if idle_removed {
                self.persist_locked(&guard);
            }
            return Err(70); // FETCH_SESSION_ID_NOT_FOUND
        };
        if session.epoch != epoch {
            // Idle removals still need a durable snapshot.
            if idle_removed {
                self.persist_locked(&guard);
            }
            return Err(71); // INVALID_FETCH_SESSION_EPOCH
        }
        session.epoch = next_epoch(epoch);
        session.last_activity_ms = now_ms;
        session.mirror_gen = session.mirror_gen.saturating_add(1);
        self.persist_locked(&guard);
        drop(guard);
        self.queue_mirror_put(session_id);
        Ok(())
    }

    /// Incremental begin: primary first, else foreign mirror without promote (Phase 147).
    ///
    /// Mirror path advances epoch / activity / `mirror_gen` in-place and persists
    /// mirrors when durable — **no** `queue_mirror_put` and **no** primary insert.
    pub fn begin_incremental_from_any(&self, session_id: i32, epoch: i32) -> Result<(), i16> {
        self.begin_incremental_from_any_at(session_id, epoch, now_ms())
    }

    /// [`Self::begin_incremental_from_any`] with explicit timestamp (tests).
    pub fn begin_incremental_from_any_at(
        &self,
        session_id: i32,
        epoch: i32,
        now_ms: i64,
    ) -> Result<(), i16> {
        // Primary path (same as begin_incremental_at, including idle eviction).
        {
            let mut guard = self.sessions.lock();
            let before = guard.len();
            self.evict_idle_locked(&mut guard, now_ms);
            let idle_removed = guard.len() != before;
            if let Some(session) = guard.get_mut(&session_id) {
                if session.epoch != epoch {
                    if idle_removed {
                        self.persist_locked(&guard);
                    }
                    return Err(71);
                }
                session.epoch = next_epoch(epoch);
                session.last_activity_ms = now_ms;
                session.mirror_gen = session.mirror_gen.saturating_add(1);
                self.persist_locked(&guard);
                drop(guard);
                self.queue_mirror_put(session_id);
                return Ok(());
            }
            if idle_removed {
                self.persist_locked(&guard);
            }
        }
        // Mirror-only (Phase 147).
        let mut mirrors = self.mirrors.lock();
        let Some(session) = mirrors.get_mut(&session_id) else {
            return Err(70);
        };
        if session.epoch != epoch {
            return Err(71);
        }
        session.epoch = next_epoch(epoch);
        session.last_activity_ms = now_ms;
        session.mirror_gen = session.mirror_gen.saturating_add(1);
        self.persist_mirrors_locked(&mirrors);
        Ok(())
    }

    /// Merge request topics/partitions into an existing session.
    ///
    /// New or updated partitions keep/replace fetch params; `last_hwm`/`last_lso`
    /// are preserved when the same partition already existed (offset updates do
    /// not clear the omit cache — empty-records + same HWM still omits).
    ///
    /// Primary mutates + `queue_mirror_put`. Mirror-only mutates in-place without
    /// put fan-out (Phase 147).
    pub fn merge_topics(&self, session_id: i32, topics: &HashMap<String, SessionTopic>) {
        {
            let mut guard = self.sessions.lock();
            if let Some(session) = guard.get_mut(&session_id) {
                merge_topics_into(session, topics);
                self.persist_locked(&guard);
                drop(guard);
                self.queue_mirror_put(session_id);
                return;
            }
        }
        let mut mirrors = self.mirrors.lock();
        if let Some(session) = mirrors.get_mut(&session_id) {
            merge_topics_into(session, topics);
            self.persist_mirrors_locked(&mirrors);
        }
    }

    /// Apply forgotten_topics_data removals.
    ///
    /// Mirror-only forget applies in-place without put fan-out (Phase 147).
    pub fn forget(&self, session_id: i32, forgotten: &[(String, Vec<i32>)]) {
        if session_id == 0 || forgotten.is_empty() {
            return;
        }
        {
            let mut guard = self.sessions.lock();
            if let Some(session) = guard.get_mut(&session_id) {
                forget_into(session, forgotten);
                self.persist_locked(&guard);
                drop(guard);
                self.queue_mirror_put(session_id);
                return;
            }
        }
        let mut mirrors = self.mirrors.lock();
        if let Some(session) = mirrors.get_mut(&session_id) {
            forget_into(session, forgotten);
            self.persist_mirrors_locked(&mirrors);
        }
    }

    /// Snapshot topics currently in the session (for empty-topics incremental).
    ///
    /// Primary first, else foreign mirror (Phase 147).
    pub fn snapshot_topics(&self, session_id: i32) -> HashMap<String, SessionTopic> {
        if let Some(s) = self.sessions.lock().get(&session_id) {
            return s.topics.clone();
        }
        self.mirrors
            .lock()
            .get(&session_id)
            .map(|s| s.topics.clone())
            .unwrap_or_default()
    }

    /// Record HWM/LSO returned for a partition after it was included in a response.
    ///
    /// Only call for `error == 0` includes (Phase 91 MVP).
    /// Mirror-only updates apply in-place without put fan-out (Phase 147).
    pub fn note_returned(
        &self,
        session_id: i32,
        topic_key: &str,
        partition: i32,
        hwm: i64,
        lso: i64,
    ) {
        if session_id == 0 {
            return;
        }
        {
            let mut guard = self.sessions.lock();
            if let Some(session) = guard.get_mut(&session_id) {
                if note_returned_into(session, topic_key, partition, hwm, lso) {
                    self.persist_locked(&guard);
                    drop(guard);
                    self.queue_mirror_put(session_id);
                }
                return;
            }
        }
        let mut mirrors = self.mirrors.lock();
        if let Some(session) = mirrors.get_mut(&session_id) {
            if note_returned_into(session, topic_key, partition, hwm, lso) {
                self.persist_mirrors_locked(&mirrors);
            }
        }
    }

    /// Drop idle sessions under the lock. Counts each removal as an eviction.
    fn evict_idle_locked(&self, sessions: &mut HashMap<i32, FetchSession>, now_ms: i64) {
        let idle_ms = self.idle_timeout_ms.load(Ordering::Relaxed);
        if idle_ms == 0 {
            return;
        }
        let idle_ms_i = idle_ms as i64;
        let removed_ids: Vec<i32> = sessions
            .iter()
            .filter(|(_, s)| now_ms.saturating_sub(s.last_activity_ms) > idle_ms_i)
            .map(|(id, _)| *id)
            .collect();
        if removed_ids.is_empty() {
            return;
        }
        for id in &removed_ids {
            sessions.remove(id);
        }
        let removed = removed_ids.len() as u64;
        self.evicted_total.fetch_add(removed, Ordering::Relaxed);
        self.idle_evicted_total
            .fetch_add(removed, Ordering::Relaxed);
        for id in removed_ids {
            self.clear_last_mirrored(id);
            self.queue_mirror_delete(id);
        }
    }

    /// If at max capacity, remove one LRU session. Counts as eviction.
    fn evict_lru_if_full_locked(&self, sessions: &mut HashMap<i32, FetchSession>) {
        let max = self.max_sessions.load(Ordering::Relaxed);
        if max == 0 || sessions.len() < max {
            return;
        }
        // Pick lowest last_activity_ms; ties → lowest session id (deterministic).
        let victim = sessions
            .iter()
            .min_by_key(|(id, s)| (s.last_activity_ms, *id))
            .map(|(id, _)| *id);
        if let Some(id) = victim {
            sessions.remove(&id);
            self.evicted_total.fetch_add(1, Ordering::Relaxed);
            self.clear_last_mirrored(id);
            self.queue_mirror_delete(id);
        }
    }

    /// Load durable snapshot; filter idle; set `restored` (Phase 115).
    fn load_from_disk_at(&self, now_ms: i64) {
        let Some(path) = self.durable_path.as_ref() else {
            return;
        };
        let file = match load_file(path) {
            Some(f) => f,
            None => {
                self.restored.store(0, Ordering::Relaxed);
                return;
            }
        };

        let idle_ms = self.idle_timeout_ms.load(Ordering::Relaxed);
        let idle_ms_i = idle_ms as i64;
        let mut loaded: HashMap<i32, FetchSession> = HashMap::new();
        let mut idle_dropped = 0u64;
        let mut max_local = 0i32;

        for s in file.sessions {
            if s.id <= 0 {
                continue;
            }
            max_local = max_local.max(session_local_part(s.id));
            if idle_ms > 0 && now_ms.saturating_sub(s.last_activity_ms) > idle_ms_i {
                idle_dropped += 1;
                continue;
            }
            if let Some(session) = stored_to_session(s) {
                loaded.insert(session.0, session.1);
            }
        }

        if idle_dropped > 0 {
            self.evicted_total
                .fetch_add(idle_dropped, Ordering::Relaxed);
            self.idle_evicted_total
                .fetch_add(idle_dropped, Ordering::Relaxed);
        }

        // `next_id` is the local counter (Phase 119); file may store local or legacy full id.
        let file_local = session_local_part(file.next_id).max(1);
        let mut next = file_local.max(max_local.saturating_add(1)).max(1);
        if self.owner_node_id.load(Ordering::Relaxed) > 0 && next > SESSION_LOCAL_MASK {
            next = max_local.saturating_add(1).max(1);
            if next > SESSION_LOCAL_MASK {
                next = 1;
            }
        }
        self.next_id.store(next, Ordering::Relaxed);
        self.restored.store(loaded.len() as u64, Ordering::Relaxed);

        let mut guard = self.sessions.lock();
        *guard = loaded;
        // Rewrite if we dropped idle entries so disk matches RAM.
        if idle_dropped > 0 {
            self.persist_locked(&guard);
        }
    }

    /// Persist full snapshot (atomic temp + rename + fsync). No-op without path.
    fn persist_locked(&self, sessions: &HashMap<i32, FetchSession>) {
        let Some(path) = self.durable_path.as_ref() else {
            return;
        };
        let next_id = self.next_id.load(Ordering::Relaxed);
        let file = snapshot_file(sessions, next_id);
        if save_file(path, &file).is_err() {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Load durable foreign mirrors; filter idle (Phase 139).
    fn load_mirrors_from_disk_at(&self, now_ms: i64) {
        let Some(path) = self.mirror_durable_path.as_ref() else {
            return;
        };
        let file = match load_file(path) {
            Some(f) => f,
            None => {
                self.mirror_restored.store(0, Ordering::Relaxed);
                return;
            }
        };

        let idle_ms = self.idle_timeout_ms.load(Ordering::Relaxed);
        let idle_ms_i = idle_ms as i64;
        let mut loaded: HashMap<i32, FetchSession> = HashMap::new();
        let mut idle_dropped = 0u64;

        for s in file.sessions {
            if s.id <= 0 {
                continue;
            }
            if idle_ms > 0 && now_ms.saturating_sub(s.last_activity_ms) > idle_ms_i {
                idle_dropped += 1;
                continue;
            }
            if let Some(session) = stored_to_session(s) {
                loaded.insert(session.0, session.1);
            }
        }

        self.mirror_restored
            .store(loaded.len() as u64, Ordering::Relaxed);
        let mut guard = self.mirrors.lock();
        *guard = loaded;
        if idle_dropped > 0 {
            self.persist_mirrors_locked(&guard);
        }
    }

    /// Persist foreign mirrors (Phase 139). No-op without path.
    fn persist_mirrors_locked(&self, mirrors: &HashMap<i32, FetchSession>) {
        let Some(path) = self.mirror_durable_path.as_ref() else {
            return;
        };
        // next_id is unused for mirrors; store 1 for a valid file shape.
        let file = snapshot_file(mirrors, 1);
        if save_file(path, &file).is_err() {
            self.persist_errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Next session epoch after a successful request (Kafka `FetchSession.nextEpoch`).
pub fn next_epoch(prev: i32) -> i32 {
    if prev < 0 {
        FINAL_EPOCH
    } else if prev == i32::MAX {
        1
    } else {
        prev + 1
    }
}

fn merge_topics_into(session: &mut FetchSession, topics: &HashMap<String, SessionTopic>) {
    for (key, topic) in topics {
        let entry = session
            .topics
            .entry(key.clone())
            .or_insert_with(|| SessionTopic {
                wire: topic.wire.clone(),
                name: topic.name.clone(),
                partitions: HashMap::new(),
            });
        entry.wire = topic.wire.clone();
        if !topic.name.is_empty() {
            entry.name = topic.name.clone();
        }
        for (pid, part) in &topic.partitions {
            let prev = entry.partitions.get(pid);
            let mut merged = part.clone();
            if merged.last_hwm.is_none() {
                if let Some(p) = prev {
                    merged.last_hwm = p.last_hwm;
                    merged.last_lso = p.last_lso;
                }
            }
            entry.partitions.insert(*pid, merged);
        }
    }
    session.mirror_gen = session.mirror_gen.saturating_add(1);
}

fn forget_into(session: &mut FetchSession, forgotten: &[(String, Vec<i32>)]) {
    for (key, parts) in forgotten {
        if parts.is_empty() {
            session.topics.remove(key);
            continue;
        }
        let empty = if let Some(topic) = session.topics.get_mut(key) {
            for p in parts {
                topic.partitions.remove(p);
            }
            topic.partitions.is_empty()
        } else {
            false
        };
        if empty {
            session.topics.remove(key);
        }
    }
    session.mirror_gen = session.mirror_gen.saturating_add(1);
}

/// Returns `true` when a partition was updated.
fn note_returned_into(
    session: &mut FetchSession,
    topic_key: &str,
    partition: i32,
    hwm: i64,
    lso: i64,
) -> bool {
    let Some(topic) = session.topics.get_mut(topic_key) else {
        return false;
    };
    let Some(part) = topic.partitions.get_mut(&partition) else {
        return false;
    };
    part.last_hwm = Some(hwm);
    part.last_lso = Some(lso);
    session.mirror_gen = session.mirror_gen.saturating_add(1);
    true
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Default idle TTL from env or 60s (Phase 95).
pub fn default_idle_timeout_ms() -> u64 {
    std::env::var("VOLANT_FETCH_SESSION_IDLE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_IDLE_TIMEOUT_MS)
}

/// Default max sessions from env or 1000 (Phase 95).
pub fn default_max_sessions() -> usize {
    std::env::var("VOLANT_FETCH_SESSION_MAX")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_SESSIONS)
}

/// Default Put fan-out min interval from env or 50ms (Phase 139).
pub fn default_mirror_put_min_interval_ms() -> u64 {
    std::env::var("VOLANT_FETCH_SESSION_MIRROR_PUT_MIN_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_MIRROR_PUT_MIN_INTERVAL_MS)
}

/// Whether durable peer mirrors are enabled via env (Phase 139). Default off.
pub fn default_mirror_durable_enabled() -> bool {
    match std::env::var("VOLANT_FETCH_SESSION_MIRROR_DURABLE") {
        Ok(s) => {
            let s = s.trim();
            s == "1" || s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Serve foreign mirror without promote on owner miss (Phase 147). Default **on**.
///
/// Set `VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE=0` (or false/no/off) for
/// legacy Phase 138 promote-on-miss when mirror is present.
pub fn default_serve_mirror_without_promote() -> bool {
    match std::env::var("VOLANT_FETCH_SESSION_SERVE_MIRROR_WITHOUT_PROMOTE") {
        Ok(s) => {
            let s = s.trim();
            !(s == "0"
                || s.eq_ignore_ascii_case("false")
                || s.eq_ignore_ascii_case("no")
                || s.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
}

/// Force promote-on-miss when mirror present (Phase 147). Default **off**.
///
/// `VOLANT_FETCH_SESSION_PROMOTE_ON_MISS=1` overrides serve-without-promote and
/// moves mirror → primary (Phase 138 path).
pub fn default_promote_on_miss() -> bool {
    match std::env::var("VOLANT_FETCH_SESSION_PROMOTE_ON_MISS") {
        Ok(s) => {
            let s = s.trim();
            s == "1" || s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Unclaimed dual-primary converge (v0.25). Default **on**.
///
/// Set `VOLANT_SESSION_DUAL_EPOCH_CONVERGE=0` (or false/no/off) to disable
/// (regression escape: both unclaimed primaries can remain).
pub fn default_dual_epoch_converge() -> bool {
    match std::env::var("VOLANT_SESSION_DUAL_EPOCH_CONVERGE") {
        Ok(s) => {
            let s = s.trim();
            !(s == "0"
                || s.eq_ignore_ascii_case("false")
                || s.eq_ignore_ascii_case("no")
                || s.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
}

/// Converge two managers that both hold an unclaimed primary for `session_id`.
///
/// The loser demotes its primary to a mirror of the winner and increments
/// `volant_fetch_session_dual_epoch_converge_total` on the loser. Tie (equal
/// gen/epoch/claim) keeps both locals. No-op when the loser's knob is off.
///
/// Returns `true` when a demote happened.
pub fn converge_dual_epoch_pair(
    a: &FetchSessionManager,
    b: &FetchSessionManager,
    session_id: i32,
) -> bool {
    let Some(sa) = a.primary_session_clone(session_id) else {
        return false;
    };
    let Some(sb) = b.primary_session_clone(session_id) else {
        return false;
    };
    if !both_unclaimed_primary_ish(&sa, &sb) {
        return false;
    }
    let a_wins = session_dual_epoch_wins(&sa, a.owner_node_id(), &sb, b.owner_node_id());
    let b_wins = session_dual_epoch_wins(&sb, b.owner_node_id(), &sa, a.owner_node_id());
    if a_wins && !b_wins {
        b.converge_dual_epoch(session_id, &sa)
    } else if b_wins && !a_wins {
        a.converge_dual_epoch(session_id, &sb)
    } else {
        false
    }
}

// --- Phase 115 durable file format ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFetchSessionsFile {
    #[serde(default = "default_file_version")]
    version: u32,
    #[serde(default = "default_next_id")]
    next_id: i32,
    #[serde(default)]
    sessions: Vec<StoredFetchSession>,
}

fn default_file_version() -> u32 {
    FETCH_SESSIONS_FILE_VERSION
}

fn default_next_id() -> i32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredFetchSession {
    id: i32,
    epoch: i32,
    last_activity_ms: i64,
    /// Peer-mirror fencing generation (Phase 139); absent on older snapshots.
    #[serde(default)]
    mirror_gen: u64,
    /// Claim-promote node id (Phase 143); absent on older snapshots → `0`.
    #[serde(default)]
    promoted_by: u32,
    /// MirrorPut wire mode: `"full"` (complete topics) or `"delta"` (upserts + removes).
    ///
    /// Default `"full"` for backward compat with pre-Phase-146 peers / durable files.
    #[serde(default = "default_mirror_mode")]
    mode: String,
    /// Topic map keys to remove when `mode=delta` (Phase 146). Ignored for full mode.
    #[serde(default)]
    remove_topic_keys: Vec<String>,
    /// Full topics map (`mode=full`) or topic upserts only (`mode=delta`).
    #[serde(default)]
    topics: Vec<StoredSessionTopic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSessionTopic {
    key: String,
    /// `"name"` or `"uuid"`.
    wire_kind: String,
    #[serde(default)]
    wire_name: Option<String>,
    #[serde(default)]
    wire_uuid_hex: Option<String>,
    #[serde(default)]
    name: String,
    #[serde(default)]
    partitions: Vec<StoredSessionPartition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSessionPartition {
    id: i32,
    fetch_offset: i64,
    current_leader_epoch: i32,
    last_fetched_epoch: i32,
    max_bytes: usize,
    #[serde(default)]
    last_hwm: Option<i64>,
    #[serde(default)]
    last_lso: Option<i64>,
}

fn session_topic_to_stored(key: &str, t: &SessionTopic) -> StoredSessionTopic {
    let (wire_kind, wire_name, wire_uuid_hex) = match &t.wire {
        TopicWireId::Name(n) => ("name".to_owned(), Some(n.clone()), None),
        TopicWireId::Uuid(u) => ("uuid".to_owned(), None, Some(hex::encode(u))),
    };
    let mut partitions: Vec<StoredSessionPartition> = t
        .partitions
        .iter()
        .map(|(pid, p)| StoredSessionPartition {
            id: *pid,
            fetch_offset: p.fetch_offset,
            current_leader_epoch: p.current_leader_epoch,
            last_fetched_epoch: p.last_fetched_epoch,
            max_bytes: p.max_bytes,
            last_hwm: p.last_hwm,
            last_lso: p.last_lso,
        })
        .collect();
    partitions.sort_by_key(|p| p.id);
    StoredSessionTopic {
        key: key.to_owned(),
        wire_kind,
        wire_name,
        wire_uuid_hex,
        name: t.name.clone(),
        partitions,
    }
}

/// Full snapshot (`mode=full`) for durable files and MirrorPut baseline export.
fn session_to_stored(id: i32, s: &FetchSession) -> StoredFetchSession {
    let mut topics: Vec<StoredSessionTopic> = s
        .topics
        .iter()
        .map(|(key, t)| session_topic_to_stored(key, t))
        .collect();
    topics.sort_by(|a, b| a.key.cmp(&b.key));
    StoredFetchSession {
        id,
        epoch: s.epoch,
        last_activity_ms: s.last_activity_ms,
        mirror_gen: s.mirror_gen,
        promoted_by: s.promoted_by,
        mode: default_mirror_mode(),
        remove_topic_keys: Vec::new(),
        topics,
    }
}

/// Delta snapshot relative to `prev` (Phase 146). Topics equal → metadata-only delta.
fn session_to_delta_stored(id: i32, cur: &FetchSession, prev: &FetchSession) -> StoredFetchSession {
    let mut remove_topic_keys: Vec<String> = prev
        .topics
        .keys()
        .filter(|k| !cur.topics.contains_key(*k))
        .cloned()
        .collect();
    remove_topic_keys.sort();

    let mut upserts: Vec<StoredSessionTopic> = cur
        .topics
        .iter()
        .filter(|(k, t)| prev.topics.get(*k) != Some(*t))
        .map(|(k, t)| session_topic_to_stored(k, t))
        .collect();
    upserts.sort_by(|a, b| a.key.cmp(&b.key));

    StoredFetchSession {
        id,
        epoch: cur.epoch,
        last_activity_ms: cur.last_activity_ms,
        mirror_gen: cur.mirror_gen,
        promoted_by: cur.promoted_by,
        mode: "delta".to_owned(),
        remove_topic_keys,
        topics: upserts,
    }
}

fn stored_topics_to_map(topics: Vec<StoredSessionTopic>) -> Option<HashMap<String, SessionTopic>> {
    let mut out = HashMap::new();
    for t in topics {
        let wire = match t.wire_kind.as_str() {
            "uuid" => {
                let hex = t.wire_uuid_hex.as_deref().unwrap_or("");
                let bytes = hex::decode_16(hex)?;
                TopicWireId::Uuid(bytes)
            }
            _ => TopicWireId::Name(t.wire_name.unwrap_or_else(|| t.name.clone())),
        };
        let mut partitions = HashMap::new();
        for p in t.partitions {
            partitions.insert(
                p.id,
                SessionPartition {
                    fetch_offset: p.fetch_offset,
                    current_leader_epoch: p.current_leader_epoch,
                    last_fetched_epoch: p.last_fetched_epoch,
                    max_bytes: p.max_bytes,
                    last_hwm: p.last_hwm,
                    last_lso: p.last_lso,
                },
            );
        }
        out.insert(
            t.key,
            SessionTopic {
                wire,
                name: t.name,
                partitions,
            },
        );
    }
    Some(out)
}

fn snapshot_file(sessions: &HashMap<i32, FetchSession>, next_id: i32) -> StoredFetchSessionsFile {
    let mut out = Vec::with_capacity(sessions.len());
    let mut ids: Vec<i32> = sessions.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        let Some(s) = sessions.get(&id) else {
            continue;
        };
        out.push(session_to_stored(id, s));
    }
    StoredFetchSessionsFile {
        version: FETCH_SESSIONS_FILE_VERSION,
        next_id,
        sessions: out,
    }
}

/// Full-mode install: topics are the complete map (`mode` ignored for durable load).
fn stored_to_session(s: StoredFetchSession) -> Option<(i32, FetchSession)> {
    let topics = stored_topics_to_map(s.topics)?;
    Some((
        s.id,
        FetchSession {
            epoch: s.epoch,
            topics,
            last_activity_ms: s.last_activity_ms,
            mirror_gen: s.mirror_gen,
            promoted_by: s.promoted_by,
        },
    ))
}

fn load_file(path: &Path) -> Option<StoredFetchSessionsFile> {
    if !path.exists() {
        return None;
    }
    let mut f = File::open(path).ok()?;
    let mut buf = String::new();
    f.read_to_string(&mut buf).ok()?;
    if buf.trim().is_empty() {
        return None;
    }
    serde_json::from_str(&buf).ok()
}

fn save_file(path: &Path, state: &StoredFetchSessionsFile) -> Result<(), ()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let _ = fs::create_dir_all(parent);
    let tmp = parent.join(format!("{}.tmp", FETCH_SESSIONS_FILE));
    let json = serde_json::to_string_pretty(state).map_err(|_| ())?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|_| ())?;
        f.write_all(json.as_bytes()).map_err(|_| ())?;
        f.sync_all().map_err(|_| ())?;
    }
    fs::rename(&tmp, path).map_err(|_| ())?;
    Ok(())
}

/// Minimal hex encode/decode without extra crate dependency.
mod hex {
    pub fn encode(bytes: &[u8; 16]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(32);
        for &b in bytes {
            s.push(HEX[(b >> 4) as usize] as char);
            s.push(HEX[(b & 0xf) as usize] as char);
        }
        s
    }

    pub fn decode_16(s: &str) -> Option<[u8; 16]> {
        if s.len() != 32 {
            return None;
        }
        let mut out = [0u8; 16];
        let bytes = s.as_bytes();
        for i in 0..16 {
            let hi = from_hex(bytes[i * 2])?;
            let lo = from_hex(bytes[i * 2 + 1])?;
            out[i] = (hi << 4) | lo;
        }
        Some(out)
    }

    fn from_hex(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(offset: i64) -> SessionPartition {
        SessionPartition::new(offset, -1, -1, 1_000_000)
    }

    fn topic_map(name: &str, offset: i64) -> HashMap<String, SessionTopic> {
        let mut topics = HashMap::new();
        let mut parts = HashMap::new();
        parts.insert(0, part(offset));
        topics.insert(
            name.into(),
            SessionTopic {
                wire: TopicWireId::Name(name.into()),
                name: name.into(),
                partitions: parts,
            },
        );
        topics
    }

    fn temp_data_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("volant-fsess-{}-{}", std::process::id(), nanos));
        let _ = fs::create_dir_all(&dir);
        dir
    }

    #[test]
    fn create_and_incremental_epoch() {
        let mgr = FetchSessionManager::with_limits(0, 0); // no TTL / no max
        let id = mgr.create_at(HashMap::new(), 1_000);
        assert!(id > 0);
        assert!(mgr.begin_incremental_at(id, 1, 1_100).is_ok());
        assert!(mgr.begin_incremental_at(id, 2, 1_200).is_ok());
        // wrong epoch
        assert_eq!(mgr.begin_incremental_at(id, 2, 1_300), Err(71));
        // unknown
        assert_eq!(mgr.begin_incremental_at(id + 99, 1, 1_400), Err(70));
        mgr.close(id);
        assert_eq!(mgr.begin_incremental_at(id, 3, 1_500), Err(70));
    }

    #[test]
    fn next_epoch_wrap() {
        assert_eq!(next_epoch(0), 1);
        assert_eq!(next_epoch(1), 2);
        assert_eq!(next_epoch(i32::MAX), 1);
        assert_eq!(next_epoch(-1), -1);
    }

    #[test]
    fn omit_unchanged_requires_matching_hwm_lso() {
        let mut p = part(0);
        assert!(!p.should_omit_unchanged(1, 1, true, 0)); // never returned
        p.last_hwm = Some(1);
        p.last_lso = Some(1);
        assert!(p.should_omit_unchanged(1, 1, true, 0));
        assert!(!p.should_omit_unchanged(2, 1, true, 0)); // hwm advanced
        assert!(!p.should_omit_unchanged(1, 2, true, 0)); // lso advanced
        assert!(!p.should_omit_unchanged(1, 1, false, 0)); // has records
        assert!(!p.should_omit_unchanged(1, 1, true, 1)); // error
    }

    #[test]
    fn note_returned_and_merge_preserves_cache() {
        let mgr = FetchSessionManager::with_limits(0, 0);
        let id = mgr.create_at(topic_map("t", 0), 1_000);
        mgr.note_returned(id, "t", 0, 5, 5);
        let snap = mgr.snapshot_topics(id);
        assert_eq!(snap["t"].partitions[&0].last_hwm, Some(5));
        assert_eq!(snap["t"].partitions[&0].last_lso, Some(5));

        // Merge with new fetch offset; cache preserved.
        mgr.merge_topics(id, &topic_map("t", 5));
        let snap = mgr.snapshot_topics(id);
        assert_eq!(snap["t"].partitions[&0].fetch_offset, 5);
        assert_eq!(snap["t"].partitions[&0].last_hwm, Some(5));
    }

    #[test]
    fn idle_ttl_evicts_then_incremental_is_70() {
        let mgr = FetchSessionManager::with_limits(100, 0); // 100ms idle
        let id = mgr.create_at(HashMap::new(), 1_000);
        assert_eq!(mgr.active_count(), 1);
        // Still within TTL
        assert!(mgr.begin_incremental_at(id, 1, 1_050).is_ok());
        // Past idle (last activity 1050, now 1200, idle 100)
        assert_eq!(mgr.begin_incremental_at(id, 2, 1_200), Err(70));
        assert_eq!(mgr.active_count(), 0);
        assert!(mgr.evicted_total() >= 1);
    }

    #[test]
    fn max_sessions_evicts_lru() {
        let mgr = FetchSessionManager::with_limits(0, 2); // no idle, max 2
        let a = mgr.create_at(HashMap::new(), 1_000);
        let b = mgr.create_at(HashMap::new(), 1_100);
        assert_eq!(mgr.active_count(), 2);
        // Touch B so A is LRU
        assert!(mgr.begin_incremental_at(b, 1, 1_200).is_ok());
        let c = mgr.create_at(HashMap::new(), 1_300);
        assert_eq!(mgr.active_count(), 2);
        // A was LRU-evicted
        assert_eq!(mgr.begin_incremental_at(a, 1, 1_400), Err(70));
        // B and C still live
        assert!(mgr.begin_incremental_at(b, 2, 1_500).is_ok());
        assert!(mgr.begin_incremental_at(c, 1, 1_600).is_ok());
        assert!(mgr.evicted_total() >= 1);
    }

    #[test]
    fn idle_disabled_with_zero() {
        let mgr = FetchSessionManager::with_limits(0, 0);
        let id = mgr.create_at(HashMap::new(), 1_000);
        // Far in the future; still alive when TTL disabled
        assert!(mgr.begin_incremental_at(id, 1, 9_999_999).is_ok());
    }

    #[test]
    fn durable_roundtrip_restores_epoch_and_omit_cache() {
        let dir = temp_data_dir();
        let id = {
            // idle=0 so artificial timestamps are not filtered on reopen.
            let mgr = FetchSessionManager::open_with_limits(&dir, 0, 0);
            let id = mgr.create_at(topic_map("orders", 1), 1_000);
            mgr.note_returned(id, "orders", 0, 7, 7);
            assert!(mgr.begin_incremental_at(id, 1, 1_100).is_ok());
            // expected epoch now 2
            assert_eq!(mgr.active_count(), 1);
            assert!(mgr.is_durable());
            id
        };

        let mgr2 = FetchSessionManager::open_with_limits(&dir, 0, 0);
        assert_eq!(mgr2.restored(), 1);
        assert_eq!(mgr2.active_count(), 1);
        // Epoch advanced to 2 after first incremental
        assert!(mgr2.begin_incremental_at(id, 2, 2_000).is_ok());
        let snap = mgr2.snapshot_topics(id);
        assert_eq!(snap["orders"].partitions[&0].last_hwm, Some(7));
        assert_eq!(snap["orders"].partitions[&0].last_lso, Some(7));
        assert_eq!(snap["orders"].partitions[&0].fetch_offset, 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_close_not_restored() {
        let dir = temp_data_dir();
        {
            let mgr = FetchSessionManager::open_with_limits(&dir, 0, 0);
            let id = mgr.create_at(topic_map("t", 0), 1_000);
            mgr.close(id);
            assert_eq!(mgr.active_count(), 0);
        }
        let mgr2 = FetchSessionManager::open_with_limits(&dir, 0, 0);
        assert_eq!(mgr2.restored(), 0);
        assert_eq!(mgr2.active_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_idle_filtered_on_load() {
        let dir = temp_data_dir();
        {
            let mgr = FetchSessionManager::open_with_limits(&dir, 0, 0);
            let id = mgr.create_at(topic_map("t", 0), 1_000);
            assert!(id > 0);
            assert_eq!(mgr.active_count(), 1);
            assert_eq!(mgr.persist_errors_total(), 0, "persist failed");
        }
        let path = dir.join(FETCH_SESSIONS_DIR).join(FETCH_SESSIONS_FILE);
        assert!(path.is_file(), "missing snapshot at {}", path.display());
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.is_empty(), "empty file");
        let mut file = load_file(&path).expect("file");
        assert_eq!(file.sessions.len(), 1, "raw={raw}");
        file.sessions[0].last_activity_ms = 1; // ancient
        save_file(&path, &file).unwrap();

        // Default product idle 60s; activity ms=1 is far past → not restored.
        let mgr3 = FetchSessionManager::open(&dir);
        assert_eq!(mgr3.restored(), 0);
        assert_eq!(mgr3.active_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn next_id_continues_after_restore() {
        let dir = temp_data_dir();
        let first = {
            let mgr = FetchSessionManager::open_with_limits(&dir, 0, 0);
            mgr.create_at(HashMap::new(), 1_000)
        };
        let mgr2 = FetchSessionManager::open_with_limits(&dir, 0, 0);
        let second = mgr2.create_at(HashMap::new(), 2_000);
        assert!(second > first);
        assert_ne!(second, first);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn owner_encode_decode_roundtrip() {
        let id = encode_session_id(2, 7);
        assert_eq!(decode_session_owner(id), Some(2));
        assert_eq!(session_local_part(id), 7);
        assert_eq!(decode_session_owner(3), None); // sequential / unencoded
        assert_eq!(decode_session_owner(0), None);
    }

    #[test]
    fn cluster_alloc_embeds_owner() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 3);
        let id = mgr.create_at(HashMap::new(), 1_000);
        assert_eq!(decode_session_owner(id), Some(3));
        assert!(session_local_part(id) > 0);
        assert!(mgr.contains(id));
        assert!(mgr.begin_incremental_at(id, 1, 1_100).is_ok());
    }

    #[test]
    fn phase138_export_apply_mirror_put_contains() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("orders", 3), 1_000);
        owner.note_returned(id, "orders", 0, 9, 9);
        let bytes = owner.export_session_bytes(id).expect("export");

        let peer = FetchSessionManager::with_limits(0, 0);
        assert!(!peer.mirror_contains(id));
        peer.apply_mirror_put(&bytes).expect("put");
        assert!(peer.mirror_contains(id));
        assert_eq!(peer.mirrored_count(), 1);
        assert_eq!(peer.mirror_puts_applied_total(), 1);
        assert!(!peer.contains(id));
        // Bad JSON rejected.
        assert!(peer.apply_mirror_put(b"not-json").is_err());
    }

    #[test]
    fn phase138_promote_moves_mirror_to_primary() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 2);
        let id = owner.create_at(topic_map("t", 1), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&bytes).unwrap();
        assert!(peer.mirror_contains(id));
        assert!(!peer.contains(id));

        assert!(peer.promote_from_mirror(id));
        assert!(peer.contains(id));
        assert!(!peer.mirror_contains(id));
        assert_eq!(peer.mirrored_count(), 0);
        assert_eq!(peer.promote_total(), 1);
        // Primary already has id → true no-op.
        assert!(peer.promote_from_mirror(id));
        assert_eq!(peer.promote_total(), 1);
        // Missing → false.
        assert!(!peer.promote_from_mirror(id + 99));
    }

    #[test]
    fn phase138_apply_mirror_delete() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(HashMap::new(), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&bytes).unwrap();
        assert!(peer.mirror_contains(id));
        peer.apply_mirror_delete(id);
        assert!(!peer.mirror_contains(id));
        assert_eq!(peer.mirror_deletes_applied_total(), 1);
        // Missing is no-op.
        peer.apply_mirror_delete(id);
        assert_eq!(peer.mirror_deletes_applied_total(), 1);
    }

    #[test]
    fn phase138_drain_mirror_ops_put_on_create() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = mgr.create_at(HashMap::new(), 1_000);
        let ops = mgr.drain_mirror_ops();
        assert!(
            ops.iter().any(|op| *op == SessionMirrorOp::Put(id)),
            "expected Put({id}), got {ops:?}"
        );
        // Drained empty.
        assert!(mgr.drain_mirror_ops().is_empty());
        // owner_node_id=0 does not queue.
        let single = FetchSessionManager::with_limits(0, 0);
        let _ = single.create_at(HashMap::new(), 1_000);
        assert!(single.drain_mirror_ops().is_empty());
    }

    #[test]
    fn phase138_close_yields_delete() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = mgr.create_at(HashMap::new(), 1_000);
        let _ = mgr.drain_mirror_ops(); // drop create Put
        mgr.close(id);
        let ops = mgr.drain_mirror_ops();
        assert_eq!(ops, vec![SessionMirrorOp::Delete(id)]);
        assert!(!mgr.contains(id));
    }

    #[test]
    fn phase139_coalesce_puts_same_session() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = mgr.create_at(HashMap::new(), 1_000);
        // create queues Put; more mutations coalesce.
        assert!(mgr.begin_incremental_at(id, 1, 1_100).is_ok());
        mgr.note_returned(id, "x", 0, 1, 1); // no topic — still queues? note_returned no-ops without topic
        mgr.merge_topics(id, &topic_map("t", 0));
        mgr.forget(id, &[]); // empty forgotten → no-op, no queue
        let ops = mgr.drain_mirror_ops();
        assert_eq!(ops, vec![SessionMirrorOp::Put(id)]);
        assert!(mgr.mirror_puts_coalesced_total() >= 1);
    }

    #[test]
    fn phase139_delete_supersedes_put() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = mgr.create_at(HashMap::new(), 1_000);
        assert!(mgr.has_pending_mirror_ops());
        mgr.close(id);
        let ops = mgr.drain_mirror_ops();
        assert_eq!(ops, vec![SessionMirrorOp::Delete(id)]);
        assert!(mgr.mirror_puts_coalesced_total() >= 1);
        assert!(!mgr.has_pending_mirror_delete());
    }

    #[test]
    fn phase139_drain_deletes_before_puts() {
        let mgr = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let a = mgr.create_at(HashMap::new(), 1_000);
        let b = mgr.create_at(HashMap::new(), 1_100);
        let _ = mgr.drain_mirror_ops();
        mgr.close(a);
        assert!(mgr.begin_incremental_at(b, 1, 1_200).is_ok());
        let ops = mgr.drain_mirror_ops();
        assert_eq!(
            ops,
            vec![SessionMirrorOp::Delete(a), SessionMirrorOp::Put(b)]
        );
    }

    #[test]
    fn phase139_apply_put_rejects_stale_gen() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("t", 0), 1_000);
        let older = owner.export_session_bytes(id).unwrap();
        assert!(owner.begin_incremental_at(id, 1, 1_100).is_ok());
        let newer = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&newer).unwrap();
        assert_eq!(peer.mirror_puts_applied_total(), 1);
        peer.apply_mirror_put(&older).unwrap();
        assert_eq!(peer.mirror_puts_applied_total(), 1);
        assert_eq!(peer.mirror_stale_put_rejects_total(), 1);
        // Mirror still holds newer gen.
        let mirror_gen = {
            let g = peer.mirrors.lock();
            g.get(&id).unwrap().mirror_gen
        };
        assert!(mirror_gen >= 2);
    }

    #[test]
    fn phase139_promote_supersede_and_keep_newer_primary() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 2);
        let id = owner.create_at(topic_map("t", 1), 1_000);
        let v1 = owner.export_session_bytes(id).unwrap();
        assert!(owner.begin_incremental_at(id, 1, 1_100).is_ok());
        let v2 = owner.export_session_bytes(id).unwrap();

        // v0.25: unclaimed dual-primary + newer put demotes local to mirror.
        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&v1).unwrap();
        assert!(peer.promote_from_mirror(id));
        assert!(peer.contains(id));
        peer.apply_mirror_put(&v2).unwrap();
        assert!(!peer.contains(id));
        assert!(peer.mirror_contains(id));
        assert_eq!(peer.dual_epoch_converge_total(), 1);
        assert_eq!(peer.mirror_puts_applied_total(), 2);

        // Keep newer primary: older mirror dropped without supersede.
        let peer2 = FetchSessionManager::with_limits(0, 0);
        peer2.apply_mirror_put(&v2).unwrap();
        assert!(peer2.promote_from_mirror(id));
        let promote_before = peer2.promote_total();
        // Inject older mirror under the same id while primary is newer.
        {
            let stored: StoredFetchSession = serde_json::from_slice(&v1).unwrap();
            let (sid, sess) = stored_to_session(stored).unwrap();
            peer2.mirrors.lock().insert(sid, sess);
        }
        assert!(peer2.promote_from_mirror(id));
        assert_eq!(peer2.promote_total(), promote_before); // no new promote
        assert!(!peer2.mirror_contains(id));
        let primary_gen = {
            let g = peer2.sessions.lock();
            g.get(&id).unwrap().mirror_gen
        };
        assert!(primary_gen >= 2);

        // Supersede: primary older, mirror newer.
        let peer3 = FetchSessionManager::with_limits(0, 0);
        peer3.apply_mirror_put(&v1).unwrap();
        assert!(peer3.promote_from_mirror(id));
        {
            let stored: StoredFetchSession = serde_json::from_slice(&v2).unwrap();
            let (sid, sess) = stored_to_session(stored).unwrap();
            peer3.mirrors.lock().insert(sid, sess);
        }
        let super_before = peer3.promote_supersede_total();
        assert!(peer3.promote_from_mirror(id));
        assert_eq!(peer3.promote_supersede_total(), super_before + 1);
        assert!(!peer3.mirror_contains(id));
        let primary_gen = {
            let g = peer3.sessions.lock();
            g.get(&id).unwrap().mirror_gen
        };
        assert!(primary_gen >= 2);
    }

    #[test]
    fn phase139_durable_mirror_roundtrip() {
        let dir = temp_data_dir();
        let id = {
            let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
            let id = owner.create_at(topic_map("orders", 2), 1_000);
            owner.note_returned(id, "orders", 0, 3, 3);
            let bytes = owner.export_session_bytes(id).unwrap();

            let mut peer = FetchSessionManager::with_limits(0, 0);
            peer.enable_mirror_durable(&dir);
            assert!(peer.is_mirror_durable());
            peer.apply_mirror_put(&bytes).unwrap();
            assert!(peer.mirror_contains(id));
            id
        };

        let mut peer2 = FetchSessionManager::with_limits(0, 0);
        peer2.enable_mirror_durable(&dir);
        assert_eq!(peer2.mirror_restored(), 1);
        assert!(peer2.mirror_contains(id));
        assert!(peer2.promote_from_mirror(id));
        assert!(peer2.contains(id));
        assert!(!peer2.mirror_contains(id));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn phase139_session_is_newer_order() {
        let base = FetchSession {
            epoch: 2,
            topics: HashMap::new(),
            last_activity_ms: 100,
            mirror_gen: 5,
            promoted_by: 0,
        };
        let higher_gen = FetchSession {
            mirror_gen: 6,
            ..base.clone()
        };
        assert!(session_is_newer(&higher_gen, &base));
        assert!(!session_is_newer(&base, &higher_gen));
        let higher_epoch = FetchSession {
            epoch: 3,
            ..base.clone()
        };
        assert!(session_is_newer(&higher_epoch, &base));
        let higher_act = FetchSession {
            last_activity_ms: 200,
            ..base.clone()
        };
        assert!(session_is_newer(&higher_act, &base));
        assert!(!session_is_newer(&base, &base));
    }

    #[test]
    fn phase143_session_claim_wins_lowest_id() {
        let base = FetchSession {
            epoch: 1,
            topics: HashMap::new(),
            last_activity_ms: 100,
            mirror_gen: 3,
            promoted_by: 0,
        };
        let claim2 = FetchSession {
            promoted_by: 2,
            ..base.clone()
        };
        let claim3 = FetchSession {
            promoted_by: 3,
            ..base.clone()
        };
        // Equal gen: lower claim wins.
        assert!(session_claim_wins(&claim2, &claim3));
        assert!(!session_claim_wins(&claim3, &claim2));
        // Claimed beats unclaimed at equal freshness.
        assert!(session_claim_wins(&claim2, &base));
        assert!(!session_claim_wins(&base, &claim2));
        // Both unclaimed: keep existing.
        assert!(!session_claim_wins(&base, &base));
        // Strictly newer gen still wins even with higher claim.
        let high_claim_new = FetchSession {
            mirror_gen: 4,
            promoted_by: 9,
            ..base.clone()
        };
        assert!(session_claim_wins(&high_claim_new, &claim2));
        assert!(!session_claim_wins(&claim2, &high_claim_new));
    }

    #[test]
    fn phase143_promoted_by_default_on_old_json() {
        let raw = r#"{"id":1,"epoch":1,"last_activity_ms":100,"mirror_gen":1,"topics":[]}"#;
        let stored: StoredFetchSession = serde_json::from_str(raw).unwrap();
        assert_eq!(stored.promoted_by, 0);
        let (id, sess) = stored_to_session(stored).unwrap();
        assert_eq!(id, 1);
        assert_eq!(sess.promoted_by, 0);
    }

    #[test]
    fn phase146_full_put_still_works() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("orders", 3), 1_000);
        let bytes = owner.export_session_bytes(id).expect("export full");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v.get("mode").and_then(|m| m.as_str()), Some("full"));

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&bytes).unwrap();
        assert!(peer.mirror_contains(id));
        assert_eq!(peer.mirror_puts_applied_total(), 1);
        assert_eq!(peer.mirror_delta_puts_total(), 0);
        assert!(peer.promote_from_mirror(id));
        let topics = peer.snapshot_topics(id);
        assert!(topics.contains_key("orders"));
    }

    #[test]
    fn phase146_delta_upsert_adds_topic() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("orders", 1), 1_000);
        let full = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&full).unwrap();

        owner.merge_topics(id, &topic_map("payments", 0));
        let prev = peer.mirrors.lock().get(&id).cloned();
        let delta = owner
            .export_session_delta_bytes(id, prev.as_ref())
            .expect("delta");
        let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
        assert_eq!(v.get("mode").and_then(|m| m.as_str()), Some("delta"));
        let topics = v.get("topics").and_then(|t| t.as_array()).unwrap();
        assert!(
            topics
                .iter()
                .any(|t| t.get("key").and_then(|k| k.as_str()) == Some("payments")),
            "upsert should include payments: {v}"
        );

        peer.apply_mirror_put(&delta).unwrap();
        assert_eq!(peer.mirror_delta_puts_total(), 1);
        assert!(peer.promote_from_mirror(id));
        let snap = peer.snapshot_topics(id);
        assert!(snap.contains_key("orders"));
        assert!(snap.contains_key("payments"));
    }

    #[test]
    fn phase146_delta_remove_topic_keys() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let mut topics = topic_map("a", 1);
        topics.extend(topic_map("b", 2));
        let id = owner.create_at(topics, 1_000);
        let full = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&full).unwrap();
        let prev = peer.mirrors.lock().get(&id).cloned();

        owner.forget(id, &[("b".into(), vec![])]);
        let delta = owner.export_session_delta_bytes(id, prev.as_ref()).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
        assert_eq!(v.get("mode").and_then(|m| m.as_str()), Some("delta"));
        let removes = v
            .get("remove_topic_keys")
            .and_then(|r| r.as_array())
            .unwrap();
        assert!(removes.iter().any(|x| x.as_str() == Some("b")));

        peer.apply_mirror_put(&delta).unwrap();
        peer.promote_from_mirror(id);
        let snap = peer.snapshot_topics(id);
        assert!(snap.contains_key("a"));
        assert!(!snap.contains_key("b"));
    }

    #[test]
    fn phase146_old_json_without_mode_applies_as_full() {
        // Pre-146 peers omit mode / remove_topic_keys; defaults → full install.
        let raw = r#"{"id":42,"epoch":2,"last_activity_ms":500,"mirror_gen":3,"promoted_by":0,"topics":[{"key":"t","wire_kind":"name","wire_name":"t","name":"t","partitions":[{"id":0,"fetch_offset":9,"current_leader_epoch":-1,"last_fetched_epoch":-1,"max_bytes":1000}]}]}"#;
        let stored: StoredFetchSession = serde_json::from_str(raw).unwrap();
        assert_eq!(stored.mode, "full");
        assert!(stored.remove_topic_keys.is_empty());

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(raw.as_bytes()).unwrap();
        assert!(peer.mirror_contains(42));
        assert_eq!(peer.mirror_delta_puts_total(), 0);
        assert!(peer.promote_from_mirror(42));
        let snap = peer.snapshot_topics(42);
        assert!(snap.contains_key("t"));
        assert_eq!(snap["t"].partitions[&0].fetch_offset, 9);
    }

    #[test]
    fn phase146_metadata_only_delta_keeps_topics() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("sticky", 5), 1_000);
        let full = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&full).unwrap();
        let prev = peer.mirrors.lock().get(&id).cloned().unwrap();

        // Epoch/activity/gen bump only (begin_incremental does not change topics).
        assert!(owner.begin_incremental_at(id, 1, 2_000).is_ok());
        let delta = owner.export_session_delta_bytes(id, Some(&prev)).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
        assert_eq!(v.get("mode").and_then(|m| m.as_str()), Some("delta"));
        assert_eq!(
            v.get("topics").and_then(|t| t.as_array()).map(|a| a.len()),
            Some(0)
        );
        assert_eq!(
            v.get("remove_topic_keys")
                .and_then(|r| r.as_array())
                .map(|a| a.len()),
            Some(0)
        );
        let new_act = v.get("last_activity_ms").and_then(|x| x.as_i64()).unwrap();
        assert_eq!(new_act, 2_000);

        peer.apply_mirror_put(&delta).unwrap();
        assert_eq!(peer.mirror_delta_puts_total(), 1);
        peer.promote_from_mirror(id);
        let snap = peer.snapshot_topics(id);
        assert!(snap.contains_key("sticky"));
        assert_eq!(snap["sticky"].partitions[&0].fetch_offset, 5);
    }

    #[test]
    fn phase146_delta_without_base_installs_upserts() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("solo", 1), 1_000);
        // Force a "delta" payload by exporting against empty prev topics.
        let empty = FetchSession {
            epoch: 0,
            topics: HashMap::new(),
            last_activity_ms: 0,
            mirror_gen: 0,
            promoted_by: 0,
        };
        let delta = owner.export_session_delta_bytes(id, Some(&empty)).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
        assert_eq!(v.get("mode").and_then(|m| m.as_str()), Some("delta"));

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&delta).unwrap();
        assert!(peer.mirror_contains(id));
        assert_eq!(peer.mirror_delta_puts_total(), 1);
        peer.promote_from_mirror(id);
        assert!(peer.snapshot_topics(id).contains_key("solo"));
    }

    #[test]
    fn phase146_export_mirror_put_bytes_uses_last_mirrored() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("t", 0), 1_000);
        let (b1, d1) = owner.export_mirror_put_bytes(id).unwrap();
        assert!(!d1, "first export is full");
        let v1: serde_json::Value = serde_json::from_slice(&b1).unwrap();
        assert_eq!(v1.get("mode").and_then(|m| m.as_str()), Some("full"));
        owner.note_last_mirrored(id);

        assert!(owner.begin_incremental_at(id, 1, 1_500).is_ok());
        let (b2, d2) = owner.export_mirror_put_bytes(id).unwrap();
        assert!(d2, "second export is delta");
        let v2: serde_json::Value = serde_json::from_slice(&b2).unwrap();
        assert_eq!(v2.get("mode").and_then(|m| m.as_str()), Some("delta"));
        owner.note_last_mirrored(id);
        owner.record_mirror_delta_put_sent();
        assert_eq!(owner.mirror_delta_puts_total(), 1);
    }

    #[test]
    fn phase147_begin_incremental_from_any_mirror_only() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(topic_map("orders", 1), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&bytes).unwrap();
        assert!(peer.has_servable_session(id));
        assert!(!peer.contains(id));
        assert!(peer.mirror_contains(id));
        let snap = peer.mirror_session_clone(id).expect("mirror clone");
        assert_eq!(snap.epoch, 1);
        assert!(snap.topics.contains_key("orders"));

        // Primary-only begin fails; from_any succeeds on mirror.
        assert_eq!(peer.begin_incremental_at(id, 1, 1_100), Err(70));
        assert!(peer.begin_incremental_from_any_at(id, 1, 1_100).is_ok());
        assert!(!peer.contains(id));
        assert!(peer.mirror_contains(id));
        assert_eq!(peer.promote_total(), 0);
        assert!(!peer.has_pending_mirror_ops());

        let after = peer.mirror_session_clone(id).unwrap();
        assert_eq!(after.epoch, 2);
        let topics = peer.snapshot_topics(id);
        assert!(topics.contains_key("orders"));

        peer.note_returned(id, "orders", 0, 5, 5);
        assert!(!peer.has_pending_mirror_ops());
        let noted = peer.mirror_session_clone(id).unwrap();
        assert_eq!(noted.topics["orders"].partitions[&0].last_hwm, Some(5));

        // Wrong epoch / missing id.
        assert_eq!(peer.begin_incremental_from_any_at(id, 99, 1_200), Err(71));
        assert_eq!(
            peer.begin_incremental_from_any_at(id + 99, 1, 1_300),
            Err(70)
        );
    }

    #[test]
    fn phase147_try_owner_miss_serve_without_promote() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(HashMap::new(), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        assert!(!peer.try_owner_miss_local_serve(id)); // no mirror

        peer.apply_mirror_put(&bytes).unwrap();
        assert!(peer.serve_mirror_without_promote());
        assert!(!peer.promote_on_miss());
        let before = peer.serve_from_mirror_total();
        assert!(peer.try_owner_miss_local_serve(id));
        assert_eq!(peer.serve_from_mirror_total(), before + 1);
        assert_eq!(peer.promote_total(), 0);
        assert!(peer.mirror_contains(id));
        assert!(!peer.contains(id));
    }

    #[test]
    fn phase147_try_owner_miss_promote_on_miss() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(HashMap::new(), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits_and_owner(0, 0, 2);
        peer.apply_mirror_put(&bytes).unwrap();
        peer.set_promote_on_miss(true);
        assert!(peer.try_owner_miss_local_serve(id));
        assert_eq!(peer.promote_total(), 1);
        assert_eq!(peer.serve_from_mirror_total(), 0);
        assert!(peer.contains(id));
        assert!(!peer.mirror_contains(id));
    }

    #[test]
    fn phase147_legacy_serve_without_promote_off_promotes() {
        let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let id = owner.create_at(HashMap::new(), 1_000);
        let bytes = owner.export_session_bytes(id).unwrap();

        let peer = FetchSessionManager::with_limits(0, 0);
        peer.apply_mirror_put(&bytes).unwrap();
        peer.set_serve_mirror_without_promote(false);
        peer.set_promote_on_miss(false);
        assert!(peer.try_owner_miss_local_serve(id));
        assert_eq!(peer.promote_total(), 1);
        assert_eq!(peer.serve_from_mirror_total(), 0);
        assert!(peer.contains(id));
    }

    fn v25_sess(epoch: i32, mirror_gen: u64, promoted_by: u32) -> FetchSession {
        FetchSession {
            epoch,
            topics: HashMap::new(),
            last_activity_ms: 1_000,
            mirror_gen,
            promoted_by,
        }
    }

    #[test]
    fn v25_session_dual_epoch_wins_order() {
        let low_gen_high_epoch = v25_sess(99, 1, 0);
        let high_gen_low_epoch = v25_sess(1, 5, 0);
        // Higher mirror_gen beats higher epoch.
        assert!(session_dual_epoch_wins(
            &high_gen_low_epoch,
            0,
            &low_gen_high_epoch,
            0
        ));
        assert!(!session_dual_epoch_wins(
            &low_gen_high_epoch,
            0,
            &high_gen_low_epoch,
            0
        ));

        let e1 = v25_sess(1, 3, 0);
        let e2 = v25_sess(2, 3, 0);
        assert!(session_dual_epoch_wins(&e2, 0, &e1, 0));
        assert!(!session_dual_epoch_wins(&e1, 0, &e2, 0));

        let unclaimed = v25_sess(1, 3, 0);
        let claim2 = v25_sess(1, 3, 2);
        let claim9 = v25_sess(1, 3, 9);
        assert!(session_dual_epoch_wins(&claim2, 0, &unclaimed, 0));
        assert!(!session_dual_epoch_wins(&unclaimed, 0, &claim2, 0));
        assert!(session_dual_epoch_wins(&claim2, 0, &claim9, 0));
        assert!(!session_dual_epoch_wins(&claim9, 0, &claim2, 0));
        // Tie: keep local.
        assert!(!session_dual_epoch_wins(&unclaimed, 0, &unclaimed, 0));
        // Owner id used when promoted_by is 0.
        assert!(session_dual_epoch_wins(&unclaimed, 2, &unclaimed, 5));
        assert!(!session_dual_epoch_wins(&unclaimed, 5, &unclaimed, 2));
    }

    #[test]
    fn v25_converge_demotes_loser_to_mirror() {
        let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
        let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
        let id = 42;
        a.install_primary(id, v25_sess(1, 5, 0));
        b.install_primary(id, v25_sess(9, 1, 0));
        assert!(a.contains(id) && b.contains(id));

        assert!(converge_dual_epoch_pair(&a, &b, id));
        assert!(a.contains(id));
        assert!(!a.mirror_contains(id));
        assert!(!b.contains(id));
        assert!(b.mirror_contains(id));
        assert_eq!(b.dual_epoch_converge_total(), 1);
        assert_eq!(a.dual_epoch_converge_total(), 0);
        let mirror = b.mirror_session_clone(id).unwrap();
        assert_eq!(mirror.mirror_gen, 5);
        assert_eq!(mirror.epoch, 1);
    }

    #[test]
    fn v25_converge_env_off_is_noop() {
        let a = FetchSessionManager::with_limits(0, 0);
        let b = FetchSessionManager::with_limits(0, 0);
        a.set_dual_epoch_converge(false);
        b.set_dual_epoch_converge(false);
        let id = 7;
        a.install_primary(id, v25_sess(1, 2, 0));
        b.install_primary(id, v25_sess(3, 1, 0));
        assert!(!converge_dual_epoch_pair(&a, &b, id));
        assert!(a.contains(id) && b.contains(id));
        assert_eq!(a.dual_epoch_converge_total(), 0);
        assert_eq!(b.dual_epoch_converge_total(), 0);
        assert!(!a.converge_dual_epoch(id, &v25_sess(3, 9, 0)));
        assert!(a.contains(id));
    }
}
