//! In-memory Fetch session state (Phase 88 + 91 omit-unchanged + Phase 95 limits).
//!
//! Process-local only: not durable, not shared across brokers. Tracks topic
//! partitions, last-seen fetch params, last-returned HWM/LSO (Phase 91), and
//! idle TTL / max concurrent sessions with lazy LRU eviction (Phase 95).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use super::topic_id::TopicWireId;

/// Kafka `FetchSession.INITIAL_EPOCH` — create / full fetch.
pub const INITIAL_EPOCH: i32 = 0;
/// Kafka `FetchSession.FINAL_EPOCH` — close session; no new session.
pub const FINAL_EPOCH: i32 = -1;

/// Default idle TTL for process-local fetch sessions (Phase 95).
pub const DEFAULT_IDLE_TIMEOUT_MS: u64 = 60_000;
/// Default max concurrent process-local fetch sessions (Phase 95).
pub const DEFAULT_MAX_SESSIONS: usize = 1000;

/// Cached partition fetch parameters inside a session.
#[derive(Debug, Clone)]
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
    pub fn should_omit_unchanged(&self, hwm: i64, lso: i64, records_empty: bool, error: i16) -> bool {
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
#[derive(Debug, Clone)]
pub struct SessionTopic {
    /// Wire identity to echo on responses (name or TopicId UUID).
    pub wire: TopicWireId,
    /// Resolved topic name (empty when unknown TopicId).
    pub name: String,
    /// Partitions keyed by partition index.
    pub partitions: HashMap<i32, SessionPartition>,
}

/// One active fetch session.
#[derive(Debug, Clone)]
pub struct FetchSession {
    /// Next expected `session_epoch` from the client.
    pub epoch: i32,
    /// Topics keyed by a stable map key (name, or `id:<hex>` for unknown UUID).
    pub topics: HashMap<String, SessionTopic>,
    /// Last successful session Fetch activity (unix ms; Phase 95).
    pub last_activity_ms: i64,
}

/// Process-local session table with idle TTL + max concurrent (Phase 95).
#[derive(Debug)]
pub struct FetchSessionManager {
    sessions: Mutex<HashMap<i32, FetchSession>>,
    next_id: AtomicI32,
    /// Idle TTL in ms; `0` disables idle eviction.
    idle_timeout_ms: AtomicU64,
    /// Max concurrent sessions; `0` = unlimited.
    max_sessions: AtomicUsize,
    /// Total sessions removed by idle TTL or LRU pressure.
    evicted_total: AtomicU64,
    /// Sessions removed by idle TTL only (Phase 97; subset of `evicted_total`).
    idle_evicted_total: AtomicU64,
}

impl Default for FetchSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchSessionManager {
    /// Manager with defaults from env (`VOLANT_FETCH_SESSION_*`) or Phase 95 defaults.
    pub fn new() -> Self {
        Self::with_limits(default_idle_timeout_ms(), default_max_sessions())
    }

    /// Manager with explicit limits (tests).
    pub fn with_limits(idle_timeout_ms: u64, max_sessions: usize) -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            next_id: AtomicI32::new(1),
            idle_timeout_ms: AtomicU64::new(idle_timeout_ms),
            max_sessions: AtomicUsize::new(max_sessions),
            evicted_total: AtomicU64::new(0),
            idle_evicted_total: AtomicU64::new(0),
        }
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
        before.saturating_sub(guard.len())
    }

    fn alloc_id(&self) -> i32 {
        // Skip 0 (INVALID_SESSION_ID). Wrap into positive range if needed.
        loop {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            if id > 0 {
                return id;
            }
            // Overflow / non-positive: reset and retry.
            let _ = self.next_id.compare_exchange(
                id.wrapping_add(1),
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
    pub fn close(&self, session_id: i32) {
        if session_id != 0 {
            self.sessions.lock().remove(&session_id);
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
            },
        );
        id
    }

    /// Validate incremental request epoch and advance expected epoch.
    ///
    /// Lazy-evicts idle sessions first. Returns `Ok(())` or Kafka top-level
    /// error code (70 / 71). Touches activity on success.
    pub fn begin_incremental(&self, session_id: i32, epoch: i32) -> Result<(), i16> {
        self.begin_incremental_at(session_id, epoch, now_ms())
    }

    /// Incremental begin with explicit timestamp (unit tests).
    pub fn begin_incremental_at(
        &self,
        session_id: i32,
        epoch: i32,
        now_ms: i64,
    ) -> Result<(), i16> {
        let mut guard = self.sessions.lock();
        self.evict_idle_locked(&mut guard, now_ms);
        let Some(session) = guard.get_mut(&session_id) else {
            return Err(70); // FETCH_SESSION_ID_NOT_FOUND
        };
        if session.epoch != epoch {
            return Err(71); // INVALID_FETCH_SESSION_EPOCH
        }
        session.epoch = next_epoch(epoch);
        session.last_activity_ms = now_ms;
        Ok(())
    }

    /// Merge request topics/partitions into an existing session.
    ///
    /// New or updated partitions keep/replace fetch params; `last_hwm`/`last_lso`
    /// are preserved when the same partition already existed (offset updates do
    /// not clear the omit cache — empty-records + same HWM still omits).
    pub fn merge_topics(&self, session_id: i32, topics: &HashMap<String, SessionTopic>) {
        let mut guard = self.sessions.lock();
        let Some(session) = guard.get_mut(&session_id) else {
            return;
        };
        for (key, topic) in topics {
            let entry = session
                .topics
                .entry(key.clone())
                .or_insert_with(|| SessionTopic {
                    wire: topic.wire.clone(),
                    name: topic.name.clone(),
                    partitions: HashMap::new(),
                });
            // Prefer latest wire/name.
            entry.wire = topic.wire.clone();
            if !topic.name.is_empty() {
                entry.name = topic.name.clone();
            }
            for (pid, part) in &topic.partitions {
                let prev = entry.partitions.get(pid);
                let mut merged = part.clone();
                // Preserve omit cache across param merges unless the request
                // carried explicit last_* (request path always None).
                if merged.last_hwm.is_none() {
                    if let Some(p) = prev {
                        merged.last_hwm = p.last_hwm;
                        merged.last_lso = p.last_lso;
                    }
                }
                entry.partitions.insert(*pid, merged);
            }
        }
    }

    /// Apply forgotten_topics_data removals.
    pub fn forget(&self, session_id: i32, forgotten: &[(String, Vec<i32>)]) {
        if session_id == 0 || forgotten.is_empty() {
            return;
        }
        let mut guard = self.sessions.lock();
        let Some(session) = guard.get_mut(&session_id) else {
            return;
        };
        for (key, parts) in forgotten {
            if parts.is_empty() {
                // Empty partition list: drop whole topic (Kafka allows this).
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
    }

    /// Snapshot topics currently in the session (for empty-topics incremental).
    pub fn snapshot_topics(&self, session_id: i32) -> HashMap<String, SessionTopic> {
        self.sessions
            .lock()
            .get(&session_id)
            .map(|s| s.topics.clone())
            .unwrap_or_default()
    }

    /// Record HWM/LSO returned for a partition after it was included in a response.
    ///
    /// Only call for `error == 0` includes (Phase 91 MVP).
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
        let mut guard = self.sessions.lock();
        let Some(session) = guard.get_mut(&session_id) else {
            return;
        };
        let Some(topic) = session.topics.get_mut(topic_key) else {
            return;
        };
        let Some(part) = topic.partitions.get_mut(&partition) else {
            return;
        };
        part.last_hwm = Some(hwm);
        part.last_lso = Some(lso);
    }

    /// Drop idle sessions under the lock. Counts each removal as an eviction.
    fn evict_idle_locked(&self, sessions: &mut HashMap<i32, FetchSession>, now_ms: i64) {
        let idle_ms = self.idle_timeout_ms.load(Ordering::Relaxed);
        if idle_ms == 0 {
            return;
        }
        let idle_ms_i = idle_ms as i64;
        let before = sessions.len();
        sessions.retain(|_, s| now_ms.saturating_sub(s.last_activity_ms) <= idle_ms_i);
        let removed = before.saturating_sub(sessions.len()) as u64;
        if removed > 0 {
            self.evicted_total.fetch_add(removed, Ordering::Relaxed);
            self.idle_evicted_total.fetch_add(removed, Ordering::Relaxed);
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

// Minimal hex encode without extra crate dependency — use a tiny helper.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(offset: i64) -> SessionPartition {
        SessionPartition::new(offset, -1, -1, 1_000_000)
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
        let mut topics = HashMap::new();
        let mut parts = HashMap::new();
        parts.insert(0, part(0));
        topics.insert(
            "t".into(),
            SessionTopic {
                wire: TopicWireId::Name("t".into()),
                name: "t".into(),
                partitions: parts,
            },
        );
        let id = mgr.create_at(topics, 1_000);
        mgr.note_returned(id, "t", 0, 5, 5);
        let snap = mgr.snapshot_topics(id);
        assert_eq!(snap["t"].partitions[&0].last_hwm, Some(5));
        assert_eq!(snap["t"].partitions[&0].last_lso, Some(5));

        // Merge with new fetch offset; cache preserved.
        let mut upd = HashMap::new();
        let mut uparts = HashMap::new();
        uparts.insert(0, part(5));
        upd.insert(
            "t".into(),
            SessionTopic {
                wire: TopicWireId::Name("t".into()),
                name: "t".into(),
                partitions: uparts,
            },
        );
        mgr.merge_topics(id, &upd);
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
}
