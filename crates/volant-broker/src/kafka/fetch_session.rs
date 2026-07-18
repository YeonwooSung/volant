//! In-memory Fetch session state (Phase 88 + Phase 91 omit-unchanged MVP).
//!
//! Process-local only: not durable, not shared across brokers. Tracks topic
//! partitions, last-seen fetch params, and last-returned HWM/LSO so empty-topics
//! incremental Fetch can omit partitions with no new data.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use parking_lot::Mutex;

use super::topic_id::TopicWireId;

/// Kafka `FetchSession.INITIAL_EPOCH` — create / full fetch.
pub const INITIAL_EPOCH: i32 = 0;
/// Kafka `FetchSession.FINAL_EPOCH` — close session; no new session.
pub const FINAL_EPOCH: i32 = -1;

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
}

/// Process-local session table.
#[derive(Debug)]
pub struct FetchSessionManager {
    sessions: Mutex<HashMap<i32, FetchSession>>,
    next_id: AtomicI32,
}

impl Default for FetchSessionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchSessionManager {
    /// Empty manager; session ids start at 1.
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            next_id: AtomicI32::new(1),
        }
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

    /// Close a session (no-op for id 0 / missing).
    pub fn close(&self, session_id: i32) {
        if session_id != 0 {
            self.sessions.lock().remove(&session_id);
        }
    }

    /// Create a new session with `expected_epoch = 1`. Returns assigned id.
    pub fn create(&self, topics: HashMap<String, SessionTopic>) -> i32 {
        let id = self.alloc_id();
        self.sessions.lock().insert(
            id,
            FetchSession {
                epoch: 1,
                topics,
            },
        );
        id
    }

    /// Validate incremental request epoch and advance expected epoch.
    ///
    /// Returns `Ok(())` or Kafka top-level error code (70 / 71).
    pub fn begin_incremental(&self, session_id: i32, epoch: i32) -> Result<(), i16> {
        let mut guard = self.sessions.lock();
        let Some(session) = guard.get_mut(&session_id) else {
            return Err(70); // FETCH_SESSION_ID_NOT_FOUND
        };
        if session.epoch != epoch {
            return Err(71); // INVALID_FETCH_SESSION_EPOCH
        }
        session.epoch = next_epoch(epoch);
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
        let mgr = FetchSessionManager::new();
        let id = mgr.create(HashMap::new());
        assert!(id > 0);
        assert!(mgr.begin_incremental(id, 1).is_ok());
        assert!(mgr.begin_incremental(id, 2).is_ok());
        // wrong epoch
        assert_eq!(mgr.begin_incremental(id, 2), Err(71));
        // unknown
        assert_eq!(mgr.begin_incremental(id + 99, 1), Err(70));
        mgr.close(id);
        assert_eq!(mgr.begin_incremental(id, 3), Err(70));
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
        let mgr = FetchSessionManager::new();
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
        let id = mgr.create(topics);
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
}
