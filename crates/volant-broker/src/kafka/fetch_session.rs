//! In-memory Fetch session state (Phase 88 MVP).
//!
//! Process-local only: not durable, not shared across brokers. Tracks topic
//! partitions and last-seen fetch params so incremental (empty-topics) requests
//! can re-fetch the session set. Always returns full record data (no
//! omit-unchanged cache).

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
                entry.partitions.insert(*pid, part.clone());
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
}
