//! Live broker set and controller election (lowest live id).

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Tracks last-seen heartbeats and derives the controller.
#[derive(Debug, Clone)]
pub struct Membership {
    /// broker_id → last heartbeat time.
    last_seen: HashMap<u32, Instant>,
    /// Session timeout.
    session_timeout: Duration,
    /// This node's id.
    self_id: u32,
}

impl Membership {
    /// Create membership with `self_id` marked live.
    pub fn new(self_id: u32, session_timeout_ms: u32, initial_ids: &[u32]) -> Self {
        let now = Instant::now();
        let mut last_seen = HashMap::new();
        // Mark self live; other brokers become live on first heartbeat.
        last_seen.insert(self_id, now);
        for &id in initial_ids {
            // Optimistically mark configured brokers live at start so controller
            // is the lowest configured id until heartbeats prove otherwise.
            last_seen.entry(id).or_insert(now);
        }
        Self {
            last_seen,
            session_timeout: Duration::from_millis(u64::from(session_timeout_ms)),
            self_id,
        }
    }

    /// Record a heartbeat from `broker_id`.
    pub fn heartbeat(&mut self, broker_id: u32) {
        self.last_seen.insert(broker_id, Instant::now());
    }

    /// Touch self (local heartbeat).
    pub fn touch_self(&mut self) {
        self.heartbeat(self.self_id);
    }

    /// Expire brokers whose last heartbeat is older than the session timeout.
    /// Returns the list of newly expired broker ids.
    pub fn expire(&mut self) -> Vec<u32> {
        let now = Instant::now();
        let mut dead = Vec::new();
        self.last_seen.retain(|id, t| {
            if *id == self.self_id {
                return true;
            }
            if now.duration_since(*t) > self.session_timeout {
                dead.push(*id);
                false
            } else {
                true
            }
        });
        dead
    }

    /// Live broker ids (sorted).
    pub fn live_brokers(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.last_seen.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// Whether `id` is currently considered live.
    pub fn is_live(&self, id: u32) -> bool {
        self.last_seen.contains_key(&id)
    }

    /// Controller = lowest live broker id.
    pub fn controller_id(&self) -> u32 {
        self.live_brokers()
            .into_iter()
            .next()
            .unwrap_or(self.self_id)
    }

    /// Whether this node is the controller.
    pub fn is_controller(&self) -> bool {
        self.controller_id() == self.self_id
    }

    /// Mark a broker immediately dead (e.g. connection failure).
    pub fn mark_dead(&mut self, id: u32) {
        if id != self.self_id {
            self.last_seen.remove(&id);
        }
    }

    /// Drop a configured id from the live set (dynamic remove).
    pub fn remove_id(&mut self, id: u32) {
        if id != self.self_id {
            self.last_seen.remove(&id);
        }
    }

    /// Reconcile last-seen with a new configured id set.
    ///
    /// Removed ids are dropped. Newly added ids are **not** marked live
    /// (they become live on heartbeat). Self always stays live.
    pub fn apply_configured_ids(&mut self, configured: &[u32]) {
        let set: std::collections::HashSet<u32> = configured.iter().copied().collect();
        self.last_seen
            .retain(|id, _| *id == self.self_id || set.contains(id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowest_id_is_controller() {
        let m = Membership::new(2, 3000, &[1, 2, 3]);
        assert_eq!(m.controller_id(), 1);
        assert!(!m.is_controller());
        let m1 = Membership::new(1, 3000, &[1, 2, 3]);
        assert!(m1.is_controller());
    }

    #[test]
    fn apply_configured_ids_drops_removed_keeps_new_offline() {
        let mut m = Membership::new(1, 3000, &[1, 2, 3]);
        assert!(m.is_live(3));
        m.apply_configured_ids(&[1, 2, 4]);
        assert!(!m.is_live(3));
        assert!(!m.is_live(4), "added id stays offline until heartbeat");
        assert!(m.is_live(1));
        m.heartbeat(4);
        assert!(m.is_live(4));
    }
}
