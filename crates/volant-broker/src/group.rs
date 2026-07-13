//! Server-side consumer group coordinator.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use uuid::Uuid;
use volant_core::Result;
use volant_protocol::ErrorCode;

use crate::assignor::range_assign_multi;
use crate::offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};

/// Result of a JoinGroup call.
#[derive(Debug, Clone)]
pub struct JoinResult {
    /// Embedded error code (0 = ok).
    pub error_code: u16,
    /// Current generation.
    pub generation: u32,
    /// Assigned member id.
    pub member_id: String,
    /// This member's assignment: (topic, partition).
    pub assignment: Vec<(String, u32)>,
}

/// Result of Heartbeat.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// 0 ok; 9 rebalance.
    pub error_code: u16,
}

/// Result of LeaveGroup.
#[derive(Debug, Clone)]
pub struct LeaveResult {
    /// 0 ok.
    pub error_code: u16,
}

/// Result of OffsetCommit.
#[derive(Debug, Clone)]
pub struct CommitResult {
    /// 0 ok.
    pub error_code: u16,
}

/// Result of OffsetFetch.
#[derive(Debug, Clone)]
pub struct FetchOffsetsResult {
    /// 0 ok.
    pub error_code: u16,
    /// Entries (offset = u64::MAX if unknown).
    pub entries: Vec<StoredOffset>,
}

#[derive(Debug)]
struct Member {
    member_id: String,
    session_timeout_ms: u32,
    last_heartbeat: Instant,
    topics: Vec<String>,
    assignment: Vec<(String, u32)>,
}

#[derive(Debug)]
struct Group {
    #[allow(dead_code)]
    group_id: String,
    generation: u32,
    members: HashMap<String, Member>,
}

/// In-memory group membership + durable offsets.
#[derive(Debug)]
pub struct GroupCoordinator {
    groups: Mutex<HashMap<String, Group>>,
    offsets: OffsetStore,
}

impl GroupCoordinator {
    /// Create a coordinator with offsets under `data_dir`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            groups: Mutex::new(HashMap::new()),
            offsets: OffsetStore::new(data_dir)?,
        })
    }

    /// Join (or re-join) a group. Eager rebalance of all members.
    ///
    /// `partition_counts` is a callback: topic name → partition count.
    pub fn join<F>(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: u32,
        topics: Vec<String>,
        partition_counts: F,
    ) -> Result<JoinResult>
    where
        F: Fn(&str) -> Option<u32>,
    {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();
        let group = groups
            .entry(group_id.to_owned())
            .or_insert_with(|| Group {
                group_id: group_id.to_owned(),
                generation: 0,
                members: HashMap::new(),
            });

        let timeout = if session_timeout_ms == 0 {
            10_000
        } else {
            session_timeout_ms
        };

        // Existing member re-joining after rebalance detection: refresh state and
        // return current assignment without bumping generation (avoids thrashing
        // when multiple members re-sync after one join/leave).
        if !member_id.is_empty() {
            if let Some(existing) = group.members.get_mut(member_id) {
                let topics_changed = existing.topics != topics;
                existing.session_timeout_ms = timeout;
                existing.last_heartbeat = Instant::now();
                existing.topics = topics;
                if topics_changed {
                    group.generation = group.generation.wrapping_add(1);
                    reassign(group, &partition_counts);
                }
                let assignment = group
                    .members
                    .get(member_id)
                    .map(|m| m.assignment.clone())
                    .unwrap_or_default();
                return Ok(JoinResult {
                    error_code: 0,
                    generation: group.generation,
                    member_id: member_id.to_owned(),
                    assignment,
                });
            }
        }

        let mid = if member_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            // Unknown member_id: accept provided id (e.g. restart with sticky id).
            member_id.to_owned()
        };

        group.members.insert(
            mid.clone(),
            Member {
                member_id: mid.clone(),
                session_timeout_ms: timeout,
                last_heartbeat: Instant::now(),
                topics,
                assignment: Vec::new(),
            },
        );

        group.generation = group.generation.wrapping_add(1);
        reassign(group, &partition_counts);

        let assignment = group
            .members
            .get(&mid)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        Ok(JoinResult {
            error_code: 0,
            generation: group.generation,
            member_id: mid,
            assignment,
        })
    }

    /// Heartbeat; returns rebalance error if generation mismatch or unknown member.
    pub fn heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
    ) -> HeartbeatResult {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return HeartbeatResult {
                error_code: ErrorCode::UnknownMemberId as u16,
            };
        };
        let Some(member) = group.members.get_mut(member_id) else {
            return HeartbeatResult {
                error_code: ErrorCode::UnknownMemberId as u16,
            };
        };
        if generation != group.generation {
            return HeartbeatResult {
                error_code: ErrorCode::RebalanceInProgress as u16,
            };
        }
        member.last_heartbeat = Instant::now();
        HeartbeatResult { error_code: 0 }
    }

    /// Leave group and rebalance remaining members.
    pub fn leave<F>(
        &self,
        group_id: &str,
        member_id: &str,
        partition_counts: F,
    ) -> LeaveResult
    where
        F: Fn(&str) -> Option<u32>,
    {
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return LeaveResult { error_code: 0 };
        };
        if group.members.remove(member_id).is_none() {
            return LeaveResult {
                error_code: ErrorCode::UnknownMemberId as u16,
            };
        }
        group.generation = group.generation.wrapping_add(1);
        reassign(group, &partition_counts);
        if group.members.is_empty() {
            groups.remove(group_id);
        }
        LeaveResult { error_code: 0 }
    }

    /// Commit offsets. Generation `0` skips membership checks (admin/CLI).
    pub fn commit_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        entries: &[(String, u32, u64, String)],
    ) -> Result<CommitResult> {
        if generation != 0 {
            let groups = self.groups.lock();
            let Some(group) = groups.get(group_id) else {
                return Ok(CommitResult {
                    error_code: ErrorCode::UnknownMemberId as u16,
                });
            };
            if !member_id.is_empty() && !group.members.contains_key(member_id) {
                return Ok(CommitResult {
                    error_code: ErrorCode::UnknownMemberId as u16,
                });
            }
            if generation != group.generation {
                return Ok(CommitResult {
                    error_code: ErrorCode::IllegalGeneration as u16,
                });
            }
        }
        for (topic, partition, offset, metadata) in entries {
            self.offsets
                .commit(group_id, topic, *partition, *offset, metadata)?;
        }
        Ok(CommitResult { error_code: 0 })
    }

    /// List known group ids (active membership + durable offset directories).
    pub fn list_group_ids(&self) -> Vec<String> {
        let mut set: HashMap<String, ()> = HashMap::new();
        for gid in self.groups.lock().keys() {
            set.insert(gid.clone(), ());
        }
        if let Ok(disk) = self.offsets.list_group_ids() {
            for gid in disk {
                set.insert(gid, ());
            }
        }
        let mut out: Vec<_> = set.into_keys().collect();
        out.sort();
        out
    }

    /// Fetch offsets. Empty `entries` → all committed for group.
    pub fn fetch_offsets(
        &self,
        group_id: &str,
        entries: &[(String, u32)],
    ) -> Result<FetchOffsetsResult> {
        if entries.is_empty() {
            let all = self.offsets.fetch_all(group_id)?;
            return Ok(FetchOffsetsResult {
                error_code: 0,
                entries: all,
            });
        }
        let mut out = Vec::with_capacity(entries.len());
        for (topic, partition) in entries {
            let (offset, metadata) = self.offsets.fetch(group_id, topic, *partition)?;
            out.push(StoredOffset {
                topic: topic.clone(),
                partition: *partition,
                offset,
                metadata,
            });
        }
        Ok(FetchOffsetsResult {
            error_code: 0,
            entries: out,
        })
    }

    /// Expire stale members (session timeout). Safe to call periodically.
    pub fn expire_sessions<F>(&self, partition_counts: F)
    where
        F: Fn(&str) -> Option<u32>,
    {
        let mut groups = self.groups.lock();
        let mut empty = Vec::new();
        for (gid, group) in groups.iter_mut() {
            let before = group.members.len();
            group.members.retain(|_, m| {
                m.last_heartbeat.elapsed()
                    <= Duration::from_millis(u64::from(m.session_timeout_ms))
            });
            if group.members.len() != before {
                group.generation = group.generation.wrapping_add(1);
                reassign(group, &partition_counts);
            }
            if group.members.is_empty() {
                empty.push(gid.clone());
            }
        }
        for gid in empty {
            groups.remove(&gid);
        }
    }

    /// Internal expire without reassignment (used before ops that reassign anyway).
    fn expire_sessions_inner(&self) {
        let mut groups = self.groups.lock();
        for group in groups.values_mut() {
            let before = group.members.len();
            group.members.retain(|_, m| {
                m.last_heartbeat.elapsed()
                    <= Duration::from_millis(u64::from(m.session_timeout_ms))
            });
            if group.members.len() != before {
                group.generation = group.generation.wrapping_add(1);
                // Assignment will be fixed by the following join/leave/heartbeat path
                // or by the public expire_sessions with partition counts.
                // Clear stale assignments until reassigned.
                for m in group.members.values_mut() {
                    m.assignment.clear();
                }
            }
        }
        groups.retain(|_, g| !g.members.is_empty());
    }

    /// Peek assignment for tests.
    #[cfg(test)]
    pub fn assignment(&self, group_id: &str, member_id: &str) -> Option<Vec<(String, u32)>> {
        let groups = self.groups.lock();
        groups
            .get(group_id)
            .and_then(|g| g.members.get(member_id).map(|m| m.assignment.clone()))
    }

    /// Peek generation for tests.
    #[cfg(test)]
    pub fn generation(&self, group_id: &str) -> Option<u32> {
        self.groups.lock().get(group_id).map(|g| g.generation)
    }
}

fn reassign<F>(group: &mut Group, partition_counts: &F)
where
    F: Fn(&str) -> Option<u32>,
{
    if group.members.is_empty() {
        return;
    }
    let mut member_ids = Vec::new();
    let mut member_topics = Vec::new();
    for m in group.members.values() {
        member_ids.push(m.member_id.clone());
        member_topics.push(m.topics.clone());
    }
    let mut counts = HashMap::new();
    for topics in &member_topics {
        for t in topics {
            if !counts.contains_key(t) {
                if let Some(n) = partition_counts(t) {
                    counts.insert(t.clone(), n);
                }
            }
        }
    }
    let assigns = range_assign_multi(&member_ids, &member_topics, &counts);
    for (i, mid) in member_ids.iter().enumerate() {
        if let Some(m) = group.members.get_mut(mid) {
            m.assignment = assigns[i].clone();
        }
    }
}

// Silence unused import warning in non-test builds if any.
#[allow(dead_code)]
fn _offset_unknown() -> u64 {
    OFFSET_UNKNOWN
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "volant-group-{}-{}",
            std::process::id(),
            nanos
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn counts(_t: &str) -> Option<u32> {
        Some(4)
    }

    #[test]
    fn two_members_disjoint_full_cover() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], counts)
            .unwrap();
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], counts)
            .unwrap();
        assert_eq!(j1.error_code, 0);
        assert_eq!(j2.error_code, 0);
        // After j2, both rebalanced — re-fetch via assignment helper.
        // j1's returned assignment is stale; current state:
        let a1 = coord.assignment("g", &j1.member_id).unwrap();
        let a2 = coord.assignment("g", &j2.member_id).unwrap();
        let mut all: Vec<u32> = a1
            .iter()
            .chain(a2.iter())
            .map(|(_, p)| *p)
            .collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3]);
        let s1: std::collections::HashSet<_> = a1.iter().cloned().collect();
        let s2: std::collections::HashSet<_> = a2.iter().cloned().collect();
        assert!(s1.is_disjoint(&s2));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn leave_gives_remaining_all_partitions() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], counts)
            .unwrap();
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], counts)
            .unwrap();
        let leave = coord.leave("g", &j2.member_id, counts);
        assert_eq!(leave.error_code, 0);
        let a1 = coord.assignment("g", &j1.member_id).unwrap();
        let parts: Vec<u32> = a1.iter().map(|(_, p)| *p).collect();
        assert_eq!(parts, vec![0, 1, 2, 3]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_fetch_across_recreate() {
        let dir = temp_dir();
        {
            let coord = GroupCoordinator::new(&dir).unwrap();
            let r = coord
                .commit_offsets(
                    "g",
                    "",
                    0,
                    &[("events".into(), 0, 7, "x".into())],
                )
                .unwrap();
            assert_eq!(r.error_code, 0);
        }
        {
            let coord = GroupCoordinator::new(&dir).unwrap();
            let r = coord
                .fetch_offsets("g", &[("events".into(), 0)])
                .unwrap();
            assert_eq!(r.entries.len(), 1);
            assert_eq!(r.entries[0].offset, 7);
            assert_eq!(r.entries[0].metadata, "x");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn heartbeat_generation_mismatch() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, vec!["t".into()], counts)
            .unwrap();
        let ok = coord.heartbeat("g", &j.member_id, j.generation);
        assert_eq!(ok.error_code, 0);
        let bad = coord.heartbeat("g", &j.member_id, j.generation + 1);
        assert_eq!(bad.error_code, ErrorCode::RebalanceInProgress as u16);
        let _ = fs::remove_dir_all(&dir);
    }
}
