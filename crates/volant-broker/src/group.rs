//! Server-side consumer group coordinator.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use uuid::Uuid;
use volant_core::Result;
use volant_protocol::ErrorCode;

use crate::assignor::sticky_assign_multi;
use crate::offset_store::{OffsetStore, StoredOffset, OFFSET_UNKNOWN};

/// Prefix for static membership member ids (Phase 12).
pub const STATIC_MEMBER_PREFIX: &str = "static:";

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
    /// Partitions this member lost vs its prior assignment (Phase 17 cooperative).
    /// Empty when the member is new or the coordinator no longer holds the prior list.
    pub revoked: Vec<(String, u32)>,
    /// Live member ids at this generation (v0.211), including the joiner, sorted.
    /// Empty group cannot happen on a successful join.
    pub members: Vec<String>,
}

/// Result of Heartbeat.
#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    /// 0 ok; 9 rebalance.
    pub error_code: u16,
}

/// Result of SyncGroup (v0.215 generation confirm).
#[derive(Debug, Clone)]
pub struct SyncGroupResult {
    /// 0 ok; 9 rebalance / wrong generation; 10 unknown member.
    pub error_code: u16,
    /// Current coordinator assignment for this member (empty on error).
    pub assignment: Vec<(String, u32)>,
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

/// One member in a group describe snapshot (Phase 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberDescription {
    /// Member id.
    pub member_id: String,
    /// Subscribed topics.
    pub topics: Vec<String>,
    /// Current assignment (topic, partition).
    pub assignment: Vec<(String, u32)>,
}

/// Live group membership snapshot (Phase 11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    /// Group id.
    pub group_id: String,
    /// Current generation.
    pub generation: u32,
    /// Members (sorted by member id).
    pub members: Vec<GroupMemberDescription>,
}

/// One group listing entry (Phase 12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListEntry {
    /// Group id.
    pub group_id: String,
    /// True when at least one live member.
    pub stable: bool,
    /// Live member count.
    pub member_count: u32,
    /// Current generation (`0` if empty).
    pub generation: u32,
}

/// Derive a stable member id from a group instance id.
pub fn static_member_id(instance_id: &str) -> String {
    format!("{STATIC_MEMBER_PREFIX}{instance_id}")
}

#[derive(Debug)]
struct Member {
    member_id: String,
    session_timeout_ms: u32,
    last_heartbeat: Instant,
    topics: Vec<String>,
    /// Current coordinator assignment (updated on every rebalance).
    assignment: Vec<(String, u32)>,
    /// Assignment last returned to this member on JoinGroup (Phase 17).
    /// Used to compute `revoked` when the member re-syncs after another
    /// member's join already updated `assignment`.
    delivered: Vec<(String, u32)>,
    /// Last generation this member confirmed via SyncGroup (0 = never).
    synced_generation: u32,
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
    ///
    /// When `group_instance_id` is non-empty and `member_id` is empty, the member
    /// id is derived as `static:{instance_id}` (Phase 12 static membership).
    ///
    /// New-member Join (or existing Join with a topics change) is fenced
    /// with error 9 unless every live member has confirmed the current
    /// generation via SyncGroup. The joiner is not auto-synced. Existing
    /// members re-joining with the same topics are never fenced and do
    /// not bump generation or mark themselves synced.
    pub fn join<F>(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: u32,
        topics: Vec<String>,
        group_instance_id: &str,
        partition_counts: F,
    ) -> Result<JoinResult>
    where
        F: Fn(&str) -> Option<u32>,
    {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();
        let group = groups.entry(group_id.to_owned()).or_insert_with(|| Group {
            group_id: group_id.to_owned(),
            generation: 0,
            members: HashMap::new(),
        });

        let timeout = if session_timeout_ms == 0 {
            10_000
        } else {
            session_timeout_ms
        };

        // Resolve member id: explicit → static instance → new UUID.
        let resolved_id = if !member_id.is_empty() {
            member_id.to_owned()
        } else if !group_instance_id.is_empty() {
            static_member_id(group_instance_id)
        } else {
            String::new()
        };

        // Existing member re-joining after rebalance detection: refresh state and
        // return current assignment without bumping generation (avoids thrashing
        // when multiple members re-sync after one join/leave).
        if !resolved_id.is_empty() {
            if group.members.contains_key(&resolved_id) {
                let topics_changed = group
                    .members
                    .get(&resolved_id)
                    .is_some_and(|m| m.topics != topics);
                if topics_changed && !all_synced(group) {
                    return Ok(fenced_join(group, resolved_id));
                }
                if let Some(existing) = group.members.get_mut(&resolved_id) {
                    existing.session_timeout_ms = timeout;
                    existing.last_heartbeat = Instant::now();
                    existing.topics = topics;
                }
                if topics_changed {
                    group.generation = group.generation.wrapping_add(1);
                    reassign(group, &partition_counts);
                    // Joiner is not auto-synced.
                }
                let assignment = group
                    .members
                    .get(&resolved_id)
                    .map(|m| m.assignment.clone())
                    .unwrap_or_default();
                // Revoked = last delivered to this member − new assignment
                // (covers both topics-change reassign and re-sync after peer join).
                let previous = group
                    .members
                    .get(&resolved_id)
                    .map(|m| m.delivered.clone())
                    .unwrap_or_default();
                let revoked = partition_diff(&previous, &assignment);
                if let Some(m) = group.members.get_mut(&resolved_id) {
                    m.delivered = assignment.clone();
                }
                return Ok(JoinResult {
                    error_code: 0,
                    generation: group.generation,
                    member_id: resolved_id,
                    assignment,
                    revoked,
                    members: live_member_ids(group),
                });
            }
        }

        // New member: fence unless every live member confirmed this generation.
        // Empty group (first Join) is always all_synced.
        if !all_synced(group) {
            return Ok(fenced_join(group, resolved_id));
        }

        let mid = if resolved_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            // Unknown member_id / static instance: accept provided id.
            resolved_id
        };

        // New member (or unknown id): no prior assignment to revoke.
        // synced_generation stays 0 — the joiner is not auto-synced.
        group.members.insert(
            mid.clone(),
            Member {
                member_id: mid.clone(),
                session_timeout_ms: timeout,
                last_heartbeat: Instant::now(),
                topics,
                assignment: Vec::new(),
                delivered: Vec::new(),
                synced_generation: 0,
            },
        );

        group.generation = group.generation.wrapping_add(1);
        reassign(group, &partition_counts);

        let assignment = group
            .members
            .get(&mid)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();
        if let Some(m) = group.members.get_mut(&mid) {
            m.delivered = assignment.clone();
        }

        Ok(JoinResult {
            error_code: 0,
            generation: group.generation,
            member_id: mid,
            assignment,
            revoked: Vec::new(),
            members: live_member_ids(group),
        })
    }

    /// Heartbeat; returns rebalance error if generation mismatch or unknown member.
    pub fn heartbeat(&self, group_id: &str, member_id: &str, generation: u32) -> HeartbeatResult {
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

    /// Confirm this member has observed `generation` (v0.215 fence).
    ///
    /// Same 9/10 as [`Self::heartbeat`]. On success sets
    /// `synced_generation = generation` and returns the current
    /// assignment. Leader assignment bytes are ignored by the caller.
    /// Heartbeat does not confirm.
    pub fn sync_group(&self, group_id: &str, member_id: &str, generation: u32) -> SyncGroupResult {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return SyncGroupResult {
                error_code: ErrorCode::UnknownMemberId as u16,
                assignment: Vec::new(),
            };
        };
        let Some(member) = group.members.get_mut(member_id) else {
            return SyncGroupResult {
                error_code: ErrorCode::UnknownMemberId as u16,
                assignment: Vec::new(),
            };
        };
        if generation != group.generation {
            return SyncGroupResult {
                error_code: ErrorCode::RebalanceInProgress as u16,
                assignment: Vec::new(),
            };
        }
        member.last_heartbeat = Instant::now();
        member.synced_generation = generation;
        let assignment = member.assignment.clone();
        SyncGroupResult {
            error_code: 0,
            assignment,
        }
    }

    /// Leave group and rebalance remaining members.
    pub fn leave<F>(&self, group_id: &str, member_id: &str, partition_counts: F) -> LeaveResult
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

    /// Snapshot of a live consumer group (Phase 11 DescribeGroup).
    ///
    /// Does not expire sessions (use the background expiry task for that).
    pub fn describe_group(&self, group_id: &str) -> Option<GroupDescription> {
        let groups = self.groups.lock();
        let group = groups.get(group_id)?;
        let mut members: Vec<GroupMemberDescription> = group
            .members
            .values()
            .map(|m| GroupMemberDescription {
                member_id: m.member_id.clone(),
                topics: m.topics.clone(),
                assignment: m.assignment.clone(),
            })
            .collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        Some(GroupDescription {
            group_id: group_id.to_owned(),
            generation: group.generation,
            members,
        })
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

    /// List groups with state for ListGroups (Phase 12).
    pub fn list_groups(&self) -> Vec<GroupListEntry> {
        let mut set: HashMap<String, ()> = HashMap::new();
        let groups = self.groups.lock();
        for gid in groups.keys() {
            set.insert(gid.clone(), ());
        }
        if let Ok(disk) = self.offsets.list_group_ids() {
            for gid in disk {
                set.insert(gid, ());
            }
        }
        let mut out: Vec<GroupListEntry> = set
            .into_keys()
            .map(|group_id| {
                if let Some(g) = groups.get(&group_id) {
                    GroupListEntry {
                        group_id,
                        stable: !g.members.is_empty(),
                        member_count: g.members.len() as u32,
                        generation: g.generation,
                    }
                } else {
                    GroupListEntry {
                        group_id,
                        stable: false,
                        member_count: 0,
                        generation: 0,
                    }
                }
            })
            .collect();
        out.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        out
    }

    /// Delete committed offsets (Phase 12). Empty `entries` deletes all for the group.
    pub fn delete_offsets(&self, group_id: &str, entries: &[(String, u32)]) -> Result<u32> {
        self.offsets.delete_many(group_id, entries)
    }

    /// Delete a consumer group (Phase 27 Kafka DeleteGroups).
    ///
    /// Fails with error code `68` (`NON_EMPTY_GROUP`) when live members remain.
    /// Returns `69` (`GROUP_ID_NOT_FOUND`) when the group has neither members
    /// nor durable offsets. On success removes membership and all offsets.
    pub fn delete_group(&self, group_id: &str) -> Result<u16> {
        {
            let groups = self.groups.lock();
            if let Some(g) = groups.get(group_id) {
                if !g.members.is_empty() {
                    return Ok(68); // NON_EMPTY_GROUP
                }
            }
        }
        let had_members = {
            let mut groups = self.groups.lock();
            groups.remove(group_id).is_some()
        };
        let had_offsets = self.offsets.list_group_ids()?.iter().any(|g| g == group_id);
        if !had_members && !had_offsets {
            return Ok(69); // GROUP_ID_NOT_FOUND
        }
        // Delete all offsets for the group.
        let _ = self.offsets.delete_many(group_id, &[])?;
        Ok(0)
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
                m.last_heartbeat.elapsed() <= Duration::from_millis(u64::from(m.session_timeout_ms))
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

    /// Drop dead members only. Do not bump generation or clear assignments.
    /// A subsequent Join may bump+reassign to heal; public [`Self::expire_sessions`]
    /// still bumps and reassigns (survivors become unsynced).
    fn expire_sessions_inner(&self) {
        let mut groups = self.groups.lock();
        for group in groups.values_mut() {
            group.members.retain(|_, m| {
                m.last_heartbeat.elapsed() <= Duration::from_millis(u64::from(m.session_timeout_ms))
            });
        }
        groups.retain(|_, g| !g.members.is_empty());
    }

    /// Peek assignment for a live member (Kafka SyncGroup + tests).
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

/// Live member ids, stable-sorted (v0.211 JoinGroup trailer).
fn live_member_ids(group: &Group) -> Vec<String> {
    let mut ids: Vec<String> = group.members.keys().cloned().collect();
    ids.sort();
    ids
}

/// Every live member has confirmed `group.generation` via SyncGroup.
/// Empty group (first Join) is always synced.
fn all_synced(group: &Group) -> bool {
    group.members.is_empty()
        || group
            .members
            .values()
            .all(|m| m.synced_generation == group.generation)
}

fn fenced_join(group: &Group, member_id: String) -> JoinResult {
    JoinResult {
        error_code: ErrorCode::RebalanceInProgress as u16,
        generation: group.generation,
        member_id,
        assignment: Vec::new(),
        revoked: Vec::new(),
        members: live_member_ids(group),
    }
}

/// Partitions in `old` that are not in `new` (set difference).
fn partition_diff(old: &[(String, u32)], new: &[(String, u32)]) -> Vec<(String, u32)> {
    let new_set: std::collections::HashSet<&(String, u32)> = new.iter().collect();
    old.iter()
        .filter(|tp| !new_set.contains(tp))
        .cloned()
        .collect()
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
    let mut previous = Vec::new();
    for m in group.members.values() {
        member_ids.push(m.member_id.clone());
        member_topics.push(m.topics.clone());
        previous.push(m.assignment.clone());
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
    // Phase 11: sticky by default (minimizes churn vs range).
    let assigns = sticky_assign_multi(&member_ids, &member_topics, &counts, &previous);
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
        let dir =
            std::env::temp_dir().join(format!("volant-group-{}-{}", std::process::id(), nanos));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn counts(_t: &str) -> Option<u32> {
        Some(4)
    }

    fn sync_ok(coord: &GroupCoordinator, group: &str, member: &str, gen: u32) {
        let r = coord.sync_group(group, member, gen);
        assert_eq!(r.error_code, 0, "sync_group {member}");
    }

    #[test]
    fn two_members_disjoint_full_cover() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.error_code, 0);
        assert_eq!(j2.error_code, 0);
        // After j2, both rebalanced — re-fetch via assignment helper.
        // j1's returned assignment is stale; current state:
        let a1 = coord.assignment("g", &j1.member_id).unwrap();
        let a2 = coord.assignment("g", &j2.member_id).unwrap();
        let mut all: Vec<u32> = a1.iter().chain(a2.iter()).map(|(_, p)| *p).collect();
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
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
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
                .commit_offsets("g", "", 0, &[("events".into(), 0, 7, "x".into())])
                .unwrap();
            assert_eq!(r.error_code, 0);
        }
        {
            let coord = GroupCoordinator::new(&dir).unwrap();
            let r = coord.fetch_offsets("g", &[("events".into(), 0)]).unwrap();
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
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let ok = coord.heartbeat("g", &j.member_id, j.generation);
        assert_eq!(ok.error_code, 0);
        let bad = coord.heartbeat("g", &j.member_id, j.generation + 1);
        assert_eq!(bad.error_code, ErrorCode::RebalanceInProgress as u16);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_group_peek_after_join() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let hb = coord.heartbeat("g", &j.member_id, j.generation);
        assert_eq!(hb.error_code, 0);
        assert_eq!(
            coord.assignment("g", &j.member_id).as_ref(),
            Some(&j.assignment)
        );
        let unknown = coord.heartbeat("g", "nobody", j.generation);
        assert_eq!(unknown.error_code, ErrorCode::UnknownMemberId as u16);
        let mismatch = coord.heartbeat("g", &j.member_id, j.generation + 1);
        assert_eq!(mismatch.error_code, ErrorCode::RebalanceInProgress as u16);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn topics_change_returns_revoked_partitions() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let counts_multi = |t: &str| -> Option<u32> {
            match t {
                "t" | "u" => Some(4),
                _ => None,
            }
        };
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts_multi)
            .unwrap();
        assert!(j1.revoked.is_empty());
        assert_eq!(j1.assignment.len(), 4);
        sync_ok(&coord, "g", &j1.member_id, j1.generation);

        // Change subscription from t → u: all t partitions are revoked.
        let j2 = coord
            .join(
                "g",
                &j1.member_id,
                10_000,
                vec!["u".into()],
                "",
                counts_multi,
            )
            .unwrap();
        assert_eq!(j2.error_code, 0);
        let revoked: std::collections::HashSet<_> = j2.revoked.iter().cloned().collect();
        let old: std::collections::HashSet<_> = j1.assignment.iter().cloned().collect();
        assert_eq!(revoked, old);
        assert!(j2.assignment.iter().all(|(t, _)| t == "u"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_member_join_has_empty_revoked() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert!(j1.revoked.is_empty());
        assert!(j2.revoked.is_empty());
        assert_eq!(j2.members.len(), 2);
        assert!(j2.members.contains(&j1.member_id));
        assert!(j2.members.contains(&j2.member_id));
        let mut sorted = j2.members.clone();
        sorted.sort();
        assert_eq!(j2.members, sorted);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resync_after_peer_join_returns_revoked() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.assignment.len(), 4);
        let first: std::collections::HashSet<_> = j1.assignment.iter().cloned().collect();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);

        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert!(j2.revoked.is_empty());

        // Member 1 re-syncs: should see revoked = old − new.
        let j1b = coord
            .join("g", &j1.member_id, 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let now: std::collections::HashSet<_> = j1b.assignment.iter().cloned().collect();
        let revoked: std::collections::HashSet<_> = j1b.revoked.iter().cloned().collect();
        let expected_revoked: std::collections::HashSet<_> =
            first.difference(&now).cloned().collect();
        assert_eq!(revoked, expected_revoked);
        assert!(
            !j1b.revoked.is_empty(),
            "solo→two members should revoke some"
        );
        // Retained partitions are sticky subset of original.
        assert!(now.is_subset(&first));
        let _ = fs::remove_dir_all(&dir);
    }

    fn member_count(coord: &GroupCoordinator, group: &str) -> usize {
        coord
            .describe_group(group)
            .map(|d| d.members.len())
            .unwrap_or(0)
    }

    #[test]
    fn second_join_without_sync_is_fenced() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.error_code, 0);
        assert_eq!(coord.generation("g"), Some(1));
        assert_eq!(member_count(&coord, "g"), 1);

        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j2.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), Some(1));
        assert_eq!(member_count(&coord, "g"), 1);
        assert_eq!(
            coord.assignment("g", &j1.member_id).as_ref(),
            Some(&j1.assignment)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_group_lifts_fence_for_second_join() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j2.error_code, 0);
        assert_eq!(coord.generation("g"), Some(2));
        assert_eq!(member_count(&coord, "g"), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn third_join_blocked_until_both_sync_or_leave() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j2.error_code, 0);

        let blocked = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), Some(2));
        assert_eq!(member_count(&coord, "g"), 2);

        sync_ok(&coord, "g", &j1.member_id, j2.generation);
        let still = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(still.error_code, ErrorCode::RebalanceInProgress as u16);

        sync_ok(&coord, "g", &j2.member_id, j2.generation);
        let j3 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j3.error_code, 0);
        assert_eq!(coord.generation("g"), Some(3));
        assert_eq!(member_count(&coord, "g"), 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn existing_member_rejoin_same_topics_during_fence() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let gen = coord.generation("g");
        let again = coord
            .join("g", &j1.member_id, 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(again.error_code, 0);
        assert_eq!(coord.generation("g"), gen);
        assert_eq!(member_count(&coord, "g"), 1);

        let blocked = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), gen);
        assert_eq!(member_count(&coord, "g"), 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn leave_bumps_without_sync_remaining_must_sync() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j2.generation);
        sync_ok(&coord, "g", &j2.member_id, j2.generation);

        let leave = coord.leave("g", &j2.member_id, counts);
        assert_eq!(leave.error_code, 0);
        let after_leave = coord.generation("g");
        assert_eq!(after_leave, Some(3));
        assert_eq!(member_count(&coord, "g"), 1);

        let blocked = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), after_leave);
        assert_eq!(member_count(&coord, "g"), 1);

        sync_ok(&coord, "g", &j1.member_id, after_leave.unwrap());
        let j3 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j3.error_code, 0);
        assert_eq!(coord.generation("g"), Some(4));
        assert_eq!(member_count(&coord, "g"), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_group_unknown_is_10_wrong_gen_is_9() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let unknown = coord.sync_group("g", "nobody", j1.generation);
        assert_eq!(unknown.error_code, ErrorCode::UnknownMemberId as u16);
        assert!(unknown.assignment.is_empty());
        let mismatch = coord.sync_group("g", &j1.member_id, j1.generation.wrapping_add(1));
        assert_eq!(mismatch.error_code, ErrorCode::RebalanceInProgress as u16);
        assert!(mismatch.assignment.is_empty());
        let ok = coord.sync_group("g", &j1.member_id, j1.generation);
        assert_eq!(ok.error_code, 0);
        assert_eq!(ok.assignment, j1.assignment);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_groups_empty_or_stable_supported_apis_stays_38() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        assert!(coord.list_groups().is_empty());
        let j1 = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        let listed = coord.list_groups();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].group_id, "g");
        assert!(listed[0].stable);
        assert_eq!(listed[0].member_count, 1);
        assert_eq!(listed[0].generation, j1.generation);
        let leave = coord.leave("g", &j1.member_id, counts);
        assert_eq!(leave.error_code, 0);
        // Empty group is removed from membership; ListGroups stays Empty/Stable.
        let after = coord.list_groups();
        assert!(after.iter().all(|e| !e.stable || e.member_count > 0));
        assert_eq!(crate::kafka::SUPPORTED_APIS.len(), 38);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expire_inner_does_not_stick_new_join() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let a = coord
            .join("g", "", 40, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &a.member_id, a.generation);
        let b = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &a.member_id, b.generation);
        sync_ok(&coord, "g", &b.member_id, b.generation);
        std::thread::sleep(std::time::Duration::from_millis(80));
        // A's session is dead. Inner expire is drop-dead-only so B stays
        // synced and C may join (heal via bump+reassign).
        let c = coord
            .join("g", "", 10_000, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(c.error_code, 0);
        assert_eq!(member_count(&coord, "g"), 2);
        assert!(coord.assignment("g", &a.member_id).is_none());
        assert!(coord.assignment("g", &b.member_id).is_some());
        let _ = fs::remove_dir_all(&dir);
    }
}
