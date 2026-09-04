//! Server-side consumer group coordinator.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use uuid::Uuid;
use volant_core::Result;
use volant_protocol::{ErrorCode, GroupState};

use crate::assignor::sticky_assign_multi;
use crate::offset_store::{OffsetStore, StoredOffset, LEADER_EPOCH_UNKNOWN, OFFSET_UNKNOWN};

/// Prefix for static membership member ids (Phase 12).
pub const STATIC_MEMBER_PREFIX: &str = "static:";

/// Join park budget when `rebalance_timeout_ms` is 0 / omitted (v0.231).
pub const DEFAULT_JOIN_PARK_MS: u32 = 1000;

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

/// Whether a group member currently owns a topic partition (v0.234).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owns {
    /// Member is live and assigned this partition.
    Allow,
    /// Unknown group or unknown member id.
    UnknownMember,
    /// Live member is not assigned this partition.
    NotAssigned,
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

/// Live group membership snapshot (Phase 11 / v0.218).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDescription {
    /// Group id.
    pub group_id: String,
    /// Current generation.
    pub generation: u32,
    /// Members (sorted by member id).
    pub members: Vec<GroupMemberDescription>,
    /// Empty / Stable / CompletingRebalance / PreparingRebalance (v0.230).
    pub state: GroupState,
}

/// One group listing entry (Phase 12 / v0.218).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupListEntry {
    /// Group id.
    pub group_id: String,
    /// Empty / Stable / CompletingRebalance / PreparingRebalance (v0.230).
    pub state: GroupState,
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
    /// Waiters for the v0.215 SyncGroup fence (released during the wait).
    join_park: Condvar,
    /// Per-group parked Join count (v0.230 List/Describe label). Not members.
    join_waiters: Mutex<HashMap<String, u32>>,
    offsets: OffsetStore,
}

impl GroupCoordinator {
    /// Create a coordinator with offsets under `data_dir`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            groups: Mutex::new(HashMap::new()),
            join_park: Condvar::new(),
            join_waiters: Mutex::new(HashMap::new()),
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
    /// New-member Join (or existing Join with a topics change) parks on
    /// `join_park` until every live member has confirmed the current
    /// generation via SyncGroup, or until the rebalance timeout. The wait
    /// releases `groups` so Heartbeat / SyncGroup / Leave on other
    /// connections can lift the fence. Timeout still returns error 9
    /// with no insert, no bump, no reassign. The joiner is not
    /// auto-synced. Existing members re-joining with the same topics
    /// are never fenced and do not bump generation or mark themselves
    /// synced.
    ///
    /// `session_timeout_ms` is member expiry only (`0` → `10_000`).
    /// Park budget is `rebalance_timeout_ms` (`0` → [`DEFAULT_JOIN_PARK_MS`]).
    pub fn join<F>(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout_ms: u32,
        rebalance_timeout_ms: u32,
        topics: Vec<String>,
        group_instance_id: &str,
        partition_counts: F,
    ) -> Result<JoinResult>
    where
        F: Fn(&str) -> Option<u32>,
    {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();

        let timeout = if session_timeout_ms == 0 {
            10_000
        } else {
            session_timeout_ms
        };
        let park_ms = if rebalance_timeout_ms == 0 {
            DEFAULT_JOIN_PARK_MS
        } else {
            rebalance_timeout_ms
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
        let existing = !resolved_id.is_empty()
            && groups
                .get(group_id)
                .is_some_and(|g| g.members.contains_key(&resolved_id));
        if existing {
            let topics_changed = groups
                .get(group_id)
                .and_then(|g| g.members.get(&resolved_id))
                .is_some_and(|m| m.topics != topics);
            if topics_changed
                && !park_until_all_synced(
                    &self.join_park,
                    &self.join_waiters,
                    &mut groups,
                    group_id,
                    park_ms,
                )
            {
                return Ok(fenced_join_lookup(&groups, group_id, resolved_id));
            }
            if let Some(group) = groups.get_mut(group_id) {
                if group.members.contains_key(&resolved_id) {
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
        }

        // New member: park unless every live member confirmed this generation.
        // Empty group (first Join) is always all_synced.
        if !park_until_all_synced(
            &self.join_park,
            &self.join_waiters,
            &mut groups,
            group_id,
            park_ms,
        ) {
            return Ok(fenced_join_lookup(&groups, group_id, resolved_id));
        }

        let group = groups.entry(group_id.to_owned()).or_insert_with(|| Group {
            group_id: group_id.to_owned(),
            generation: 0,
            members: HashMap::new(),
        });

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
    /// Confirm-only: no assignment apply. Same as
    /// [`Self::sync_group_with_assignments`] with an empty list.
    pub fn sync_group(&self, group_id: &str, member_id: &str, generation: u32) -> SyncGroupResult {
        self.sync_group_with_assignments(group_id, member_id, generation, &[])
    }

    /// Confirm `generation` and optionally apply decoded member assignments.
    ///
    /// Same 9/10 as [`Self::heartbeat`]. On success, for each `(member_id,
    /// assignment)` whose member exists in the group, replace that member's
    /// assignment; unknown member ids are skipped. Then set this caller's
    /// `synced_generation = generation` and return this member's assignment
    /// (Join peek if nothing applied). Heartbeat does not confirm.
    ///
    /// Empty / unparseable wire bytes are the caller's skip — this method
    /// never fails SyncGroup because an assignment did not decode.
    pub fn sync_group_with_assignments(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        assignments: &[(String, Vec<(String, u32)>)],
    ) -> SyncGroupResult {
        self.expire_sessions_inner();
        let mut groups = self.groups.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return SyncGroupResult {
                error_code: ErrorCode::UnknownMemberId as u16,
                assignment: Vec::new(),
            };
        };
        if !group.members.contains_key(member_id) {
            return SyncGroupResult {
                error_code: ErrorCode::UnknownMemberId as u16,
                assignment: Vec::new(),
            };
        }
        if generation != group.generation {
            return SyncGroupResult {
                error_code: ErrorCode::RebalanceInProgress as u16,
                assignment: Vec::new(),
            };
        }
        for (mid, parts) in assignments {
            if let Some(m) = group.members.get_mut(mid) {
                m.assignment = parts.clone();
            }
        }
        let member = group
            .members
            .get_mut(member_id)
            .expect("member exists after contains_key");
        member.last_heartbeat = Instant::now();
        member.synced_generation = generation;
        let assignment = member.assignment.clone();
        self.join_park.notify_all();
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
        self.join_park.notify_all();
        LeaveResult { error_code: 0 }
    }

    /// OffsetCommit membership fence (v0.219 / Kafka TxnOffsetCommit v3+).
    ///
    /// Unknown group or unknown member → 10; wrong generation → 11;
    /// non-empty member whose `synced_generation` does not match → 9.
    /// Empty `member_id` still checks group existence and generation.
    pub(crate) fn check_commit_fence(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
    ) -> u16 {
        let groups = self.groups.lock();
        let Some(group) = groups.get(group_id) else {
            return ErrorCode::UnknownMemberId as u16;
        };
        if !member_id.is_empty() && !group.members.contains_key(member_id) {
            return ErrorCode::UnknownMemberId as u16;
        }
        if generation != group.generation {
            return ErrorCode::IllegalGeneration as u16;
        }
        if !member_id.is_empty() {
            if let Some(member) = group.members.get(member_id) {
                if member.synced_generation != generation {
                    return ErrorCode::RebalanceInProgress as u16;
                }
            }
        }
        0
    }

    /// Commit offsets. Generation `0` skips membership checks (admin/CLI).
    ///
    /// Non-empty `member_id` with a matching generation is fenced with
    /// error 9 until that member has SyncGroup-confirmed
    /// (`synced_generation == generation`). Empty `member_id` keeps
    /// today's generation/member checks only (v0.219).
    pub fn commit_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        entries: &[(String, u32, u64, String)],
    ) -> Result<CommitResult> {
        self.commit_offsets_with_epoch(
            group_id,
            member_id,
            generation,
            entries
                .iter()
                .map(|(t, p, o, m)| (t.as_str(), *p, *o, m.as_str(), LEADER_EPOCH_UNKNOWN)),
        )
    }

    /// Commit offsets with Kafka `committed_leader_epoch` per entry.
    ///
    /// Native / admin callers without an epoch use [`Self::commit_offsets`]
    /// (writes `-1`). Generation `0` skips membership checks.
    pub fn commit_offsets_with_epoch<'a, I>(
        &self,
        group_id: &str,
        member_id: &str,
        generation: u32,
        entries: I,
    ) -> Result<CommitResult>
    where
        I: IntoIterator<Item = (&'a str, u32, u64, &'a str, i32)>,
    {
        if generation != 0 {
            let error_code = self.check_commit_fence(group_id, member_id, generation);
            if error_code != 0 {
                return Ok(CommitResult { error_code });
            }
        }
        for (topic, partition, offset, metadata, leader_epoch) in entries {
            self.offsets.commit_with_epoch(
                group_id,
                topic,
                partition,
                offset,
                metadata,
                leader_epoch,
            )?;
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
        let parked = parked_count(&self.join_waiters, group_id);
        Some(GroupDescription {
            group_id: group_id.to_owned(),
            generation: group.generation,
            members,
            state: listed_state(group, parked),
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
        let waiters = self.join_waiters.lock();
        let mut out: Vec<GroupListEntry> = set
            .into_keys()
            .map(|group_id| {
                let parked = waiters.get(&group_id).copied().unwrap_or(0);
                if let Some(g) = groups.get(&group_id) {
                    GroupListEntry {
                        group_id,
                        state: listed_state(g, parked),
                        member_count: g.members.len() as u32,
                        generation: g.generation,
                    }
                } else {
                    GroupListEntry {
                        group_id,
                        state: if parked > 0 {
                            GroupState::PreparingRebalance
                        } else {
                            GroupState::Empty
                        },
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
            let (offset, metadata, leader_epoch) =
                self.offsets.fetch(group_id, topic, *partition)?;
            out.push(StoredOffset {
                topic: topic.clone(),
                partition: *partition,
                offset,
                metadata,
                leader_epoch,
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
        let mut bumped = false;
        for (gid, group) in groups.iter_mut() {
            let before = group.members.len();
            group.members.retain(|_, m| {
                m.last_heartbeat.elapsed() <= Duration::from_millis(u64::from(m.session_timeout_ms))
            });
            if group.members.len() != before {
                group.generation = group.generation.wrapping_add(1);
                reassign(group, &partition_counts);
                bumped = true;
            }
            if group.members.is_empty() {
                empty.push(gid.clone());
            }
        }
        for gid in empty {
            groups.remove(&gid);
        }
        if bumped {
            self.join_park.notify_all();
        }
    }

    /// Drop dead members only. Do not bump generation or clear assignments.
    /// A subsequent Join may bump+reassign to heal; public [`Self::expire_sessions`]
    /// still bumps and reassigns (survivors become unsynced).
    fn expire_sessions_inner(&self) {
        let mut groups = self.groups.lock();
        let mut dropped = false;
        for group in groups.values_mut() {
            let before = group.members.len();
            group.members.retain(|_, m| {
                m.last_heartbeat.elapsed() <= Duration::from_millis(u64::from(m.session_timeout_ms))
            });
            if group.members.len() != before {
                dropped = true;
            }
        }
        groups.retain(|_, g| !g.members.is_empty());
        if dropped {
            self.join_park.notify_all();
        }
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

    /// Whether `member_id` currently owns `topic`/`partition` in `group_id`.
    ///
    /// Unknown group is [`Owns::UnknownMember`]. Empty `group_id` or
    /// `member_id` is the caller's skip (admin / CLI / old clients) — this
    /// method still reports ownership if invoked.
    pub fn member_owns(
        &self,
        group_id: &str,
        member_id: &str,
        topic: &str,
        partition: u32,
    ) -> Owns {
        let groups = self.groups.lock();
        let Some(group) = groups.get(group_id) else {
            return Owns::UnknownMember;
        };
        let Some(member) = group.members.get(member_id) else {
            return Owns::UnknownMember;
        };
        if member
            .assignment
            .iter()
            .any(|(t, p)| t == topic && *p == partition)
        {
            Owns::Allow
        } else {
            Owns::NotAssigned
        }
    }
}

/// Decode native SyncGroup `assignment_bytes` as an `Assignment` list.
///
/// Wire: `u32 LE count` then `{ u16 LE topic, u32 LE partition }*`. Empty
/// or leftover/truncated bytes return `None` (caller keeps the Join peek).
/// A well-formed empty list (`count = 0`, exactly 4 bytes) is `Some([])`.
pub(crate) fn decode_native_assignment_list(data: &[u8]) -> Option<Vec<(String, u32)>> {
    if data.is_empty() {
        return None;
    }
    if data.len() < 4 {
        return None;
    }
    let n = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
    let rest = data.len() - 4;
    // Each item is at least 2 (topic len) + 4 (partition) = 6 bytes.
    if n > rest / 6 {
        return None;
    }
    let mut i = 4;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if i + 2 > data.len() {
            return None;
        }
        let tlen = u16::from_le_bytes(data[i..i + 2].try_into().ok()?) as usize;
        i += 2;
        if i + tlen + 4 > data.len() {
            return None;
        }
        let topic = std::str::from_utf8(&data[i..i + tlen]).ok()?.to_owned();
        i += tlen;
        let partition = u32::from_le_bytes(data[i..i + 4].try_into().ok()?);
        i += 4;
        out.push((topic, partition));
    }
    if i != data.len() {
        return None;
    }
    Some(out)
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

/// Park until `all_synced` or `timeout_ms`. Releases `groups` while waiting.
/// Missing / empty group is treated as synced (first Join).
/// Increments the per-group parked waiter count while waiting (v0.230).
fn park_until_all_synced(
    park: &Condvar,
    waiters: &Mutex<HashMap<String, u32>>,
    groups: &mut parking_lot::MutexGuard<'_, HashMap<String, Group>>,
    group_id: &str,
    timeout_ms: u32,
) -> bool {
    if groups.get(group_id).map(all_synced).unwrap_or(true) {
        return true;
    }
    inc_join_waiters(waiters, group_id);
    let deadline = Instant::now() + Duration::from_millis(u64::from(timeout_ms));
    let synced = loop {
        if groups.get(group_id).map(all_synced).unwrap_or(true) {
            break true;
        }
        let now = Instant::now();
        if now >= deadline {
            break false;
        }
        if park
            .wait_for(groups, deadline.saturating_duration_since(now))
            .timed_out()
        {
            break groups.get(group_id).map(all_synced).unwrap_or(true);
        }
    };
    dec_join_waiters(waiters, group_id);
    synced
}

fn inc_join_waiters(waiters: &Mutex<HashMap<String, u32>>, group_id: &str) {
    let mut map = waiters.lock();
    *map.entry(group_id.to_owned()).or_insert(0) += 1;
}

fn dec_join_waiters(waiters: &Mutex<HashMap<String, u32>>, group_id: &str) {
    let mut map = waiters.lock();
    match map.get_mut(group_id) {
        Some(n) if *n > 1 => *n -= 1,
        Some(_) => {
            map.remove(group_id);
        }
        None => {}
    }
}

fn parked_count(waiters: &Mutex<HashMap<String, u32>>, group_id: &str) -> u32 {
    waiters.lock().get(group_id).copied().unwrap_or(0)
}

/// List/describe label: PreparingRebalance while Join waiters exist.
fn listed_state(group: &Group, parked: u32) -> GroupState {
    if parked > 0 {
        GroupState::PreparingRebalance
    } else if group.members.is_empty() {
        GroupState::Empty
    } else if all_synced(group) {
        GroupState::Stable
    } else {
        GroupState::CompletingRebalance
    }
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

fn fenced_join_lookup(
    groups: &HashMap<String, Group>,
    group_id: &str,
    member_id: String,
) -> JoinResult {
    match groups.get(group_id) {
        Some(group) => fenced_join(group, member_id),
        None => JoinResult {
            error_code: ErrorCode::RebalanceInProgress as u16,
            generation: 0,
            member_id,
            assignment: Vec::new(),
            revoked: Vec::new(),
            members: Vec::new(),
        },
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
    use std::sync::Arc;
    use std::thread;
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            assert_eq!(r.entries[0].leader_epoch, LEADER_EPOCH_UNKNOWN);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_with_epoch_persists() {
        let dir = temp_dir();
        {
            let coord = GroupCoordinator::new(&dir).unwrap();
            let r = coord
                .commit_offsets_with_epoch("g", "", 0, [("events", 0, 7u64, "x", 3i32)])
                .unwrap();
            assert_eq!(r.error_code, 0);
        }
        {
            let coord = GroupCoordinator::new(&dir).unwrap();
            let r = coord.fetch_offsets("g", &[("events".into(), 0)]).unwrap();
            assert_eq!(r.entries.len(), 1);
            assert_eq!(r.entries[0].offset, 7);
            assert_eq!(r.entries[0].leader_epoch, 3);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn heartbeat_generation_mismatch() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts_multi)
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
                0,
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.assignment.len(), 4);
        let first: std::collections::HashSet<_> = j1.assignment.iter().cloned().collect();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);

        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert!(j2.revoked.is_empty());

        // Member 1 re-syncs: should see revoked = old − new.
        let j1b = coord
            .join("g", &j1.member_id, 10_000, 0, vec!["t".into()], "", counts)
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

    fn fetch_offset(coord: &GroupCoordinator, group: &str, topic: &str, partition: u32) -> u64 {
        coord
            .fetch_offsets(group, &[(topic.into(), partition)])
            .unwrap()
            .entries[0]
            .offset
    }

    #[test]
    fn second_join_without_sync_is_fenced() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.error_code, 0);
        assert_eq!(coord.generation("g"), Some(1));
        assert_eq!(member_count(&coord, "g"), 1);

        let j2 = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j2.error_code, 0);

        let blocked = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), Some(2));
        assert_eq!(member_count(&coord, "g"), 2);

        sync_ok(&coord, "g", &j1.member_id, j2.generation);
        let still = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(still.error_code, ErrorCode::RebalanceInProgress as u16);

        sync_ok(&coord, "g", &j2.member_id, j2.generation);
        let j3 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let gen = coord.generation("g");
        let again = coord
            .join("g", &j1.member_id, 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(again.error_code, 0);
        assert_eq!(coord.generation("g"), gen);
        assert_eq!(member_count(&coord, "g"), 1);

        let blocked = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let j2 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &j1.member_id, j2.generation);
        sync_ok(&coord, "g", &j2.member_id, j2.generation);

        let leave = coord.leave("g", &j2.member_id, counts);
        assert_eq!(leave.error_code, 0);
        let after_leave = coord.generation("g");
        assert_eq!(after_leave, Some(3));
        assert_eq!(member_count(&coord, "g"), 1);

        let blocked = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), after_leave);
        assert_eq!(member_count(&coord, "g"), 1);

        sync_ok(&coord, "g", &j1.member_id, after_leave.unwrap());
        let j3 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
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

    fn encode_native_assignment(parts: &[(String, u32)]) -> Vec<u8> {
        let mut dst = Vec::new();
        dst.extend_from_slice(&(parts.len() as u32).to_le_bytes());
        for (topic, p) in parts {
            let b = topic.as_bytes();
            dst.extend_from_slice(&(b.len() as u16).to_le_bytes());
            dst.extend_from_slice(b);
            dst.extend_from_slice(&p.to_le_bytes());
        }
        dst
    }

    #[test]
    fn sync_group_empty_apply_keeps_join_assignment() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let r = coord.sync_group("g", &j.member_id, j.generation);
        assert_eq!(r.error_code, 0);
        assert_eq!(r.assignment, j.assignment);
        assert_eq!(
            coord.assignment("g", &j.member_id).as_ref(),
            Some(&j.assignment)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_group_with_assignments_sets_member_partitions() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j.assignment.len(), 4);
        let want = vec![("t".into(), 1u32), ("t".into(), 3)];
        let r = coord.sync_group_with_assignments(
            "g",
            &j.member_id,
            j.generation,
            &[(j.member_id.clone(), want.clone())],
        );
        assert_eq!(r.error_code, 0);
        assert_eq!(r.assignment, want);
        assert_eq!(coord.assignment("g", &j.member_id).as_ref(), Some(&want));
        assert_eq!(coord.member_owns("g", &j.member_id, "t", 1), Owns::Allow);
        assert_eq!(
            coord.member_owns("g", &j.member_id, "t", 0),
            Owns::NotAssigned
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_group_with_assignments_skips_unknown_member() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let applied = vec![("t".into(), 2u32)];
        let r = coord.sync_group_with_assignments(
            "g",
            &j.member_id,
            j.generation,
            &[
                ("nobody".into(), vec![("t".into(), 0)]),
                (j.member_id.clone(), applied.clone()),
            ],
        );
        assert_eq!(r.error_code, 0);
        assert_eq!(r.assignment, applied);
        assert!(coord.assignment("g", "nobody").is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn decode_native_assignment_list_empty_and_garbage() {
        assert_eq!(decode_native_assignment_list(&[]), None);
        assert_eq!(decode_native_assignment_list(&[0xff, 0x00, b'x']), None);
        assert_eq!(
            decode_native_assignment_list(&encode_native_assignment(&[])),
            Some(vec![])
        );
        let parts = vec![("events".into(), 0u32), ("events".into(), 2)];
        assert_eq!(
            decode_native_assignment_list(&encode_native_assignment(&parts)),
            Some(parts)
        );
        let mut leftover = encode_native_assignment(&[("t".into(), 1)]);
        leftover.push(0xff);
        assert_eq!(decode_native_assignment_list(&leftover), None);
    }

    #[test]
    fn list_groups_completing_then_stable_empty_supported_apis_stays_49() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        assert!(coord.list_groups().is_empty());

        coord
            .commit_offsets("g-empty", "", 0, &[("t".into(), 0, 1, String::new())])
            .unwrap();
        let empty = coord
            .list_groups()
            .into_iter()
            .find(|e| e.group_id == "g-empty")
            .unwrap();
        assert_eq!(empty.state, GroupState::Empty);
        assert_eq!(empty.member_count, 0);

        let j1 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let listed = coord.list_groups();
        let live = listed.iter().find(|e| e.group_id == "g").unwrap();
        assert_eq!(live.state, GroupState::CompletingRebalance);
        assert_eq!(live.member_count, 1);
        assert_eq!(live.generation, j1.generation);
        assert_eq!(
            coord.describe_group("g").unwrap().state,
            GroupState::CompletingRebalance
        );

        sync_ok(&coord, "g", &j1.member_id, j1.generation);
        let after_sync = coord
            .list_groups()
            .into_iter()
            .find(|e| e.group_id == "g")
            .unwrap();
        assert_eq!(after_sync.state, GroupState::Stable);
        assert_eq!(coord.describe_group("g").unwrap().state, GroupState::Stable);

        let leave = coord.leave("g", &j1.member_id, counts);
        assert_eq!(leave.error_code, 0);
        let after = coord.list_groups();
        assert!(after
            .iter()
            .all(|e| e.state == GroupState::Empty
                || e.member_count > 0 && e.state != GroupState::Empty));
        assert_eq!(crate::kafka::SUPPORTED_APIS.len(), 69);
        let _ = fs::remove_dir_all(&dir);
    }

    fn listed(coord: &GroupCoordinator, group: &str) -> GroupListEntry {
        coord
            .list_groups()
            .into_iter()
            .find(|e| e.group_id == group)
            .unwrap_or_else(|| panic!("missing listed group {group}"))
    }

    #[test]
    fn first_join_without_second_is_completing_rebalance() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j1 = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(j1.error_code, 0);
        let live = listed(&coord, "g");
        assert_eq!(live.state, GroupState::CompletingRebalance);
        assert_eq!(live.member_count, 1);
        assert_eq!(
            coord.describe_group("g").unwrap().state,
            GroupState::CompletingRebalance
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_describe_preparing_while_join_parked() {
        let dir = temp_dir();
        let coord = Arc::new(GroupCoordinator::new(&dir).unwrap());
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(a.error_code, 0);
        assert_eq!(listed(&coord, "g").state, GroupState::CompletingRebalance);

        let coord_b = Arc::clone(&coord);
        let handle = thread::spawn(move || {
            coord_b.join("g", "", 10_000, 5_000, vec!["t".into()], "", counts)
        });

        let deadline = Instant::now() + Duration::from_millis(100);
        loop {
            let live = listed(&coord, "g");
            if live.state == GroupState::PreparingRebalance {
                assert_eq!(live.member_count, 1);
                assert_eq!(
                    coord.describe_group("g").unwrap().state,
                    GroupState::PreparingRebalance
                );
                assert_eq!(member_count(&coord, "g"), 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "expected PreparingRebalance while B is parked, got {:?}",
                live.state
            );
            thread::sleep(Duration::from_millis(5));
        }

        sync_ok(&coord, "g", &a.member_id, a.generation);
        let b = handle.join().expect("join thread").unwrap();
        assert_eq!(b.error_code, 0);
        assert_eq!(member_count(&coord, "g"), 2);
        let after_b = listed(&coord, "g");
        assert_eq!(after_b.state, GroupState::CompletingRebalance);
        assert_eq!(after_b.member_count, 2);
        assert_eq!(
            coord.describe_group("g").unwrap().state,
            GroupState::CompletingRebalance
        );

        sync_ok(&coord, "g", &a.member_id, b.generation);
        sync_ok(&coord, "g", &b.member_id, b.generation);
        assert_eq!(listed(&coord, "g").state, GroupState::Stable);
        assert_eq!(coord.describe_group("g").unwrap().state, GroupState::Stable);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_member_before_sync_is_9() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let r = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation,
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(r.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), OFFSET_UNKNOWN);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_member_after_sync_is_0() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let blocked = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation,
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        sync_ok(&coord, "g", &j.member_id, j.generation);
        let r = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation,
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(r.error_code, 0);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_admin_gen0_works_during_fence() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let blocked = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation,
                &[("t".into(), 0, 9, String::new())],
            )
            .unwrap();
        assert_eq!(blocked.error_code, ErrorCode::RebalanceInProgress as u16);
        let admin = coord
            .commit_offsets("g", "", 0, &[("t".into(), 0, 9, "admin".into())])
            .unwrap();
        assert_eq!(admin.error_code, 0);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), 9);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_empty_member_nonzero_gen_skips_sync_fence() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        // Empty member_id: matching gen commits without SyncGroup confirm.
        let ok = coord
            .commit_offsets("g", "", j.generation, &[("t".into(), 0, 3, String::new())])
            .unwrap();
        assert_eq!(ok.error_code, 0);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), 3);
        // Today's checks only: wrong gen is still 11; unknown group is 10.
        let wrong = coord
            .commit_offsets(
                "g",
                "",
                j.generation.wrapping_add(1),
                &[("t".into(), 0, 4, String::new())],
            )
            .unwrap();
        assert_eq!(wrong.error_code, ErrorCode::IllegalGeneration as u16);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), 3);
        let unknown = coord
            .commit_offsets(
                "ghost",
                "",
                j.generation,
                &[("t".into(), 0, 4, String::new())],
            )
            .unwrap();
        assert_eq!(unknown.error_code, ErrorCode::UnknownMemberId as u16);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn offset_commit_wrong_gen_is_11_unknown_member_is_10() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let j = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let wrong = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation.wrapping_add(1),
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(wrong.error_code, ErrorCode::IllegalGeneration as u16);
        let unknown = coord
            .commit_offsets(
                "g",
                "nobody",
                j.generation,
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(unknown.error_code, ErrorCode::UnknownMemberId as u16);
        assert_eq!(fetch_offset(&coord, "g", "t", 0), OFFSET_UNKNOWN);
        sync_ok(&coord, "g", &j.member_id, j.generation);
        let still_wrong = coord
            .commit_offsets(
                "g",
                &j.member_id,
                j.generation.wrapping_add(1),
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(still_wrong.error_code, ErrorCode::IllegalGeneration as u16);
        let still_unknown = coord
            .commit_offsets(
                "g",
                "nobody",
                j.generation,
                &[("t".into(), 0, 5, String::new())],
            )
            .unwrap();
        assert_eq!(still_unknown.error_code, ErrorCode::UnknownMemberId as u16);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expire_inner_does_not_stick_new_join() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let a = coord
            .join("g", "", 40, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &a.member_id, a.generation);
        let b = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &a.member_id, b.generation);
        sync_ok(&coord, "g", &b.member_id, b.generation);
        std::thread::sleep(std::time::Duration::from_millis(80));
        // A's session is dead. Inner expire is drop-dead-only so B stays
        // synced and C may join (heal via bump+reassign).
        let c = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(c.error_code, 0);
        assert_eq!(member_count(&coord, "g"), 2);
        assert!(coord.assignment("g", &a.member_id).is_none());
        assert!(coord.assignment("g", &b.member_id).is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parked_join_succeeds_after_sync() {
        let dir = temp_dir();
        let coord = Arc::new(GroupCoordinator::new(&dir).unwrap());
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(a.error_code, 0);
        assert_eq!(member_count(&coord, "g"), 1);

        let coord_b = Arc::clone(&coord);
        let handle = thread::spawn(move || {
            coord_b.join("g", "", 10_000, 5_000, vec!["t".into()], "", counts)
        });
        thread::sleep(Duration::from_millis(50));
        assert_eq!(member_count(&coord, "g"), 1);

        sync_ok(&coord, "g", &a.member_id, a.generation);
        let b = handle.join().expect("join thread").unwrap();
        assert_eq!(b.error_code, 0);
        assert_eq!(coord.generation("g"), Some(2));
        assert_eq!(member_count(&coord, "g"), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn heartbeat_during_parked_join_is_not_deadlocked() {
        let dir = temp_dir();
        let coord = Arc::new(GroupCoordinator::new(&dir).unwrap());
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(a.error_code, 0);

        let coord_b = Arc::clone(&coord);
        let handle = thread::spawn(move || {
            coord_b.join("g", "", 10_000, 5_000, vec!["t".into()], "", counts)
        });
        thread::sleep(Duration::from_millis(50));
        assert_eq!(member_count(&coord, "g"), 1);

        let hb = coord.heartbeat("g", &a.member_id, a.generation);
        assert_eq!(hb.error_code, 0);

        sync_ok(&coord, "g", &a.member_id, a.generation);
        let b = handle.join().expect("join thread").unwrap();
        assert_eq!(b.error_code, 0);
        assert_eq!(coord.generation("g"), Some(2));
        assert_eq!(member_count(&coord, "g"), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn parked_join_timeout_still_returns_9() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(a.error_code, 0);
        assert_eq!(coord.generation("g"), Some(1));

        let b = coord
            .join("g", "", 10_000, 150, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(b.error_code, ErrorCode::RebalanceInProgress as u16);
        assert_eq!(coord.generation("g"), Some(1));
        assert_eq!(member_count(&coord, "g"), 1);
        // Park used rebalance=150, not session. First member stays live.
        let hb = coord.heartbeat("g", &a.member_id, a.generation);
        assert_eq!(hb.error_code, 0);
        assert_eq!(
            coord.assignment("g", &a.member_id).as_ref(),
            Some(&a.assignment)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn zero_rebalance_parks_at_most_default_1s() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        assert_eq!(a.error_code, 0);

        let start = Instant::now();
        let b = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(b.error_code, ErrorCode::RebalanceInProgress as u16);
        assert!(
            elapsed >= Duration::from_millis(700),
            "park too short: {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(2500),
            "park used session 10s, not default 1s: {elapsed:?}"
        );
        assert_eq!(coord.generation("g"), Some(1));
        assert_eq!(member_count(&coord, "g"), 1);
        let hb = coord.heartbeat("g", &a.member_id, a.generation);
        assert_eq!(hb.error_code, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn member_owns_allow_unknown_and_stolen() {
        let dir = temp_dir();
        let coord = GroupCoordinator::new(&dir).unwrap();
        assert_eq!(
            coord.member_owns("g", "nobody", "t", 0),
            Owns::UnknownMember
        );
        let a = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        sync_ok(&coord, "g", &a.member_id, a.generation);
        assert_eq!(coord.member_owns("g", &a.member_id, "t", 0), Owns::Allow);
        assert_eq!(coord.member_owns("g", "ghost", "t", 0), Owns::UnknownMember);
        let b = coord
            .join("g", "", 10_000, 0, vec!["t".into()], "", counts)
            .unwrap();
        let a_now = coord.assignment("g", &a.member_id).unwrap();
        let b_now = coord.assignment("g", &b.member_id).unwrap();
        let stolen = b_now
            .iter()
            .find(|tp| !a_now.contains(tp))
            .expect("b owns something a does not");
        assert_eq!(
            coord.member_owns("g", &a.member_id, &stolen.0, stolen.1),
            Owns::NotAssigned
        );
        assert_eq!(
            coord.member_owns("g", &b.member_id, &stolen.0, stolen.1),
            Owns::Allow
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
