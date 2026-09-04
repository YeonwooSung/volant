//! Group-coordinated consumer (`GroupConsumer`).
//!
//! After a successful [`GroupConsumer::join`] / [`GroupConsumer::join_static`],
//! a background task heartbeats every [`heartbeat_interval`] so a silent
//! consumer does not expire. Disable with
//! [`GroupConsumer::join_with_heartbeat`] / [`GroupConsumer::join_static_with_heartbeat`].
//! [`GroupConsumer::poll`] still heartbeats once at the start of the call.
//!
//! Offset commit is explicit by default. Opt in with
//! [`GroupConsumer::join_with_auto_commit`]: after a successful `poll` that
//! returned records, commit immediately (interval zero) or on the first such
//! poll and then every `auto_commit_interval`. This is **not** Kafka
//! `enable.auto.commit` — there is no background commit timer.
//!
//! After join / rebalance, OffsetFetch seeds each newly assigned partition.
//! A committed offset that is not `OFFSET_UNKNOWN` is used as-is. Otherwise
//! [`GroupConsumer::join_with_auto_offset_reset`] applies `earliest` (default:
//! native ListOffsets earliest), `latest` (native ListOffsets LEO), or
//! `none` (error). If ListOffsets fails or a wanted partition is missing,
//! join returns `Err` (no silent 0). Invalid reset strings fail before
//! JoinGroup. This is **not** Kafka `auto.offset.reset` (no timestamp /
//! isolation selector).
//!
//! Fetch-set assignment is the broker JoinGroup result by default,
//! confirmed by a best-effort [`Client::sync_group`] peek after join /
//! rejoin (v0.208; empty or error keeps JoinGroup). Opt in with
//! [`GroupConsumer::join_with_assignor`] (`"range"`): after that peek,
//! live member ids from the JoinGroup trailer (v0.211) + `metadata()`
//! feed `range_assign_multi`. Empty / missing trailer falls back to
//! DescribeGroup. Empty / `"broker"` keep today's assignment. Invalid
//! assignor strings fail before JoinGroup. DescribeGroup errors fall
//! back to the peeked assignment (or solo-range over `[self]` when that
//! assignment is empty). SyncGroup is peek/confirm, not Kafka
//! CompletingRebalance.
//!
//! Poll Fetch size is 100 messages / 4 MiB by default. Opt in with
//! [`GroupConsumer::join_with_fetch_knobs`]. Zero clamps to those
//! defaults. [`GroupConsumer::poll`] still passes `max_wait_ms = 0` on
//! the Fetch RPC.
//!
//! The join lock serializes the heartbeat task against `poll` / `commit` /
//! `leave`. This is **not** a fully concurrent consumer — do not call `poll`
//! from two tasks. [`GroupConsumer::leave`] stops the task and sends
//! LeaveGroup. `Drop` only `abort()`s the task (no LeaveGroup); call
//! `leave().await` for a clean leave.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use volant_core::{Error, Offset, Result};
use volant_protocol::{FetchRecord, OffsetCommitEntry, OffsetEntry};

use crate::assignor::range_assign_multi;
use crate::client::Client;

/// Wire sentinel: unknown / not-committed offset (`docs/PHASE3_SPEC.md`).
const OFFSET_UNKNOWN: u64 = u64::MAX;

/// Fetch position when OffsetFetch is missing or [`OFFSET_UNKNOWN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutoOffsetReset {
    /// Native ListOffsets earliest (default).
    Earliest,
    /// Native ListOffsets latest (LEO).
    Latest,
    /// Error; do not start at 0.
    None,
}

impl AutoOffsetReset {
    fn as_str(self) -> &'static str {
        match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::None => "none",
        }
    }
}

fn parse_auto_offset_reset(name: &str) -> Result<AutoOffsetReset> {
    match name {
        "" | "earliest" => Ok(AutoOffsetReset::Earliest),
        "latest" => Ok(AutoOffsetReset::Latest),
        "none" => Ok(AutoOffsetReset::None),
        other => Err(Error::InvalidArgument(format!(
            "unknown auto_offset_reset: {other:?}"
        ))),
    }
}

/// Fetch-set assignor after a successful JoinGroup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Assignor {
    /// Honor the broker JoinGroup assignment (default; after SyncGroup peek).
    Broker,
    /// Local range over JoinGroup members trailer, else DescribeGroup.
    Range,
}

impl Assignor {
    fn as_str(self) -> &'static str {
        match self {
            Self::Broker => "broker",
            Self::Range => "range",
        }
    }
}

fn parse_assignor(name: &str) -> Result<Assignor> {
    match name {
        "" | "broker" => Ok(Assignor::Broker),
        "range" => Ok(Assignor::Range),
        other => Err(Error::InvalidArgument(format!(
            "unknown assignor: {other:?}"
        ))),
    }
}

/// Historical poll Fetch `max_messages` (not Client fetch's 128).
const POLL_MAX_MESSAGES: u32 = 100;
/// Historical poll Fetch `max_bytes` (same as [`Client::fetch`]).
const POLL_MAX_BYTES: u32 = 4 * 1024 * 1024;

fn clamp_fetch_max_messages(n: u32) -> u32 {
    if n == 0 {
        POLL_MAX_MESSAGES
    } else {
        n
    }
}

fn clamp_fetch_max_bytes(n: u32) -> u32 {
    if n == 0 {
        POLL_MAX_BYTES
    } else {
        n
    }
}

/// Minimum background heartbeat period (ms).
const HEARTBEAT_INTERVAL_MIN_MS: u32 = 100;
/// Maximum background heartbeat period (ms).
const HEARTBEAT_INTERVAL_MAX_MS: u32 = 3000;
/// How long [`GroupConsumer::leave`] waits for the heartbeat task to exit.
const HEARTBEAT_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// Background heartbeat period: `session_timeout_ms / 3`, clamped to
/// `[100ms, 3000ms]`.
pub fn heartbeat_interval(session_timeout_ms: u32) -> Duration {
    let third = session_timeout_ms / 3;
    let ms = third.clamp(HEARTBEAT_INTERVAL_MIN_MS, HEARTBEAT_INTERVAL_MAX_MS);
    Duration::from_millis(u64::from(ms))
}

#[derive(Debug)]
struct JoinState {
    member_id: String,
    generation: u32,
    assignment: Vec<(String, u32)>,
    last_revoked: Vec<(String, u32)>,
    positions: HashMap<(String, u32), u64>,
    /// Last successful auto or explicit commit (None = never).
    last_auto_commit: Option<Instant>,
    /// Positions advanced since the last successful commit.
    dirty: bool,
}

#[derive(Debug)]
struct Shared {
    /// Serializes heartbeat vs poll / commit / leave. Not a concurrent consumer.
    gate: AsyncMutex<()>,
    state: Mutex<JoinState>,
    heartbeat_count: AtomicU64,
    /// Offset reset when OffsetFetch is missing or `OFFSET_UNKNOWN`.
    auto_offset_reset: AutoOffsetReset,
    /// Fetch-set assignor; reused on heartbeat-driven rejoin.
    assignor: Assignor,
    /// Poll Fetch `max_messages`; reused after rejoin.
    fetch_max_messages: u32,
    /// Poll Fetch `max_bytes`; reused after rejoin.
    fetch_max_bytes: u32,
}

/// High-level consumer that joins a group, polls assigned partitions, and commits.
#[derive(Debug)]
pub struct GroupConsumer {
    client: Arc<Client>,
    group_id: String,
    topics: Vec<String>,
    session_timeout_ms: u32,
    /// Static membership instance id (Phase 12); empty = dynamic.
    group_instance_id: String,
    /// Opt-in auto-commit after a successful poll that returned records.
    auto_commit: bool,
    /// Zero = after every such poll; otherwise first poll then this interval.
    auto_commit_interval: Duration,
    shared: Arc<Shared>,
    hb_stop: Option<watch::Sender<bool>>,
    hb_task: Option<JoinHandle<()>>,
}

impl GroupConsumer {
    /// Join a consumer group on the given topics (background heartbeat on).
    pub async fn join(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
    ) -> Result<Self> {
        Self::join_static_with_heartbeat(client, group_id, topics, session_timeout_ms, "", true)
            .await
    }

    /// Join with static membership (`group_instance_id`, Phase 12).
    ///
    /// Empty `group_instance_id` is dynamic membership. Background heartbeat on.
    pub async fn join_static(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
    ) -> Result<Self> {
        Self::join_static_with_heartbeat(
            client,
            group_id,
            topics,
            session_timeout_ms,
            group_instance_id,
            true,
        )
        .await
    }

    /// [`join`](Self::join) with an explicit background-heartbeat switch.
    ///
    /// `heartbeat = false` keeps poll-only membership (no task).
    pub async fn join_with_heartbeat(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        heartbeat: bool,
    ) -> Result<Self> {
        Self::join_static_with_heartbeat(
            client,
            group_id,
            topics,
            session_timeout_ms,
            "",
            heartbeat,
        )
        .await
    }

    /// [`join_static`](Self::join_static) with an explicit background-heartbeat switch.
    pub async fn join_static_with_heartbeat(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
        heartbeat: bool,
    ) -> Result<Self> {
        Self::join_with_auto_commit(
            client,
            group_id,
            topics,
            session_timeout_ms,
            group_instance_id,
            heartbeat,
            false,
            Duration::ZERO,
        )
        .await
    }

    /// Join with opt-in auto-commit after a successful `poll` that returned
    /// records (v0.60).
    ///
    /// Existing [`join`](Self::join) / [`join_static`](Self::join_static) /
    /// [`join_with_heartbeat`](Self::join_with_heartbeat) /
    /// [`join_static_with_heartbeat`](Self::join_static_with_heartbeat)
    /// stay explicit-only. `auto_commit = false` is that same default.
    ///
    /// When `auto_commit` is on:
    /// - interval **zero**: commit after every successful poll that returned
    ///   records;
    /// - interval **> 0**: first such poll always commits, then when at least
    ///   `auto_commit_interval` has elapsed since the last auto or explicit
    ///   [`commit`](Self::commit).
    ///
    /// Empty polls never auto-commit. `leave` best-effort commits leftover
    /// dirty positions, then LeaveGroup. Not Kafka `enable.auto.commit`
    /// (no background commit timer independent of `poll`).
    pub async fn join_with_auto_commit(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
        heartbeat: bool,
        auto_commit: bool,
        auto_commit_interval: Duration,
    ) -> Result<Self> {
        Self::join_with_auto_offset_reset(
            client,
            group_id,
            topics,
            session_timeout_ms,
            group_instance_id,
            heartbeat,
            auto_commit,
            auto_commit_interval,
            "earliest",
        )
        .await
    }

    /// Join with opt-in `auto_offset_reset` (v0.67).
    ///
    /// Same arguments as [`join_with_auto_commit`](Self::join_with_auto_commit)
    /// plus `auto_offset_reset`: `"earliest"` (default; native ListOffsets
    /// earliest), `"latest"` (native ListOffsets LEO), or `"none"` (error
    /// if OffsetFetch is missing / `OFFSET_UNKNOWN`). If ListOffsets fails
    /// or a wanted partition is missing, join returns `Err` (no silent 0).
    /// Invalid strings return an `InvalidArgument` error **before** JoinGroup.
    /// Empty string is `"earliest"`.
    ///
    /// Existing [`join`](Self::join) / [`join_static`](Self::join_static) /
    /// [`join_with_heartbeat`](Self::join_with_heartbeat) /
    /// [`join_static_with_heartbeat`](Self::join_static_with_heartbeat) /
    /// [`join_with_auto_commit`](Self::join_with_auto_commit) keep
    /// `"earliest"`. Rejoin / heartbeat-driven rebalance reuse the same
    /// policy. Not Kafka `auto.offset.reset`. Broker JoinGroup assignment.
    pub async fn join_with_auto_offset_reset(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
        heartbeat: bool,
        auto_commit: bool,
        auto_commit_interval: Duration,
        auto_offset_reset: &str,
    ) -> Result<Self> {
        Self::join_with_assignor(
            client,
            group_id,
            topics,
            session_timeout_ms,
            group_instance_id,
            heartbeat,
            auto_commit,
            auto_commit_interval,
            auto_offset_reset,
            "broker",
        )
        .await
    }

    /// Join with an opt-in fetch-set assignor (v0.73).
    ///
    /// Same arguments as
    /// [`join_with_auto_offset_reset`](Self::join_with_auto_offset_reset)
    /// plus `assignor`: `"broker"` (default; honor JoinGroup) or `"range"`
    /// (JoinGroup member ids, else DescribeGroup, + `range_assign_multi`).
    /// Invalid strings return `InvalidArgument` **before** JoinGroup. Empty
    /// string is `"broker"`.
    ///
    /// Existing [`join`](Self::join) / [`join_static`](Self::join_static) /
    /// [`join_with_heartbeat`](Self::join_with_heartbeat) /
    /// [`join_static_with_heartbeat`](Self::join_static_with_heartbeat) /
    /// [`join_with_auto_commit`](Self::join_with_auto_commit) /
    /// [`join_with_auto_offset_reset`](Self::join_with_auto_offset_reset)
    /// keep `"broker"`. Rejoin / heartbeat-driven rebalance reuse the same
    /// policy. SyncGroup peek runs first (v0.208); range still uses
    /// DescribeGroup. Poll Fetch size stays 100 / 4 MiB.
    pub async fn join_with_assignor(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
        heartbeat: bool,
        auto_commit: bool,
        auto_commit_interval: Duration,
        auto_offset_reset: &str,
        assignor: &str,
    ) -> Result<Self> {
        Self::join_with_fetch_knobs(
            client,
            group_id,
            topics,
            session_timeout_ms,
            group_instance_id,
            heartbeat,
            auto_commit,
            auto_commit_interval,
            auto_offset_reset,
            assignor,
            POLL_MAX_MESSAGES,
            POLL_MAX_BYTES,
        )
        .await
    }

    /// Join with opt-in poll Fetch knobs (v0.76).
    ///
    /// Same arguments as [`join_with_assignor`](Self::join_with_assignor)
    /// plus `fetch_max_messages` / `fetch_max_bytes`. `0` clamps to the
    /// historical poll defaults (100 / 4 MiB). Existing
    /// [`join`](Self::join) / [`join_static`](Self::join_static) /
    /// [`join_with_heartbeat`](Self::join_with_heartbeat) /
    /// [`join_static_with_heartbeat`](Self::join_static_with_heartbeat) /
    /// [`join_with_auto_commit`](Self::join_with_auto_commit) /
    /// [`join_with_auto_offset_reset`](Self::join_with_auto_offset_reset) /
    /// [`join_with_assignor`](Self::join_with_assignor) keep those
    /// defaults. Rejoin reuses the same knobs. [`poll`](Self::poll)
    /// still passes `max_wait_ms = 0` on the Fetch RPC. Not Kafka
    /// `max.poll.records`.
    pub async fn join_with_fetch_knobs(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
        heartbeat: bool,
        auto_commit: bool,
        auto_commit_interval: Duration,
        auto_offset_reset: &str,
        assignor: &str,
        fetch_max_messages: u32,
        fetch_max_bytes: u32,
    ) -> Result<Self> {
        let assignor = parse_assignor(assignor)?;
        let auto_offset_reset = parse_auto_offset_reset(auto_offset_reset)?;
        let fetch_max_messages = clamp_fetch_max_messages(fetch_max_messages);
        let fetch_max_bytes = clamp_fetch_max_bytes(fetch_max_bytes);
        let group_id = group_id.into();
        let group_instance_id = group_instance_id.into();
        let timeout = if session_timeout_ms == 0 {
            10_000
        } else {
            session_timeout_ms
        };
        let this = Self {
            client,
            group_id,
            topics,
            session_timeout_ms: timeout,
            group_instance_id,
            auto_commit,
            auto_commit_interval,
            shared: Arc::new(Shared {
                gate: AsyncMutex::new(()),
                state: Mutex::new(JoinState {
                    member_id: String::new(),
                    generation: 0,
                    assignment: Vec::new(),
                    last_revoked: Vec::new(),
                    positions: HashMap::new(),
                    last_auto_commit: None,
                    dirty: false,
                }),
                heartbeat_count: AtomicU64::new(0),
                auto_offset_reset,
                assignor,
                fetch_max_messages,
                fetch_max_bytes,
            }),
            hb_stop: None,
            hb_task: None,
        };
        {
            let _gate = this.shared.gate.lock().await;
            do_join(
                &this.client,
                &this.group_id,
                &this.topics,
                this.session_timeout_ms,
                &this.group_instance_id,
                &this.shared,
            )
            .await?;
        }
        let mut this = this;
        if heartbeat {
            this.spawn_heartbeat();
        }
        Ok(this)
    }

    fn spawn_heartbeat(&mut self) {
        let (stop_tx, stop_rx) = watch::channel(false);
        let client = Arc::clone(&self.client);
        let group_id = self.group_id.clone();
        let topics = self.topics.clone();
        let session_timeout_ms = self.session_timeout_ms;
        let group_instance_id = self.group_instance_id.clone();
        let shared = Arc::clone(&self.shared);
        self.hb_stop = Some(stop_tx);
        self.hb_task = Some(tokio::spawn(async move {
            heartbeat_loop(
                client,
                group_id,
                topics,
                session_timeout_ms,
                group_instance_id,
                shared,
                stop_rx,
            )
            .await;
        }));
    }

    async fn shutdown_heartbeat(&mut self) {
        if let Some(tx) = self.hb_stop.take() {
            let _ = tx.send(true);
        }
        if let Some(mut handle) = self.hb_task.take() {
            tokio::select! {
                _ = &mut handle => {}
                _ = tokio::time::sleep(HEARTBEAT_SHUTDOWN_TIMEOUT) => {
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
    }

    /// Heartbeat + fetch from all assigned partitions.
    ///
    /// Do not call from two tasks; the lock only serializes this call against
    /// the background heartbeat.
    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        let _gate = self.shared.gate.lock().await;
        let (member_id, generation) = membership(&self.shared);
        self.shared.heartbeat_count.fetch_add(1, Ordering::Relaxed);
        let hb = self
            .client
            .heartbeat(&self.group_id, &member_id, generation)
            .await?;
        if hb.needs_rebalance() {
            do_join(
                &self.client,
                &self.group_id,
                &self.topics,
                self.session_timeout_ms,
                &self.group_instance_id,
                &self.shared,
            )
            .await?;
        }

        let assignment = lock_state(&self.shared).assignment.clone();
        let mut out = Vec::new();
        for (topic, partition) in assignment {
            let from = lock_state(&self.shared)
                .positions
                .get(&(topic.clone(), partition))
                .copied()
                .unwrap_or(0);
            let max_messages = clamp_fetch_max_messages(self.shared.fetch_max_messages);
            let max_bytes = clamp_fetch_max_bytes(self.shared.fetch_max_bytes);
            let result = self
                .client
                .fetch_opts(
                    &topic,
                    partition,
                    Offset::new(from),
                    max_messages,
                    0,
                    max_bytes,
                )
                .await?;
            for r in result.records {
                let next = r.offset.saturating_add(1);
                lock_state(&self.shared)
                    .positions
                    .insert((topic.clone(), partition), next);
                out.push(FetchedRecord {
                    topic: topic.clone(),
                    partition,
                    record: r,
                });
            }
        }
        if !out.is_empty() {
            lock_state(&self.shared).dirty = true;
            self.maybe_auto_commit().await?;
        }
        Ok(out)
    }

    /// Commit last+1 positions for all assigned partitions.
    ///
    /// Resets the auto-commit interval clock on success.
    pub async fn commit(&self) -> Result<()> {
        let _gate = self.shared.gate.lock().await;
        self.commit_unlocked().await
    }

    async fn commit_unlocked(&self) -> Result<()> {
        let (member_id, generation, entries) = {
            let state = lock_state(&self.shared);
            if state.positions.is_empty() {
                return Ok(());
            }
            let entries: Vec<OffsetCommitEntry> = state
                .positions
                .iter()
                .map(|((topic, partition), offset)| OffsetCommitEntry {
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *offset,
                    metadata: String::new(),
                })
                .collect();
            (state.member_id.clone(), state.generation, entries)
        };
        self.client
            .commit_offsets(&self.group_id, &member_id, generation, entries)
            .await?;
        let mut state = lock_state(&self.shared);
        state.last_auto_commit = Some(Instant::now());
        state.dirty = false;
        Ok(())
    }

    async fn maybe_auto_commit(&self) -> Result<()> {
        let last = lock_state(&self.shared).last_auto_commit;
        if !due_for_auto_commit(
            self.auto_commit,
            self.auto_commit_interval,
            last,
            Instant::now(),
        ) {
            return Ok(());
        }
        self.commit_unlocked().await
    }

    /// Stop the heartbeat task (if any) and leave the group (consumes self).
    ///
    /// Idempotent with `Drop`: after this returns the task is gone. Required
    /// for a clean LeaveGroup — `Drop` only aborts the task.
    ///
    /// Auto-commit on + dirty positions: best-effort commit once (error
    /// swallowed), then LeaveGroup.
    pub async fn leave(mut self) -> Result<()> {
        self.shutdown_heartbeat().await;
        let _gate = self.shared.gate.lock().await;
        if self.auto_commit && lock_state(&self.shared).dirty {
            let _ = self.commit_unlocked().await;
        }
        let member_id = lock_state(&self.shared).member_id.clone();
        self.client.leave_group(&self.group_id, &member_id).await
    }

    /// Current assignment as (topic, partition) pairs.
    pub fn assignment(&self) -> Vec<(String, u32)> {
        lock_state(&self.shared).assignment.clone()
    }

    /// Partitions revoked on the most recent join/rebalance (Phase 17).
    pub fn last_revoked(&self) -> Vec<(String, u32)> {
        lock_state(&self.shared).last_revoked.clone()
    }

    /// Group member id.
    pub fn member_id(&self) -> String {
        lock_state(&self.shared).member_id.clone()
    }

    /// Current generation.
    pub fn generation(&self) -> u32 {
        lock_state(&self.shared).generation
    }

    /// Group id.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Current next-read positions.
    pub fn positions(&self) -> HashMap<(String, u32), u64> {
        lock_state(&self.shared).positions.clone()
    }

    /// Current `auto_offset_reset` policy (`earliest`, `latest`, or `none`).
    pub fn auto_offset_reset(&self) -> &'static str {
        self.shared.auto_offset_reset.as_str()
    }

    /// Current fetch-set assignor (`broker` or `range`).
    pub fn assignor(&self) -> &'static str {
        self.shared.assignor.as_str()
    }

    /// Poll Fetch `max_messages` (default 100).
    pub fn fetch_max_messages(&self) -> u32 {
        self.shared.fetch_max_messages
    }

    /// Poll Fetch `max_bytes` (default 4 MiB).
    pub fn fetch_max_bytes(&self) -> u32 {
        self.shared.fetch_max_bytes
    }

    /// Heartbeat RPCs issued by this consumer (poll + background).
    pub fn heartbeat_count(&self) -> u64 {
        self.shared.heartbeat_count.load(Ordering::Relaxed)
    }
}

impl Drop for GroupConsumer {
    fn drop(&mut self) {
        if let Some(tx) = self.hb_stop.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = self.hb_task.take() {
            task.abort();
        }
    }
}

/// Whether an auto-commit should run after a successful poll that returned records.
fn due_for_auto_commit(
    auto_commit: bool,
    interval: Duration,
    last: Option<Instant>,
    now: Instant,
) -> bool {
    if !auto_commit {
        return false;
    }
    match last {
        None => true,
        Some(_) if interval.is_zero() => true,
        Some(t) => now.saturating_duration_since(t) >= interval,
    }
}

fn lock_state(shared: &Shared) -> std::sync::MutexGuard<'_, JoinState> {
    shared.state.lock().unwrap_or_else(|e| e.into_inner())
}

fn membership(shared: &Shared) -> (String, u32) {
    let state = lock_state(shared);
    (state.member_id.clone(), state.generation)
}

async fn do_join(
    client: &Client,
    group_id: &str,
    topics: &[String],
    session_timeout_ms: u32,
    group_instance_id: &str,
    shared: &Shared,
) -> Result<()> {
    let (member_id, previous) = {
        let state = lock_state(shared);
        (state.member_id.clone(), state.assignment.clone())
    };
    let result = client
        .join_group_with_instance(
            group_id,
            &member_id,
            session_timeout_ms,
            topics.to_vec(),
            group_instance_id,
        )
        .await?;
    let mut join_assignment: Vec<(String, u32)> = result
        .assignment
        .into_iter()
        .map(|a| (a.topic, a.partition))
        .collect();
    // Best-effort peek/confirm (v0.208). Non-empty replaces JoinGroup;
    // empty or Err keep Join. Does not increment heartbeat_count.
    if let Ok(peeked) = client
        .sync_group(group_id, &result.member_id, result.generation)
        .await
    {
        if !peeked.is_empty() {
            join_assignment = peeked.into_iter().map(|a| (a.topic, a.partition)).collect();
        }
    }
    let new_assignment = if shared.assignor == Assignor::Range {
        apply_range_override(
            client,
            group_id,
            topics,
            &result.member_id,
            &join_assignment,
            &result.members,
        )
        .await
    } else {
        join_assignment
    };

    let old_set: HashSet<(String, u32)> = previous.iter().cloned().collect();
    let new_set: HashSet<(String, u32)> = new_assignment.iter().cloned().collect();

    let mut revoked: Vec<(String, u32)> = old_set.difference(&new_set).cloned().collect();
    if shared.assignor != Assignor::Range {
        for a in result.revoked {
            let tp = (a.topic, a.partition);
            if !revoked.contains(&tp) {
                revoked.push(tp);
            }
        }
    }
    revoked.sort();

    let added: Vec<(String, u32)> = new_set.difference(&old_set).cloned().collect();
    let positions_empty = lock_state(shared).positions.is_empty();

    let to_fetch: Vec<(String, u32)> =
        if !added.is_empty() || positions_empty && !new_assignment.is_empty() {
            if previous.is_empty() {
                new_assignment.clone()
            } else {
                added
            }
        } else {
            Vec::new()
        };
    let fetched = if to_fetch.is_empty() {
        Vec::new()
    } else {
        let entries: Vec<OffsetEntry> = to_fetch
            .iter()
            .map(|(t, p)| OffsetEntry {
                topic: t.clone(),
                partition: *p,
            })
            .collect();
        client.fetch_offsets(group_id, entries).await?
    };

    let policy = shared.auto_offset_reset;
    let missing = {
        let mut state = lock_state(shared);
        for tp in &revoked {
            state.positions.remove(tp);
        }
        state.member_id = result.member_id;
        state.generation = result.generation;
        state.assignment = new_assignment;
        state.last_revoked = revoked;
        for e in fetched {
            if e.offset != OFFSET_UNKNOWN {
                state.positions.insert((e.topic, e.partition), e.offset);
            }
        }
        state
            .assignment
            .iter()
            .filter(|tp| !state.positions.contains_key(*tp))
            .cloned()
            .collect::<Vec<_>>()
    };

    let reset = apply_reset(client, policy, &missing).await?;
    if !reset.is_empty() {
        let mut state = lock_state(shared);
        for (tp, pos) in reset {
            state.positions.entry(tp).or_insert(pos);
        }
    }
    Ok(())
}

/// DescribeGroup members for local range, or `None` to fall back.
async fn range_members_from_describe(
    client: &Client,
    group_id: &str,
    self_id: &str,
    self_topics: &[String],
) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    let desc = client.describe_group(group_id).await.ok()?;
    let mut ids = Vec::new();
    let mut topics = Vec::new();
    let mut seen = false;
    for member in desc.members {
        if member.member_id == self_id {
            seen = true;
        }
        ids.push(member.member_id);
        topics.push(member.topics);
    }
    if !seen {
        ids.push(self_id.to_string());
        topics.push(self_topics.to_vec());
    }
    if ids.is_empty() || !ids.iter().any(|id| id == self_id) {
        return None;
    }
    Some((ids, topics))
}

fn partition_counts_from_metadata(meta: crate::client::Metadata) -> HashMap<String, u32> {
    meta.topics
        .into_iter()
        .map(|t| (t.name, t.partitions.len() as u32))
        .collect()
}

/// Member ids + per-member topics from the JoinGroup trailer.
/// `None` when the trailer is empty / missing (DescribeGroup fallback).
fn range_members_from_join(
    join_members: &[String],
    self_topics: &[String],
) -> Option<(Vec<String>, Vec<Vec<String>>)> {
    if join_members.is_empty() {
        return None;
    }
    let topics = vec![self_topics.to_vec(); join_members.len()];
    Some((join_members.to_vec(), topics))
}

/// Range fetch set after a successful JoinGroup (and SyncGroup peek).
/// Never fails the join: empty JoinGroup members fall back to
/// DescribeGroup; DescribeGroup / empty-members / missing-self /
/// metadata errors fall back to the peeked assignment, or solo-range
/// over `[self]` when that assignment is empty.
async fn apply_range_override(
    client: &Client,
    group_id: &str,
    self_topics: &[String],
    self_id: &str,
    join_assignment: &[(String, u32)],
    join_members: &[String],
) -> Vec<(String, u32)> {
    let from_join = range_members_from_join(join_members, self_topics);
    let (ids, member_topics) = if let Some(pair) = from_join {
        pair
    } else {
        let described = range_members_from_describe(client, group_id, self_id, self_topics).await;
        match described {
            Some(pair) => pair,
            None if join_assignment.is_empty() => {
                (vec![self_id.to_string()], vec![self_topics.to_vec()])
            }
            None => return join_assignment.to_vec(),
        }
    };

    let counts = match client.metadata().await {
        Ok(meta) => partition_counts_from_metadata(meta),
        Err(_) if join_assignment.is_empty() => return Vec::new(),
        Err(_) => return join_assignment.to_vec(),
    };

    let assigned = range_assign_multi(&ids, &member_topics, &counts);
    match ids.iter().position(|id| id == self_id) {
        Some(idx) => assigned.get(idx).cloned().unwrap_or_default(),
        None if join_assignment.is_empty() => {
            range_assign_multi(&[self_id.to_string()], &[self_topics.to_vec()], &counts)
                .into_iter()
                .next()
                .unwrap_or_default()
        }
        None => join_assignment.to_vec(),
    }
}

/// Seed positions for OffsetFetch miss / `OFFSET_UNKNOWN`.
async fn apply_reset(
    client: &Client,
    policy: AutoOffsetReset,
    partitions: &[(String, u32)],
) -> Result<Vec<((String, u32), u64)>> {
    if partitions.is_empty() {
        return Ok(Vec::new());
    }
    match policy {
        AutoOffsetReset::None => {
            let (topic, partition) = &partitions[0];
            Err(Error::InvalidArgument(format!(
                "no committed offset for {topic}-{partition} and auto_offset_reset=\"none\""
            )))
        }
        AutoOffsetReset::Earliest | AutoOffsetReset::Latest => {
            let use_earliest = matches!(policy, AutoOffsetReset::Earliest);
            let mut by_topic: HashMap<String, Vec<u32>> = HashMap::new();
            for (topic, partition) in partitions {
                by_topic.entry(topic.clone()).or_default().push(*partition);
            }
            let mut out = Vec::with_capacity(partitions.len());
            for (topic, parts) in by_topic {
                let listing = client.list_offsets(&topic, parts.clone()).await?;
                let got: HashMap<u32, u64> = listing
                    .entries
                    .into_iter()
                    .map(|e| {
                        (
                            e.partition,
                            if use_earliest { e.earliest } else { e.latest },
                        )
                    })
                    .collect();
                for partition in parts {
                    let pos = got.get(&partition).copied().ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "list_offsets missing partition {topic}-{partition}"
                        ))
                    })?;
                    out.push(((topic.clone(), partition), pos));
                }
            }
            Ok(out)
        }
    }
}

async fn heartbeat_loop(
    client: Arc<Client>,
    group_id: String,
    topics: Vec<String>,
    session_timeout_ms: u32,
    group_instance_id: String,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
) {
    let interval = heartbeat_interval(session_timeout_ms);
    loop {
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(interval) => {
                if *stop.borrow() {
                    break;
                }
                let _gate = shared.gate.lock().await;
                if *stop.borrow() {
                    break;
                }
                let (member_id, generation) = membership(&shared);
                shared.heartbeat_count.fetch_add(1, Ordering::Relaxed);
                match client.heartbeat(&group_id, &member_id, generation).await {
                    Ok(hb) if hb.needs_rebalance() => {
                        let _ = do_join(
                            &client,
                            &group_id,
                            &topics,
                            session_timeout_ms,
                            &group_instance_id,
                            &shared,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        }
    }
}

/// A record fetched by [`GroupConsumer::poll`] with topic/partition context.
#[derive(Debug, Clone)]
pub struct FetchedRecord {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: u32,
    /// Wire record.
    pub record: FetchRecord,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_interval_clamps() {
        assert_eq!(heartbeat_interval(0), Duration::from_millis(100));
        assert_eq!(heartbeat_interval(150), Duration::from_millis(100));
        assert_eq!(heartbeat_interval(900), Duration::from_millis(300));
        assert_eq!(heartbeat_interval(10_000), Duration::from_millis(3000));
    }

    #[test]
    fn auto_commit_due_default_off() {
        let now = Instant::now();
        assert!(!due_for_auto_commit(false, Duration::ZERO, None, now));
        assert!(!due_for_auto_commit(
            false,
            Duration::from_secs(10),
            None,
            now
        ));
    }

    #[test]
    fn auto_commit_due_interval_zero_always() {
        let now = Instant::now();
        assert!(due_for_auto_commit(true, Duration::ZERO, None, now));
        assert!(due_for_auto_commit(true, Duration::ZERO, Some(now), now));
    }

    #[test]
    fn auto_commit_due_first_poll_then_interval() {
        let t0 = Instant::now();
        let interval = Duration::from_secs(10);
        assert!(due_for_auto_commit(true, interval, None, t0));
        assert!(!due_for_auto_commit(
            true,
            interval,
            Some(t0),
            t0 + Duration::from_millis(5)
        ));
        assert!(due_for_auto_commit(true, interval, Some(t0), t0 + interval));
    }

    #[test]
    fn auto_offset_reset_parses() {
        assert_eq!(
            parse_auto_offset_reset("earliest").unwrap(),
            AutoOffsetReset::Earliest
        );
        assert_eq!(
            parse_auto_offset_reset("").unwrap(),
            AutoOffsetReset::Earliest
        );
        assert_eq!(
            parse_auto_offset_reset("latest").unwrap(),
            AutoOffsetReset::Latest
        );
        assert_eq!(
            parse_auto_offset_reset("none").unwrap(),
            AutoOffsetReset::None
        );
        let err = parse_auto_offset_reset("banana").unwrap_err();
        assert!(err.to_string().contains("unknown auto_offset_reset"));
        assert!(err.to_string().contains("banana"));
    }

    #[test]
    fn assignor_parses() {
        assert_eq!(parse_assignor("broker").unwrap(), Assignor::Broker);
        assert_eq!(parse_assignor("").unwrap(), Assignor::Broker);
        assert_eq!(parse_assignor("range").unwrap(), Assignor::Range);
        let err = parse_assignor("banana").unwrap_err();
        assert!(err.to_string().contains("unknown assignor"));
        assert!(err.to_string().contains("banana"));
    }

    #[test]
    fn fetch_knobs_clamp_zero_to_defaults() {
        assert_eq!(clamp_fetch_max_messages(0), POLL_MAX_MESSAGES);
        assert_eq!(clamp_fetch_max_bytes(0), POLL_MAX_BYTES);
        assert_eq!(clamp_fetch_max_messages(10), 10);
        assert_eq!(clamp_fetch_max_bytes(4096), 4096);
    }

    #[test]
    fn range_members_from_join_empty_falls_back() {
        assert!(range_members_from_join(&[], &["t".into()]).is_none());
    }

    #[test]
    fn range_members_from_join_uses_trailer_ids() {
        let (ids, topics) =
            range_members_from_join(&["m-a".into(), "m-b".into()], &["t".into()]).expect("members");
        assert_eq!(ids, vec!["m-a", "m-b"]);
        assert_eq!(topics, vec![vec!["t".to_string()], vec!["t".to_string()]]);
        let mut counts = HashMap::new();
        counts.insert("t".into(), 4u32);
        let assigned = range_assign_multi(&ids, &topics, &counts);
        assert_eq!(assigned[0], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(assigned[1], vec![("t".into(), 2), ("t".into(), 3)]);
    }
}
