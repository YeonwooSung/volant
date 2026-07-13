//! Group-coordinated consumer (`GroupConsumer`).

use std::collections::HashMap;
use std::sync::Arc;

use volant_core::{Offset, Result};
use volant_protocol::{FetchRecord, OffsetCommitEntry, OffsetEntry};

use crate::client::Client;

/// Wire sentinel: unknown / not-committed offset (`docs/PHASE3_SPEC.md`).
const OFFSET_UNKNOWN: u64 = u64::MAX;

/// High-level consumer that joins a group, polls assigned partitions, and commits.
#[derive(Debug)]
pub struct GroupConsumer {
    client: Arc<Client>,
    group_id: String,
    topics: Vec<String>,
    session_timeout_ms: u32,
    member_id: String,
    /// Static membership instance id (Phase 12); empty = dynamic.
    group_instance_id: String,
    generation: u32,
    assignment: Vec<(String, u32)>,
    /// Next fetch offset per (topic, partition).
    positions: HashMap<(String, u32), u64>,
}

impl GroupConsumer {
    /// Join a consumer group on the given topics.
    pub async fn join(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
    ) -> Result<Self> {
        Self::join_static(client, group_id, topics, session_timeout_ms, "").await
    }

    /// Join with static membership (`group_instance_id`, Phase 12).
    ///
    /// Empty `group_instance_id` is dynamic membership.
    pub async fn join_static(
        client: Arc<Client>,
        group_id: impl Into<String>,
        topics: Vec<String>,
        session_timeout_ms: u32,
        group_instance_id: impl Into<String>,
    ) -> Result<Self> {
        let group_id = group_id.into();
        let group_instance_id = group_instance_id.into();
        let timeout = if session_timeout_ms == 0 {
            10_000
        } else {
            session_timeout_ms
        };
        let mut this = Self {
            client,
            group_id,
            topics,
            session_timeout_ms: timeout,
            member_id: String::new(),
            group_instance_id,
            generation: 0,
            assignment: Vec::new(),
            positions: HashMap::new(),
        };
        this.do_join().await?;
        Ok(this)
    }

    async fn do_join(&mut self) -> Result<()> {
        let result = self
            .client
            .join_group_with_instance(
                &self.group_id,
                &self.member_id,
                self.session_timeout_ms,
                self.topics.clone(),
                &self.group_instance_id,
            )
            .await?;
        self.member_id = result.member_id;
        self.generation = result.generation;
        self.assignment = result
            .assignment
            .into_iter()
            .map(|a| (a.topic, a.partition))
            .collect();
        self.reset_positions_from_offsets().await?;
        Ok(())
    }

    async fn reset_positions_from_offsets(&mut self) -> Result<()> {
        self.positions.clear();
        if self.assignment.is_empty() {
            return Ok(());
        }
        let entries: Vec<OffsetEntry> = self
            .assignment
            .iter()
            .map(|(t, p)| OffsetEntry {
                topic: t.clone(),
                partition: *p,
            })
            .collect();
        let fetched = self.client.fetch_offsets(&self.group_id, entries).await?;
        for e in fetched {
            let pos = if e.offset == OFFSET_UNKNOWN { 0 } else { e.offset };
            self.positions.insert((e.topic, e.partition), pos);
        }
        // Ensure every assigned partition has a position.
        for (t, p) in &self.assignment {
            self.positions.entry((t.clone(), *p)).or_insert(0);
        }
        Ok(())
    }

    /// Heartbeat + fetch from all assigned partitions.
    pub async fn poll(&mut self) -> Result<Vec<FetchedRecord>> {
        // Heartbeat; re-join on rebalance.
        let hb = self
            .client
            .heartbeat(&self.group_id, &self.member_id, self.generation)
            .await?;
        if hb.needs_rebalance() {
            self.do_join().await?;
        }

        let mut out = Vec::new();
        let assignment = self.assignment.clone();
        for (topic, partition) in assignment {
            let from = *self.positions.get(&(topic.clone(), partition)).unwrap_or(&0);
            let result = self
                .client
                .fetch(&topic, partition, Offset::new(from), 100, 0)
                .await?;
            for r in result.records {
                let next = r.offset.saturating_add(1);
                self.positions.insert((topic.clone(), partition), next);
                out.push(FetchedRecord {
                    topic: topic.clone(),
                    partition,
                    record: r,
                });
            }
        }
        Ok(out)
    }

    /// Commit last+1 positions for all assigned partitions.
    pub async fn commit(&self) -> Result<()> {
        if self.positions.is_empty() {
            return Ok(());
        }
        let entries: Vec<OffsetCommitEntry> = self
            .positions
            .iter()
            .map(|((topic, partition), offset)| OffsetCommitEntry {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                metadata: String::new(),
            })
            .collect();
        self.client
            .commit_offsets(&self.group_id, &self.member_id, self.generation, entries)
            .await
    }

    /// Leave the group (consumes self).
    pub async fn leave(self) -> Result<()> {
        self.client
            .leave_group(&self.group_id, &self.member_id)
            .await
    }

    /// Current assignment as (topic, partition) pairs.
    pub fn assignment(&self) -> &[(String, u32)] {
        &self.assignment
    }

    /// Group member id.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Current generation.
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Group id.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Current next-read positions.
    pub fn positions(&self) -> &HashMap<(String, u32), u64> {
        &self.positions
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
