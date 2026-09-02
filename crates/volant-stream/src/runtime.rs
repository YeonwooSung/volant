//! Stream application runtime (at-least-once and exactly-once).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use volant_client::{Client, TransactionalProducer};
use volant_core::{Error, Result};

use crate::pipeline::Pipeline;
use crate::sink::TopicSink;
use crate::source::TopicSource;
use crate::state::{ensure_changelog_topic, produce_changelog_in_txn};
use crate::topology::Topology;

/// Delivery / offset-commit guarantee for a stream application (Phase 151).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingGuarantee {
    /// Default: produce sink then commit group offsets separately.
    ///
    /// Crash between produce and commit may redeliver inputs (duplicate outputs).
    AtLeastOnce,
    /// Atomic sink produce + group offset commit via Volant transactions.
    ///
    /// Uses [`TransactionalProducer`] with the given `transactional_id` (fences
    /// prior owners). Depends on Volant write-through txns + soft markers — not
    /// full Kafka Streams EOS / 2PC with durable stream state.
    ExactlyOnce {
        /// Transactional id (fences prior owners of the same id).
        transactional_id: String,
    },
}

impl Default for ProcessingGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

/// Running stream application.
///
/// **At-least-once (default):** offsets are committed **after** a successful
/// sink produce. A crash between produce and commit may redeliver input
/// records (duplicate outputs).
///
/// **Exactly-once (Phase 151 / 153 / v0.9):** each non-empty step stages durable
/// state, begins a transaction, produces sink records, optionally produces
/// changelog deltas, adds consumer group offsets, commits the txn, then
/// commits the state checkpoint. Empty polls abort the checkpoint and skip
/// the txn. Changelog is opt-in ([`crate::topology::StreamBuilder::changelog_topic`]).
pub struct StreamApp {
    /// Topology name.
    pub name: String,
    source: TopicSource,
    sink: TopicSink,
    pipeline: Pipeline,
    guarantee: ProcessingGuarantee,
    /// Present when `guarantee` is [`ProcessingGuarantee::ExactlyOnce`].
    txn: Option<TransactionalProducer>,
    /// Opt-in changelog topic (v0.9). `None` = Phase 153 process-local only.
    changelog_topic: Option<String>,
}

impl StreamApp {
    /// Join the source group and prepare the sink from a built topology.
    ///
    /// Uses [`Topology::processing_guarantee`] (default at-least-once).
    pub async fn start(client: Arc<Client>, topology: Topology) -> Result<Self> {
        Self::start_with_guarantee(client, topology.processing_guarantee.clone(), topology).await
    }

    /// Start with an explicit processing guarantee (overrides topology flag).
    pub async fn start_with_guarantee(
        client: Arc<Client>,
        guarantee: ProcessingGuarantee,
        topology: Topology,
    ) -> Result<Self> {
        let source = TopicSource::join(
            Arc::clone(&client),
            topology.source_topic,
            topology.source_config,
        )
        .await?;
        let sink = TopicSink::new(Arc::clone(&client), topology.sink_topic);
        let txn = match &guarantee {
            ProcessingGuarantee::AtLeastOnce => None,
            ProcessingGuarantee::ExactlyOnce { transactional_id } => {
                let addr = client.current_addr().await;
                let tp =
                    TransactionalProducer::connect(vec![addr], transactional_id.clone()).await?;
                Some(tp)
            }
        };
        let changelog_topic = topology.changelog_topic;
        let mut pipeline = topology.pipeline;
        // Replay only when EOS + changelog are both configured (opt-in).
        if matches!(guarantee, ProcessingGuarantee::ExactlyOnce { .. }) {
            if let Some(ref topic) = changelog_topic {
                ensure_changelog_topic(&client, topic).await?;
                pipeline.replay_changelog(&client, topic).await?;
            }
        }
        Ok(Self {
            name: topology.name,
            source,
            sink,
            pipeline,
            guarantee,
            txn,
            changelog_topic,
        })
    }

    /// Convenience: start with exactly-once using `transactional_id`.
    pub async fn start_exactly_once(
        client: Arc<Client>,
        topology: Topology,
        transactional_id: impl Into<String>,
    ) -> Result<Self> {
        Self::start_with_guarantee(
            client,
            ProcessingGuarantee::ExactlyOnce {
                transactional_id: transactional_id.into(),
            },
            topology,
        )
        .await
    }

    /// Active processing guarantee.
    pub fn processing_guarantee(&self) -> &ProcessingGuarantee {
        &self.guarantee
    }

    /// Opt-in changelog topic, if configured.
    pub fn changelog_topic(&self) -> Option<&str> {
        self.changelog_topic.as_deref()
    }

    /// Run until `max_polls` is reached (if `Some`) or forever.
    ///
    /// Each iteration: poll → process → punctuate → sink → commit (ALO or EOS).
    pub async fn run(&mut self, max_polls: Option<u64>) -> Result<()> {
        let mut polls = 0u64;
        loop {
            if let Some(max) = max_polls {
                if polls >= max {
                    break;
                }
            }
            self.step().await?;
            polls = polls.saturating_add(1);
        }
        Ok(())
    }

    /// Single poll → process → sink → commit cycle.
    ///
    /// Dispatches on [`ProcessingGuarantee`].
    pub async fn step(&mut self) -> Result<()> {
        match &self.guarantee {
            ProcessingGuarantee::AtLeastOnce => self.step_at_least_once().await,
            ProcessingGuarantee::ExactlyOnce { .. } => self.step_exactly_once().await,
        }
    }

    /// At-least-once: produce then commit offsets (may redeliver on crash).
    async fn step_at_least_once(&mut self) -> Result<()> {
        let records = self.source.poll().await?;
        let mut out = self.pipeline.process(records)?;
        let now = now_ms();
        out.extend(self.pipeline.punctuate(now)?);
        self.sink.send_all(&out).await?;
        self.source.commit().await?;
        Ok(())
    }

    /// Exactly-once: transactional produce + deferred group offsets (Phase 151)
    /// with durable state checkpoint after EndTxn (Phase 153) and optional
    /// changelog produce in the same txn (v0.9).
    ///
    /// Order:
    /// 1. `pipeline.begin_checkpoint()` — durable puts stage only
    /// 2. poll → process → punctuate
    /// 3. empty skip → `abort_checkpoint` (no txn)
    /// 4. txn begin → sink produce → **changelog produce** → add_offsets → EndTxn
    /// 5. on success → `pipeline.commit_checkpoint()`
    /// 6. on fail → abort txn + `abort_checkpoint`
    ///
    /// ALO path does not use checkpoints (DurableStore remains immediate-put).
    /// Changelog produce is skipped when no topic is configured or no staged deltas.
    async fn step_exactly_once(&mut self) -> Result<()> {
        self.pipeline.begin_checkpoint();

        let records = match self.source.poll().await {
            Ok(r) => r,
            Err(e) => {
                self.pipeline.abort_checkpoint();
                return Err(e);
            }
        };
        let had_input = !records.is_empty();
        let mut out = match self.pipeline.process(records) {
            Ok(o) => o,
            Err(e) => {
                self.pipeline.abort_checkpoint();
                return Err(e);
            }
        };
        let now = now_ms();
        match self.pipeline.punctuate(now) {
            Ok(p) => out.extend(p),
            Err(e) => {
                self.pipeline.abort_checkpoint();
                return Err(e);
            }
        }

        // Empty poll, no punctuate emissions → no txn; drop staged state.
        if !had_input && out.is_empty() {
            self.pipeline.abort_checkpoint();
            return Ok(());
        }

        let group_id = self.source.group_id().to_owned();
        let pending = self.source.pending_offsets();
        // If somehow no outputs and no positions, nothing to commit.
        if out.is_empty() && pending.is_empty() {
            self.pipeline.abort_checkpoint();
            return Ok(());
        }

        let txn = match self.txn.as_mut() {
            Some(t) => t,
            None => {
                self.pipeline.abort_checkpoint();
                return Err(Error::InvalidArgument(
                    "exactly-once step requires transactional producer".into(),
                ));
            }
        };

        let deltas = self.pipeline.staged_changelog();
        if let Err(e) = eos_try_commit(
            txn,
            &self.sink,
            &group_id,
            pending,
            &out,
            self.changelog_topic.as_deref(),
            &deltas,
        )
        .await
        {
            if txn.is_open() {
                let _ = txn.abort().await;
            }
            self.pipeline.abort_checkpoint();
            return Err(e);
        }

        // Only after successful EndTxn — durable state may advance.
        if let Err(e) = self.pipeline.commit_checkpoint() {
            // Broker txn already committed; state commit failed (honesty residual).
            return Err(e);
        }
        Ok(())
    }

    /// Process an offline batch (no network) — for tests.
    pub fn process_offline(
        &mut self,
        records: Vec<volant_core::Record>,
    ) -> Result<Vec<volant_core::Record>> {
        let mut out = self.pipeline.process(records)?;
        out.extend(self.pipeline.punctuate(now_ms())?);
        Ok(out)
    }

    /// Access the pipeline mutably (tests).
    pub fn pipeline_mut(&mut self) -> &mut Pipeline {
        &mut self.pipeline
    }

    /// Leave the consumer group cleanly.
    pub async fn shutdown(self) -> Result<()> {
        self.source.leave().await
    }
}

async fn eos_try_commit(
    txn: &mut TransactionalProducer,
    sink: &TopicSink,
    group_id: &str,
    pending: Vec<(String, u32, u64)>,
    out: &[volant_core::Record],
    changelog_topic: Option<&str>,
    deltas: &[(Bytes, Option<Bytes>)],
) -> Result<()> {
    txn.begin().await?;
    sink.send_all_in_txn(txn, out).await?;
    if let Some(topic) = changelog_topic {
        produce_changelog_in_txn(txn, topic, deltas).await?;
    }
    if !pending.is_empty() {
        txn.add_offsets(group_id, pending);
    }
    txn.commit().await?;
    Ok(())
}

/// Run a pipeline offline (no broker): process + optional punctuate.
pub fn process_pipeline(
    pipeline: &mut Pipeline,
    records: Vec<volant_core::Record>,
    punctuate_now_ms: Option<i64>,
) -> Result<Vec<volant_core::Record>> {
    let mut out = pipeline.process(records)?;
    if let Some(now) = punctuate_now_ms {
        out.extend(pipeline.punctuate(now)?);
    }
    Ok(out)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
