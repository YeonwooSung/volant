//! Stream application runtime (at-least-once and exactly-once).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_client::{Client, TransactionalProducer};
use volant_core::{Error, Result};

use crate::pipeline::Pipeline;
use crate::sink::TopicSink;
use crate::source::TopicSource;
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
/// **Exactly-once (Phase 151):** each non-empty step begins a transaction,
/// produces sink records transactionally, adds consumer group offsets, and
/// commits atomically. Empty polls (no input, no output) skip the transaction.
pub struct StreamApp {
    /// Topology name.
    pub name: String,
    source: TopicSource,
    sink: TopicSink,
    pipeline: Pipeline,
    guarantee: ProcessingGuarantee,
    /// Present when `guarantee` is [`ProcessingGuarantee::ExactlyOnce`].
    txn: Option<TransactionalProducer>,
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
        Ok(Self {
            name: topology.name,
            source,
            sink,
            pipeline: topology.pipeline,
            guarantee,
            txn,
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

    /// Exactly-once: transactional produce + deferred group offsets (Phase 151).
    ///
    /// Empty poll with no punctuate output skips the transaction.
    async fn step_exactly_once(&mut self) -> Result<()> {
        let records = self.source.poll().await?;
        let had_input = !records.is_empty();
        let mut out = self.pipeline.process(records)?;
        let now = now_ms();
        out.extend(self.pipeline.punctuate(now)?);

        // Empty poll, no punctuate emissions → no txn.
        if !had_input && out.is_empty() {
            return Ok(());
        }

        let group_id = self.source.group_id().to_owned();
        let pending = self.source.pending_offsets();
        // If somehow no outputs and no positions, nothing to commit.
        if out.is_empty() && pending.is_empty() {
            return Ok(());
        }

        let txn = self.txn.as_mut().ok_or_else(|| {
            Error::InvalidArgument("exactly-once step requires transactional producer".into())
        })?;

        if let Err(e) = eos_try_commit(txn, &self.sink, &group_id, pending, &out).await {
            if txn.is_open() {
                let _ = txn.abort().await;
            }
            return Err(e);
        }
        Ok(())
    }

    /// Process an offline batch (no network) — for tests.
    pub fn process_offline(&mut self, records: Vec<volant_core::Record>) -> Result<Vec<volant_core::Record>> {
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
) -> Result<()> {
    txn.begin().await?;
    sink.send_all_in_txn(txn, out).await?;
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
