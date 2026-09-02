//! Stream application runtime (at-least-once and exactly-once).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_client::{Client, TransactionalProducer};
use volant_core::{Error, Result};

use crate::pipeline::Pipeline;
use crate::sink::TopicSink;
use crate::source::TopicSource;
use crate::topology::Topology;

/// Suffix used to build the dedicated cross-app fence transactional id.
///
/// Full id: `{application_id}{APP_FENCE_TXN_SUFFIX}` →
/// `{application_id}::__volant_app_fence`.
pub const APP_FENCE_TXN_SUFFIX: &str = "::__volant_app_fence";

/// Dedicated fence transactional id for a non-empty `application_id`.
///
/// Format: `{application_id}::__volant_app_fence`. One fence id per
/// application — not Kafka Streams `application.server` / task assignment.
pub fn app_fence_transactional_id(application_id: &str) -> String {
    format!("{application_id}{APP_FENCE_TXN_SUFFIX}")
}

/// Delivery / offset-commit guarantee for a stream application (Phase 151 / v0.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessingGuarantee {
    /// Default: produce sink then commit group offsets separately.
    ///
    /// Crash between produce and commit may redeliver inputs (duplicate outputs).
    AtLeastOnce,
    /// Atomic sink produce + group offset commit via Volant transactions.
    ///
    /// Uses [`TransactionalProducer`] with the given `transactional_id` (fences
    /// prior owners of **that** id). Optional [`Self::ExactlyOnce::application_id`]
    /// adds a second fence that covers every task of the same app even when
    /// transactional ids differ (v0.8). Depends on Volant write-through txns +
    /// soft markers — not full Kafka Streams EOS / 2PC with durable stream state.
    ExactlyOnce {
        /// Per-task transactional id (fences prior owners of the same id).
        ///
        /// Unchanged from Phase 151: not namespaced by `application_id`.
        transactional_id: String,
        /// Optional application id for **cross-app** fencing (v0.8).
        ///
        /// When `Some` and non-empty, the runtime claims
        /// [`app_fence_transactional_id`] via `InitProducerId` at start and
        /// heartbeats that fence id (BeginTxn + EndTxn abort) each EOS step.
        /// A second runtime with the same `application_id` (even a different
        /// `transactional_id`) bumps the fence epoch; this app's next step
        /// fails with a fenced / invalid-epoch error. `None` or empty = Phase
        /// 151/153 behavior (no app fence).
        application_id: Option<String>,
    },
}

impl Default for ProcessingGuarantee {
    fn default() -> Self {
        Self::AtLeastOnce
    }
}

impl ProcessingGuarantee {
    /// Non-empty application id when this is exactly-once with app fencing.
    pub fn application_id(&self) -> Option<&str> {
        match self {
            Self::ExactlyOnce {
                application_id: Some(app),
                ..
            } if !app.is_empty() => Some(app.as_str()),
            _ => None,
        }
    }
}

/// Running stream application.
///
/// **At-least-once (default):** offsets are committed **after** a successful
/// sink produce. A crash between produce and commit may redeliver input
/// records (duplicate outputs).
///
/// **Exactly-once (Phase 151 / 153):** each non-empty step stages durable
/// state, begins a transaction, produces sink records transactionally, adds
/// consumer group offsets, commits the txn, then commits the state checkpoint.
/// Empty polls (no input, no output) abort the checkpoint and skip the txn.
pub struct StreamApp {
    /// Topology name.
    pub name: String,
    source: TopicSource,
    sink: TopicSink,
    pipeline: Pipeline,
    guarantee: ProcessingGuarantee,
    /// Present when `guarantee` is [`ProcessingGuarantee::ExactlyOnce`].
    txn: Option<TransactionalProducer>,
    /// Process-wide app-fence producer (`{application_id}::__volant_app_fence`).
    ///
    /// Claimed at start via InitProducerId; heartbeated each EOS step so a
    /// later owner of the same fence id fences this runtime.
    app_fence: Option<TransactionalProducer>,
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
            ProcessingGuarantee::ExactlyOnce {
                transactional_id, ..
            } => {
                let addr = client.current_addr().await;
                let tp =
                    TransactionalProducer::connect(vec![addr], transactional_id.clone()).await?;
                Some(tp)
            }
        };
        // Claim `{application_id}::__volant_app_fence` before the first EOS step
        // so a later start with the same application_id fences this runtime.
        let app_fence = match guarantee.application_id() {
            Some(app) => {
                let addr = client.current_addr().await;
                let fence_id = app_fence_transactional_id(app);
                let mut fence = TransactionalProducer::connect(vec![addr], fence_id).await?;
                heartbeat_app_fence(&mut fence)
                    .await
                    .map_err(|e| map_app_fence_error(app, e))?;
                Some(fence)
            }
            None => None,
        };
        Ok(Self {
            name: topology.name,
            source,
            sink,
            pipeline: topology.pipeline,
            guarantee,
            txn,
            app_fence,
        })
    }

    /// Convenience: start with exactly-once using `transactional_id`.
    ///
    /// No application fence (`application_id = None`). Use
    /// [`Self::start_exactly_once_app`] for cross-app fencing.
    pub async fn start_exactly_once(
        client: Arc<Client>,
        topology: Topology,
        transactional_id: impl Into<String>,
    ) -> Result<Self> {
        Self::start_with_guarantee(
            client,
            ProcessingGuarantee::ExactlyOnce {
                transactional_id: transactional_id.into(),
                application_id: None,
            },
            topology,
        )
        .await
    }

    /// Start exactly-once with a cross-app fence on `application_id`.
    ///
    /// Empty `application_id` is treated as absent (Phase 151/153).
    pub async fn start_exactly_once_app(
        client: Arc<Client>,
        topology: Topology,
        application_id: impl Into<String>,
        transactional_id: impl Into<String>,
    ) -> Result<Self> {
        let application_id = application_id.into();
        Self::start_with_guarantee(
            client,
            ProcessingGuarantee::ExactlyOnce {
                transactional_id: transactional_id.into(),
                application_id: if application_id.is_empty() {
                    None
                } else {
                    Some(application_id)
                },
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

    /// Exactly-once: transactional produce + deferred group offsets (Phase 151)
    /// with durable state checkpoint after EndTxn (Phase 153) and optional
    /// cross-app fence heartbeat (v0.8).
    ///
    /// Order:
    /// 1. app-fence heartbeat (BeginTxn + abort on `{app}::__volant_app_fence`)
    /// 2. `pipeline.begin_checkpoint()` — durable puts stage only
    /// 3. poll → process → punctuate
    /// 4. empty skip → `abort_checkpoint` (no txn)
    /// 5. txn begin → produce → add_offsets → commit
    /// 6. on success → `pipeline.commit_checkpoint()`
    /// 7. on fail → abort txn + `abort_checkpoint`
    ///
    /// ALO path does not use checkpoints (DurableStore remains immediate-put).
    /// Absent / empty `application_id` skips step 1 (Phase 151/153).
    async fn step_exactly_once(&mut self) -> Result<()> {
        if let Err(e) = self.heartbeat_app_fence_if_configured().await {
            return Err(e);
        }

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

        if let Err(e) = eos_try_commit(txn, &self.sink, &group_id, pending, &out).await {
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

    /// BeginTxn + abort on the app-fence producer; stale epoch → fenced.
    async fn heartbeat_app_fence_if_configured(&mut self) -> Result<()> {
        let Some(app) = self.guarantee.application_id().map(str::to_owned) else {
            return Ok(());
        };
        let Some(fence) = self.app_fence.as_mut() else {
            return Ok(());
        };
        heartbeat_app_fence(fence)
            .await
            .map_err(|e| map_app_fence_error(&app, e))
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

/// Cheap fence heartbeat: BeginTxn checks the stored producer epoch.
///
/// After a second app `InitProducerId`s the same fence id, this fails with
/// [`volant_protocol::ErrorCode::InvalidProducerEpoch`] (native 19).
async fn heartbeat_app_fence(fence: &mut TransactionalProducer) -> Result<()> {
    fence.begin().await?;
    fence.abort().await
}

fn is_producer_epoch_error(e: &Error) -> bool {
    let s = e.to_string();
    s.contains("error_code=19")
        || s.contains("InvalidProducerEpoch")
        || s.to_ascii_lowercase().contains("fenced")
        || s.to_ascii_lowercase().contains("producer epoch")
}

fn map_app_fence_error(application_id: &str, e: Error) -> Error {
    if is_producer_epoch_error(&e) {
        Error::Protocol(format!(
            "application fenced (application_id={application_id}): {e}"
        ))
    } else {
        Error::Protocol(format!(
            "application fence heartbeat failed (application_id={application_id}): {e}"
        ))
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
