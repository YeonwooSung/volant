//! Fluent topology builder (`StreamBuilder`).

use std::path::{Path, PathBuf};

use volant_core::{Error, Result};

use crate::operator::Operator;
use crate::pipeline::Pipeline;
use crate::runtime::ProcessingGuarantee;
use crate::source::SourceConfig;
use crate::state::{StreamStateError, DEFAULT_CHANGELOG_TOPIC};

/// Fluent builder for a source → operators → sink topology.
pub struct StreamBuilder {
    name: String,
    source_topic: Option<String>,
    source_config: Option<SourceConfig>,
    sink_topic: Option<String>,
    /// Optional directory for durable stream state ([`crate::state::DurableStore`]).
    ///
    /// Phase 149: stored on the built [`Topology`] for apps to open stores.
    /// Callers wire durable reduce via [`crate::ops::count_reduce_durable`] or
    /// [`StreamBuilder::reduce_count_durable`] — default [`StreamBuilder::reduce_count`]
    /// still uses in-memory state.
    state_dir: Option<PathBuf>,
    /// Processing guarantee (Phase 151). Default: at-least-once.
    processing_guarantee: ProcessingGuarantee,
    /// Opt-in changelog topic (v0.9). `None` = Phase 153 process-local only.
    changelog_topic: Option<String>,
    pipeline: Pipeline,
}

impl StreamBuilder {
    /// Start a new topology named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source_topic: None,
            source_config: None,
            sink_topic: None,
            state_dir: None,
            processing_guarantee: ProcessingGuarantee::AtLeastOnce,
            changelog_topic: None,
            pipeline: Pipeline::new(),
        }
    }

    /// Application / topology name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set an optional directory for durable stream state stores.
    ///
    /// Does not auto-wire operators: pass the path to
    /// [`crate::ops::count_reduce_durable`] / [`StreamBuilder::reduce_count_durable`],
    /// or open [`crate::state::DurableStore`] yourself. The path is copied onto
    /// [`Topology::state_dir`] at build time for application use.
    pub fn state_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.state_dir = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set the source topic and consumer config.
    pub fn source_topic(mut self, topic: impl Into<String>, config: SourceConfig) -> Self {
        self.source_topic = Some(topic.into());
        self.source_config = Some(config);
        self
    }

    /// Append a map operator.
    pub fn map<F>(self, f: F) -> Self
    where
        F: FnMut(volant_core::Record) -> Result<volant_core::Record> + Send + 'static,
    {
        self.then(crate::ops::map(f))
    }

    /// Append a filter operator.
    pub fn filter<F>(self, f: F) -> Self
    where
        F: FnMut(&volant_core::Record) -> bool + Send + 'static,
    {
        self.then(crate::ops::filter(f))
    }

    /// Append a flat_map operator.
    pub fn flat_map<F>(self, f: F) -> Self
    where
        F: FnMut(volant_core::Record) -> Result<Vec<volant_core::Record>> + Send + 'static,
    {
        self.then(crate::ops::flat_map(f))
    }

    /// Append a foreach side-effect operator.
    pub fn foreach<F>(self, f: F) -> Self
    where
        F: FnMut(&volant_core::Record) + Send + 'static,
    {
        self.then(crate::ops::foreach(f))
    }

    /// Append a keyed count reduce with in-memory state.
    pub fn reduce_count(self) -> Self {
        self.then(crate::ops::count_reduce())
    }

    /// Append a keyed count reduce backed by [`crate::state::DurableStore`]
    /// under [`StreamBuilder::state_dir`].
    ///
    /// Requires `state_dir` to be set. Opens (or creates) the store immediately.
    pub fn reduce_count_durable(self) -> std::result::Result<Self, StreamStateError> {
        let path = self.state_dir.clone().ok_or_else(|| {
            StreamStateError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "state_dir required for reduce_count_durable",
            ))
        })?;
        let op = crate::ops::count_reduce_durable(path)?;
        Ok(self.then(op))
    }

    /// Append an arbitrary operator.
    pub fn then<O: Operator + 'static>(mut self, op: O) -> Self {
        self.pipeline = std::mem::take(&mut self.pipeline).then(op);
        self
    }

    /// Set the sink topic.
    pub fn sink_topic(mut self, topic: impl Into<String>) -> Self {
        self.sink_topic = Some(topic.into());
        self
    }

    /// Enable exactly-once processing (Phase 151) with the given transactional id.
    ///
    /// Sink produces and source group offsets are committed atomically via
    /// [`volant_client::TransactionalProducer`]. Fences prior owners of the
    /// same id. Default remains [`ProcessingGuarantee::AtLeastOnce`].
    pub fn exactly_once(mut self, transactional_id: impl Into<String>) -> Self {
        self.processing_guarantee = ProcessingGuarantee::ExactlyOnce {
            transactional_id: transactional_id.into(),
        };
        self
    }

    /// Override the processing guarantee explicitly.
    pub fn processing_guarantee(mut self, guarantee: ProcessingGuarantee) -> Self {
        self.processing_guarantee = guarantee;
        self
    }

    /// Enable changelog produce/replay with the default topic
    /// [`DEFAULT_CHANGELOG_TOPIC`] (`__volant_changelog`).
    ///
    /// Opt-in. Existing `exactly_once` + [`crate::state::DurableStore::open`]
    /// without this stays Phase 153 (process-local staging). Changelog
    /// records are produced only on the EOS path, in the same txn as sink +
    /// offsets.
    pub fn changelog(self) -> Self {
        self.changelog_topic(DEFAULT_CHANGELOG_TOPIC)
    }

    /// Enable changelog produce/replay on `topic` (created on first use).
    ///
    /// Prefer `{topology_or_store}__changelog` when more than one app shares
    /// a cluster. Default off.
    pub fn changelog_topic(mut self, topic: impl Into<String>) -> Self {
        self.changelog_topic = Some(topic.into());
        self
    }

    /// Build a [`Topology`] (validates source + sink are set).
    pub fn build(self) -> Result<Topology> {
        let source_topic = self
            .source_topic
            .ok_or_else(|| Error::InvalidArgument("source_topic required".into()))?;
        let source_config = self
            .source_config
            .ok_or_else(|| Error::InvalidArgument("source config required".into()))?;
        let sink_topic = self
            .sink_topic
            .ok_or_else(|| Error::InvalidArgument("sink_topic required".into()))?;
        Ok(Topology {
            name: self.name,
            source_topic,
            source_config,
            sink_topic,
            state_dir: self.state_dir,
            processing_guarantee: self.processing_guarantee,
            changelog_topic: self.changelog_topic,
            pipeline: self.pipeline,
        })
    }

    /// Build an offline pipeline only (no source/sink) for unit tests.
    pub fn build_pipeline(self) -> Pipeline {
        self.pipeline
    }
}

/// Compiled topology ready for [`crate::runtime::StreamApp`].
pub struct Topology {
    /// Topology name.
    pub name: String,
    /// Input topic.
    pub source_topic: String,
    /// Group consumer config.
    pub source_config: SourceConfig,
    /// Output topic.
    pub sink_topic: String,
    /// Optional durable state directory (Phase 149). Apps may open
    /// [`crate::state::DurableStore`] here; not auto-consumed by the runtime.
    pub state_dir: Option<PathBuf>,
    /// At-least-once (default) or exactly-once (Phase 151).
    pub processing_guarantee: ProcessingGuarantee,
    /// Opt-in changelog topic (v0.9). `None` keeps Phase 153 process-local staging.
    pub changelog_topic: Option<String>,
    /// Operator chain.
    pub pipeline: Pipeline,
}
