//! Fluent topology builder (`StreamBuilder`).

use volant_core::{Error, Result};

use crate::operator::Operator;
use crate::pipeline::Pipeline;
use crate::source::SourceConfig;

/// Fluent builder for a source → operators → sink topology.
pub struct StreamBuilder {
    name: String,
    source_topic: Option<String>,
    source_config: Option<SourceConfig>,
    sink_topic: Option<String>,
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
            pipeline: Pipeline::new(),
        }
    }

    /// Application / topology name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Set the source topic and consumer config.
    pub fn source_topic(
        mut self,
        topic: impl Into<String>,
        config: SourceConfig,
    ) -> Self {
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

    /// Append a keyed count reduce.
    pub fn reduce_count(self) -> Self {
        self.then(crate::ops::count_reduce())
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
    /// Operator chain.
    pub pipeline: Pipeline,
}
