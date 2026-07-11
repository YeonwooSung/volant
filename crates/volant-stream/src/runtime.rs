//! Stream application runtime (at-least-once).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use volant_client::Client;
use volant_core::Result;

use crate::pipeline::Pipeline;
use crate::sink::TopicSink;
use crate::source::TopicSource;
use crate::topology::Topology;

/// Running stream application.
///
/// **At-least-once semantics:** offsets are committed **after** a successful
/// sink produce. A crash between produce and commit may redeliver input
/// records (duplicate outputs).
pub struct StreamApp {
    /// Topology name.
    pub name: String,
    source: TopicSource,
    sink: TopicSink,
    pipeline: Pipeline,
}

impl StreamApp {
    /// Join the source group and prepare the sink from a built topology.
    pub async fn start(client: Arc<Client>, topology: Topology) -> Result<Self> {
        let source = TopicSource::join(
            Arc::clone(&client),
            topology.source_topic,
            topology.source_config,
        )
        .await?;
        let sink = TopicSink::new(client, topology.sink_topic);
        Ok(Self {
            name: topology.name,
            source,
            sink,
            pipeline: topology.pipeline,
        })
    }

    /// Run until `max_polls` is reached (if `Some`) or forever.
    ///
    /// Each iteration: poll → process → punctuate → sink → commit.
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
    pub async fn step(&mut self) -> Result<()> {
        let records = self.source.poll().await?;
        let mut out = self.pipeline.process(records)?;
        let now = now_ms();
        out.extend(self.pipeline.punctuate(now)?);
        self.sink.send_all(&out).await?;
        self.source.commit().await?;
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
