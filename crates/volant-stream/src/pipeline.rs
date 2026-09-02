//! Composable processing pipeline.

use bytes::Bytes;
use volant_client::Client;
use volant_core::{Record, Result};

use crate::operator::Operator;
use crate::state::fetch_changelog_records;

/// Ordered chain of operators.
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

impl Pipeline {
    /// Create an empty pipeline.
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    /// Append an operator to the pipeline.
    pub fn then<O: Operator + 'static>(mut self, op: O) -> Self {
        self.operators.push(Box::new(op));
        self
    }

    /// Append a boxed operator.
    pub fn then_box(mut self, op: Box<dyn Operator>) -> Self {
        self.operators.push(op);
        self
    }

    /// Number of operators in the chain.
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    /// Whether the pipeline has no operators.
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Run a batch of records through all operators.
    pub fn process(&mut self, mut records: Vec<Record>) -> Result<Vec<Record>> {
        for op in &mut self.operators {
            let mut next = Vec::new();
            for record in records {
                next.extend(op.process(record)?);
            }
            records = next;
        }
        Ok(records)
    }

    /// Punctuate every operator (window flush), chaining outputs through the
    /// remaining downstream operators.
    pub fn punctuate(&mut self, now_ms: i64) -> Result<Vec<Record>> {
        let mut all_out = Vec::new();
        let n = self.operators.len();
        for i in 0..n {
            let punctuated = self.operators[i].punctuate(now_ms)?;
            let mut records = punctuated;
            for j in (i + 1)..n {
                let mut next = Vec::new();
                for record in records {
                    next.extend(self.operators[j].process(record)?);
                }
                records = next;
            }
            all_out.extend(records);
        }
        Ok(all_out)
    }

    /// Enter checkpoint staging on every operator (Phase 153 EOS).
    pub fn begin_checkpoint(&mut self) {
        for op in &mut self.operators {
            op.begin_checkpoint();
        }
    }

    /// Commit staged state on every operator after successful EndTxn.
    pub fn commit_checkpoint(&mut self) -> Result<()> {
        for op in &mut self.operators {
            op.commit_checkpoint()?;
        }
        Ok(())
    }

    /// Abort staged state on every operator (txn fail or empty step).
    pub fn abort_checkpoint(&mut self) {
        for op in &mut self.operators {
            op.abort_checkpoint();
        }
    }

    /// Collect staged changelog deltas from every operator.
    pub fn staged_changelog(&self) -> Vec<(Bytes, Option<Bytes>)> {
        let mut out = Vec::new();
        for op in &self.operators {
            out.extend(op.staged_changelog());
        }
        out
    }

    /// Apply a changelog record to every operator (last-write-wins per store).
    pub fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        for op in &mut self.operators {
            op.apply_changelog(key.clone(), value.clone());
        }
    }

    /// Replay committed changelog records onto every operator store.
    ///
    /// Best-effort last-write-wins per key. Shared topic is applied to all
    /// stores (MVP: not per-store namespaced).
    pub async fn replay_changelog(&mut self, client: &Client, topic: &str) -> Result<()> {
        for (key, value) in fetch_changelog_records(client, topic).await? {
            self.apply_changelog(key, value);
        }
        Ok(())
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
