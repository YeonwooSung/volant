//! Composable processing pipeline.

use volant_core::{Record, Result};

use crate::operator::Operator;

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
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
