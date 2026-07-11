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

    /// Run a single record through all operators.
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
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}
