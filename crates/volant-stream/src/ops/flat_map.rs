//! Flat-map operator.

use volant_core::{Record, Result};

use crate::operator::Operator;

/// Expand each input into zero or more outputs.
pub fn flat_map<F>(f: F) -> impl Operator
where
    F: FnMut(Record) -> Result<Vec<Record>> + Send + 'static,
{
    FlatMapOp { f }
}

struct FlatMapOp<F> {
    f: F,
}

impl<F> Operator for FlatMapOp<F>
where
    F: FnMut(Record) -> Result<Vec<Record>> + Send + 'static,
{
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        (self.f)(record)
    }

    fn name(&self) -> &str {
        "flat_map"
    }
}
