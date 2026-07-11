//! Map operator.

use volant_core::{Record, Result};

use crate::operator::Operator;

/// Apply `f` to each record, producing exactly one output per input.
pub fn map<F>(f: F) -> impl Operator
where
    F: FnMut(Record) -> Result<Record> + Send + 'static,
{
    MapOp { f }
}

struct MapOp<F> {
    f: F,
}

impl<F> Operator for MapOp<F>
where
    F: FnMut(Record) -> Result<Record> + Send + 'static,
{
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        Ok(vec![(self.f)(record)?])
    }

    fn name(&self) -> &str {
        "map"
    }
}
