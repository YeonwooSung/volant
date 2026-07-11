//! Foreach (side-effect) operator.

use volant_core::{Record, Result};

use crate::operator::Operator;

/// Invoke `f` for each record and forward the record unchanged.
pub fn foreach<F>(f: F) -> impl Operator
where
    F: FnMut(&Record) + Send + 'static,
{
    ForeachOp { f }
}

struct ForeachOp<F> {
    f: F,
}

impl<F> Operator for ForeachOp<F>
where
    F: FnMut(&Record) + Send + 'static,
{
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        (self.f)(&record);
        Ok(vec![record])
    }

    fn name(&self) -> &str {
        "foreach"
    }
}
