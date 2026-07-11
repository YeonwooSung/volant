//! Filter operator.

use volant_core::{Record, Result};

use crate::operator::Operator;

/// Keep records for which `pred` returns true.
pub fn filter<F>(pred: F) -> impl Operator
where
    F: FnMut(&Record) -> bool + Send + 'static,
{
    FilterOp { pred }
}

struct FilterOp<F> {
    pred: F,
}

impl<F> Operator for FilterOp<F>
where
    F: FnMut(&Record) -> bool + Send + 'static,
{
    fn process(&mut self, record: Record) -> Result<Vec<Record>> {
        if (self.pred)(&record) {
            Ok(vec![record])
        } else {
            Ok(vec![])
        }
    }

    fn name(&self) -> &str {
        "filter"
    }
}
