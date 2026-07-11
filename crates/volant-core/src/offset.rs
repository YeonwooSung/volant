//! Log offset types.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Monotonic position of a record in a partition log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
pub struct Offset(pub u64);

impl Offset {
    /// The earliest possible offset.
    pub const ZERO: Self = Self(0);

    /// Create an offset from a raw u64.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Return the next offset after this one.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Raw underlying value.
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Offset {
    fn from(value: u64) -> Self {
        Self(value)
    }
}
