//! Built-in stream operators.

mod filter;
mod flat_map;
mod foreach;
mod map;
mod reduce;

pub use filter::filter;
pub use flat_map::flat_map;
pub use foreach::foreach;
pub use map::map;
pub use reduce::{
    count_reduce, count_reduce_durable, count_reduce_with_store, reduce, reduce_with_store, Reduce,
};
