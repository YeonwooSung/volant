//! Wire protocol codec and RPC framing for Volant.
//!
//! Binary framing is designed for zero-copy decode paths where possible:
//! length-prefixed frames, CRC-protected headers, and batch-oriented payloads.

#![deny(missing_docs)]

pub mod codec;
pub mod frame;
pub mod request;
pub mod response;

pub use frame::{Frame, FrameHeader};
pub use request::Request;
pub use response::Response;
