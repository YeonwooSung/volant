//! Shared error types.

use thiserror::Error;

/// Convenience result alias for Volant crates.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error type used across Volant components.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O failure (disk, network, mmap).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol encode/decode failure.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Storage engine failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// Topic or partition not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Invalid argument or configuration.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Operation not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}
