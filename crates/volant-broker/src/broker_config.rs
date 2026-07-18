//! Broker-level config keys for Kafka Describe/AlterConfigs (Phase 99).
//!
//! Process-local knobs only (same as `Broker` setters / env at construction).
//! Not a full Kafka DynamicBrokerConfig surface.

use volant_core::{Error, Result};

use crate::kafka::fetch_session::{DEFAULT_IDLE_TIMEOUT_MS, DEFAULT_MAX_SESSIONS};

/// Kafka standard: max transaction timeout (Phase 96).
pub const KEY_TRANSACTION_MAX_TIMEOUT_MS: &str = "transaction.max.timeout.ms";
/// Volant open-txn broker default timeout (Phase 93).
pub const KEY_OPEN_TXN_TIMEOUT_MS: &str = "volant.open.transaction.timeout.ms";
/// Volant prepared-txn timeout (Phase 92).
pub const KEY_PREPARED_TXN_TIMEOUT_MS: &str = "volant.prepared.transaction.timeout.ms";
/// Volant fetch-session idle TTL (Phase 95).
pub const KEY_FETCH_SESSION_IDLE_MS: &str = "volant.fetch.session.idle.ms";
/// Volant max concurrent fetch sessions (Phase 95).
pub const KEY_FETCH_SESSION_MAX: &str = "volant.fetch.session.max";
/// Volant background sweep interval (Phase 97).
pub const KEY_SWEEP_INTERVAL_MS: &str = "volant.sweep.interval.ms";

/// Product default for `transaction.max.timeout.ms` (Kafka-aligned 15m).
pub const DEFAULT_TRANSACTION_MAX_TIMEOUT_MS: u64 = 900_000;
/// Product default for open-txn timeout.
pub const DEFAULT_OPEN_TXN_TIMEOUT_MS: u64 = 60_000;
/// Product default for prepared-txn timeout.
pub const DEFAULT_PREPARED_TXN_TIMEOUT_MS: u64 = 60_000;
/// Product default for sweep interval.
pub const DEFAULT_SWEEP_INTERVAL_MS: u64 = 1_000;

/// All broker config keys in stable Describe order.
pub const BROKER_CONFIG_KEYS: &[&str] = &[
    KEY_TRANSACTION_MAX_TIMEOUT_MS,
    KEY_OPEN_TXN_TIMEOUT_MS,
    KEY_PREPARED_TXN_TIMEOUT_MS,
    KEY_FETCH_SESSION_IDLE_MS,
    KEY_FETCH_SESSION_MAX,
    KEY_SWEEP_INTERVAL_MS,
];

/// Short documentation for DescribeConfigs v3+ when requested.
pub fn documentation(key: &str) -> Option<&'static str> {
    match key {
        KEY_TRANSACTION_MAX_TIMEOUT_MS => {
            Some("Max transaction timeout in ms; InitProducerId over-max → error 50; 0 = no max")
        }
        KEY_OPEN_TXN_TIMEOUT_MS => {
            Some("Broker default open transaction timeout in ms when client timeout ≤ 0; 0 disables")
        }
        KEY_PREPARED_TXN_TIMEOUT_MS => {
            Some("Prepared (2PC) transaction timeout in ms before auto-abort; 0 disables")
        }
        KEY_FETCH_SESSION_IDLE_MS => {
            Some("Fetch session idle TTL in ms; 0 disables idle eviction")
        }
        KEY_FETCH_SESSION_MAX => {
            Some("Max concurrent fetch sessions; 0 = unlimited")
        }
        KEY_SWEEP_INTERVAL_MS => {
            Some("Background open/prepared/session sweep interval in ms; 0 disables sweeper")
        }
        _ => None,
    }
}

/// Product default string for DELETE / empty Alter value restore.
pub fn product_default(key: &str) -> Option<u64> {
    match key {
        KEY_TRANSACTION_MAX_TIMEOUT_MS => Some(DEFAULT_TRANSACTION_MAX_TIMEOUT_MS),
        KEY_OPEN_TXN_TIMEOUT_MS => Some(DEFAULT_OPEN_TXN_TIMEOUT_MS),
        KEY_PREPARED_TXN_TIMEOUT_MS => Some(DEFAULT_PREPARED_TXN_TIMEOUT_MS),
        KEY_FETCH_SESSION_IDLE_MS => Some(DEFAULT_IDLE_TIMEOUT_MS),
        KEY_FETCH_SESSION_MAX => Some(DEFAULT_MAX_SESSIONS as u64),
        KEY_SWEEP_INTERVAL_MS => Some(DEFAULT_SWEEP_INTERVAL_MS),
        _ => None,
    }
}

/// Whether `key` is a known broker config.
pub fn is_known_key(key: &str) -> bool {
    product_default(key).is_some()
}

/// Validate entries without applying (validate_only / pre-check).
///
/// Empty values mean DELETE (restore product default) and are always valid for
/// known keys.
pub fn validate_entries(entries: &[(String, String)]) -> Result<()> {
    for (k, v) in entries {
        if !is_known_key(k) {
            return Err(Error::InvalidArgument(format!(
                "unknown broker config key: {k}"
            )));
        }
        if v.trim().is_empty() {
            continue;
        }
        parse_u64(v, k)?;
    }
    Ok(())
}

/// Parse a non-empty decimal u64 config value.
pub fn parse_u64(v: &str, key: &str) -> Result<u64> {
    let t = v.trim();
    if t.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "empty value for broker config {key}"
        )));
    }
    t.parse::<u64>().map_err(|_| {
        Error::InvalidArgument(format!(
            "invalid broker config value for {key}: {v} (want non-negative integer)"
        ))
    })
}

/// Resolve SET value or empty → product default for DELETE.
pub fn resolve_value(key: &str, value: &str) -> Result<u64> {
    if value.trim().is_empty() {
        return product_default(key).ok_or_else(|| {
            Error::InvalidArgument(format!("unknown broker config key: {key}"))
        });
    }
    parse_u64(value, key)
}
