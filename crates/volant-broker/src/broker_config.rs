//! Broker-level config keys for Kafka Describe/AlterConfigs (Phase 99–102).
//!
//! Phase 99: process-local knobs via Describe/Alter on BROKER resources.
//! Phase 100: durable file under `{data_dir}/__broker_config/state.json`.
//! Phase 102: **sparse** durable overlay — only keys explicitly altered are
//! written; DELETE removes the key so env can re-apply on next restart.
//!
//! Precedence (load): product default → env at construction → sparse durable
//! file (keys present only) → runtime alter. Not a full Kafka
//! DynamicBrokerConfig surface.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
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

/// On-disk schema version for durable broker config (Phase 100).
pub const BROKER_CONFIG_FILE_VERSION: u32 = 1;

/// Directory name under `data_dir` for durable broker knobs.
pub const BROKER_CONFIG_DIR: &str = "__broker_config";

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
            Some("Background open/prepared/session sweep interval in ms; 0 pauses sweeper (lazy expire remains)")
        }
        _ => None,
    }
}

/// Product default for DELETE / empty Alter value restore.
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

/// On-disk durable broker config overlay (Phase 100 file; Phase 102 sparse).
///
/// `configs` holds **only** keys that were explicitly SET via Alter /
/// IncrementalAlter. Missing keys are not frozen — load leaves product→env.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerConfigFile {
    /// Schema version (currently 1).
    #[serde(default = "default_file_version")]
    pub version: u32,
    /// Wire-name → value for **explicitly altered** knobs. Unknown keys ignored
    /// on load. Empty map ≡ no durable overlay.
    #[serde(default)]
    pub configs: HashMap<String, u64>,
}

fn default_file_version() -> u32 {
    BROKER_CONFIG_FILE_VERSION
}

impl Default for BrokerConfigFile {
    fn default() -> Self {
        Self {
            version: BROKER_CONFIG_FILE_VERSION,
            configs: HashMap::new(),
        }
    }
}

impl BrokerConfigFile {
    /// Build a sparse overlay from `(key, decimal_string)` pairs.
    ///
    /// Unknown keys skipped. Empty values omit the key (same as DELETE on an
    /// empty map). Prefer [`Self::apply_alter`] when merging into an existing
    /// overlay.
    pub fn from_entries(entries: &[(String, String)]) -> Result<Self> {
        let mut file = Self::default();
        file.apply_alter(entries)?;
        Ok(file)
    }

    /// Build from live u64 values keyed by wire name.
    pub fn from_values(values: &[(String, u64)]) -> Self {
        let mut configs = HashMap::new();
        for (k, v) in values {
            if is_known_key(k) {
                configs.insert(k.clone(), *v);
            }
        }
        Self {
            version: BROKER_CONFIG_FILE_VERSION,
            configs,
        }
    }

    /// Merge Alter / IncrementalAlter entries into this sparse overlay (Phase 102).
    ///
    /// - Non-empty value → SET key in map
    /// - Empty / whitespace value → DELETE key from map (env may re-apply next boot)
    /// - Unknown keys skipped (caller should validate first)
    pub fn apply_alter(&mut self, entries: &[(String, String)]) -> Result<()> {
        self.version = BROKER_CONFIG_FILE_VERSION;
        for (k, v) in entries {
            if !is_known_key(k) {
                continue;
            }
            if v.trim().is_empty() {
                self.configs.remove(k);
            } else {
                self.configs.insert(k.clone(), parse_u64(v, k)?);
            }
        }
        Ok(())
    }

    /// Whether the overlay has no durable keys.
    pub fn is_empty(&self) -> bool {
        self.configs.is_empty()
    }
}

/// File-backed durable broker config store (Phase 100–102).
///
/// Layout: `{data_dir}/__broker_config/state.json` (atomic replace on write).
/// Phase 102: sparse overlay; empty configs → file removed.
#[derive(Debug)]
pub struct BrokerConfigStore {
    path: PathBuf,
}

impl BrokerConfigStore {
    /// Open (or create dir) under `data_dir/__broker_config`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join(BROKER_CONFIG_DIR);
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "create broker config dir {}: {e}",
                dir.display()
            ))
        })?;
        Ok(Self {
            path: dir.join("state.json"),
        })
    }

    /// Path to `state.json`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load sparse overlay; `None` if file missing or empty.
    pub fn load(&self) -> Result<Option<BrokerConfigFile>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!(
                "open broker config {}: {e}",
                self.path.display()
            ))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).map_err(|e| {
            Error::Storage(format!("read broker config: {e}"))
        })?;
        if buf.trim().is_empty() {
            return Ok(None);
        }
        let file: BrokerConfigFile = serde_json::from_str(&buf).map_err(|e| {
            Error::Storage(format!("parse broker config: {e}"))
        })?;
        if file.configs.is_empty() {
            return Ok(None);
        }
        Ok(Some(file))
    }

    /// Atomically persist sparse overlay (write temp + fsync + rename).
    pub fn save(&self, state: &BrokerConfigFile) -> Result<()> {
        if state.configs.is_empty() {
            return self.clear();
        }
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|e| {
            Error::Storage(format!(
                "create broker config dir {}: {e}",
                parent.display()
            ))
        })?;
        let tmp = parent.join("state.json.tmp");
        let json = serde_json::to_string_pretty(state).map_err(|e| {
            Error::Storage(format!("encode broker config: {e}"))
        })?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open broker config tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write broker config: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync broker config: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!("rename broker config: {e}"))
        })?;
        Ok(())
    }

    /// Remove durable file if present (empty overlay / all keys deleted).
    pub fn clear(&self) -> Result<()> {
        if self.path.exists() {
            fs::remove_file(&self.path).map_err(|e| {
                Error::Storage(format!(
                    "remove broker config {}: {e}",
                    self.path.display()
                ))
            })?;
        }
        // Best-effort remove leftover tmp.
        if let Some(parent) = self.path.parent() {
            let tmp = parent.join("state.json.tmp");
            let _ = fs::remove_file(tmp);
        }
        Ok(())
    }

    /// Load-or-default, apply alter entries, save or clear (Phase 102).
    pub fn merge_alter(&self, entries: &[(String, String)]) -> Result<()> {
        let mut file = self.load()?.unwrap_or_default();
        file.apply_alter(entries)?;
        self.save(&file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "volant-broker-cfg-{}-{}",
            std::process::id(),
            nanos
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = temp_dir();
        let store = BrokerConfigStore::open(&dir).unwrap();
        assert!(store.load().unwrap().is_none());

        let mut file = BrokerConfigFile::default();
        file.configs
            .insert(KEY_TRANSACTION_MAX_TIMEOUT_MS.into(), 123_456);
        file.configs.insert(KEY_SWEEP_INTERVAL_MS.into(), 50);
        store.save(&file).unwrap();

        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.version, BROKER_CONFIG_FILE_VERSION);
        assert_eq!(
            loaded.configs.get(KEY_TRANSACTION_MAX_TIMEOUT_MS),
            Some(&123_456)
        );
        assert_eq!(loaded.configs.get(KEY_SWEEP_INTERVAL_MS), Some(&50));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn from_entries_skips_unknown() {
        let entries = vec![
            (KEY_OPEN_TXN_TIMEOUT_MS.into(), "7000".into()),
            ("log.retention.ms".into(), "1".into()),
        ];
        let file = BrokerConfigFile::from_entries(&entries).unwrap();
        assert_eq!(file.configs.len(), 1);
        assert_eq!(file.configs.get(KEY_OPEN_TXN_TIMEOUT_MS), Some(&7_000));
    }

    #[test]
    fn apply_alter_sparse_set_and_delete() {
        let mut file = BrokerConfigFile::default();
        file.apply_alter(&[(KEY_SWEEP_INTERVAL_MS.into(), "50".into())])
            .unwrap();
        assert_eq!(file.configs.len(), 1);
        assert_eq!(file.configs.get(KEY_SWEEP_INTERVAL_MS), Some(&50));

        // SET another key — still sparse (not full six)
        file.apply_alter(&[(KEY_OPEN_TXN_TIMEOUT_MS.into(), "7000".into())])
            .unwrap();
        assert_eq!(file.configs.len(), 2);

        // DELETE first key
        file.apply_alter(&[(KEY_SWEEP_INTERVAL_MS.into(), "".into())])
            .unwrap();
        assert_eq!(file.configs.len(), 1);
        assert!(!file.configs.contains_key(KEY_SWEEP_INTERVAL_MS));
        assert_eq!(file.configs.get(KEY_OPEN_TXN_TIMEOUT_MS), Some(&7_000));
    }

    #[test]
    fn merge_alter_clears_file_when_empty() {
        let dir = temp_dir();
        let store = BrokerConfigStore::open(&dir).unwrap();
        store
            .merge_alter(&[(KEY_SWEEP_INTERVAL_MS.into(), "77".into())])
            .unwrap();
        assert!(store.path().exists());
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.configs.len(), 1);

        store
            .merge_alter(&[(KEY_SWEEP_INTERVAL_MS.into(), "".into())])
            .unwrap();
        assert!(!store.path().exists(), "empty overlay removes state.json");
        assert!(store.load().unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
