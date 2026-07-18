//! Durable per-topic configuration (Phase 13).
//!
//! Layout: `{data_dir}/__topic_configs/{topic}.json`

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// Config key: time-based retention in milliseconds.
pub const KEY_RETENTION_MS: &str = "retention.ms";
/// Config key: size-based retention in bytes.
pub const KEY_RETENTION_BYTES: &str = "retention.bytes";
/// Config key: target segment roll size in bytes.
pub const KEY_SEGMENT_BYTES: &str = "segment.bytes";
/// Config key: log cleanup policy (`delete` or `compact`).
pub const KEY_CLEANUP_POLICY: &str = "cleanup.policy";

/// Per-topic log/retention settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicConfig {
    /// Drop sealed segments older than this many ms (`None` = disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<u64>,
    /// Drop oldest sealed segments until total size ≤ this (`None` = disabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_bytes: Option<u64>,
    /// Target segment roll size in bytes (`None` = broker default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_bytes: Option<u64>,
    /// When true, compact sealed segments by key (Phase 16). Default delete-only.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub compact: bool,
}

impl TopicConfig {
    /// Parse from key/value pairs. Empty value clears that key.
    pub fn from_entries(entries: &[(String, String)]) -> Result<Self> {
        let mut cfg = Self::default();
        cfg.apply_entries(entries)?;
        Ok(cfg)
    }

    /// Merge key/value updates into this config. Empty value clears.
    pub fn apply_entries(&mut self, entries: &[(String, String)]) -> Result<()> {
        for (k, v) in entries {
            match k.as_str() {
                KEY_RETENTION_MS => {
                    self.retention_ms = parse_opt_u64(v, KEY_RETENTION_MS)?;
                }
                KEY_RETENTION_BYTES => {
                    self.retention_bytes = parse_opt_u64(v, KEY_RETENTION_BYTES)?;
                }
                KEY_SEGMENT_BYTES => {
                    self.segment_bytes = parse_opt_u64(v, KEY_SEGMENT_BYTES)?;
                }
                KEY_CLEANUP_POLICY => {
                    self.compact = parse_cleanup_policy(v)?;
                }
                other => {
                    return Err(Error::InvalidArgument(format!(
                        "unknown topic config key: {other}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Flatten to sorted key/value list for DescribeConfigs.
    pub fn to_entries(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        out.push((
            KEY_RETENTION_MS.into(),
            self.retention_ms
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ));
        out.push((
            KEY_RETENTION_BYTES.into(),
            self.retention_bytes
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ));
        out.push((
            KEY_SEGMENT_BYTES.into(),
            self.segment_bytes
                .map(|v| v.to_string())
                .unwrap_or_default(),
        ));
        out.push((
            KEY_CLEANUP_POLICY.into(),
            if self.compact {
                "compact".into()
            } else {
                "delete".into()
            },
        ));
        out
    }
}

fn parse_cleanup_policy(v: &str) -> Result<bool> {
    let t = v.trim().to_ascii_lowercase();
    if t.is_empty() || t == "delete" {
        return Ok(false);
    }
    if t == "compact" {
        return Ok(true);
    }
    Err(Error::InvalidArgument(format!(
        "invalid cleanup.policy value: {v} (want delete|compact)"
    )))
}

fn parse_opt_u64(v: &str, key: &str) -> Result<Option<u64>> {
    let t = v.trim();
    if t.is_empty() {
        return Ok(None);
    }
    t.parse::<u64>()
        .map(Some)
        .map_err(|_| Error::InvalidArgument(format!("invalid {key} value: {v}")))
}

/// File-backed topic config map.
#[derive(Debug)]
pub struct TopicConfigStore {
    dir: PathBuf,
}

impl TopicConfigStore {
    /// Open (or create) store under `data_dir/__topic_configs`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join("__topic_configs");
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!("create topic config dir {}: {e}", dir.display()))
        })?;
        Ok(Self { dir })
    }

    fn path_for(&self, topic: &str) -> PathBuf {
        self.dir.join(format!("{}.json", sanitize(topic)))
    }

    /// Load config for a topic; defaults if missing.
    pub fn load(&self, topic: &str) -> Result<TopicConfig> {
        let path = self.path_for(topic);
        if !path.exists() {
            return Ok(TopicConfig::default());
        }
        let mut f = File::open(&path).map_err(|e| {
            Error::Storage(format!("open topic config {}: {e}", path.display()))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)
            .map_err(|e| Error::Storage(format!("read topic config: {e}")))?;
        if buf.trim().is_empty() {
            return Ok(TopicConfig::default());
        }
        serde_json::from_str(&buf)
            .map_err(|e| Error::Storage(format!("parse topic config: {e}")))
    }

    /// Persist config (atomic replace).
    pub fn save(&self, topic: &str, cfg: &TopicConfig) -> Result<()> {
        // Re-ensure parent exists (defense vs external delete / test isolation).
        fs::create_dir_all(&self.dir).map_err(|e| {
            Error::Storage(format!(
                "create topic config dir {}: {e}",
                self.dir.display()
            ))
        })?;
        let path = self.path_for(topic);
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(cfg)
            .map_err(|e| Error::Storage(format!("encode topic config: {e}")))?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open topic config tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write topic config: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync topic config: {e}")))?;
        }
        fs::rename(&tmp, &path)
            .map_err(|e| Error::Storage(format!("rename topic config: {e}")))?;
        Ok(())
    }

    /// Remove durable config when topic is deleted.
    pub fn delete(&self, topic: &str) -> Result<()> {
        let path = self.path_for(topic);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                Error::Storage(format!("delete topic config {}: {e}", path.display()))
            })?;
        }
        Ok(())
    }

    /// Load all configs (topic name as stored filename stem — sanitized).
    pub fn load_all(&self) -> Result<HashMap<String, TopicConfig>> {
        let mut out = HashMap::new();
        if !self.dir.exists() {
            return Ok(out);
        }
        for ent in fs::read_dir(&self.dir)
            .map_err(|e| Error::Storage(format!("list topic configs: {e}")))?
        {
            let ent = ent.map_err(|e| Error::Storage(e.to_string()))?;
            let name = ent.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".json") || name.ends_with(".tmp") {
                continue;
            }
            let stem = name.trim_end_matches(".json").to_owned();
            // We store sanitized names; callers match against live topic names.
            let cfg = self.load(&stem)?;
            out.insert(stem, cfg);
        }
        Ok(out)
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
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
        std::env::temp_dir().join(format!(
            "volant-tcfg-{}-{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn roundtrip_and_entries() {
        let dir = temp_dir();
        let _ = fs::remove_dir_all(&dir);
        let store = TopicConfigStore::open(&dir).unwrap();
        let mut cfg = TopicConfig::default();
        cfg.apply_entries(&[
            (KEY_RETENTION_MS.into(), "1000".into()),
            (KEY_SEGMENT_BYTES.into(), "4096".into()),
        ])
        .unwrap();
        store.save("events", &cfg).unwrap();
        let loaded = store.load("events").unwrap();
        assert_eq!(loaded.retention_ms, Some(1000));
        assert_eq!(loaded.segment_bytes, Some(4096));
        assert!(loaded.retention_bytes.is_none());

        let mut cfg2 = loaded;
        cfg2.apply_entries(&[(KEY_RETENTION_MS.into(), "".into())])
            .unwrap();
        assert!(cfg2.retention_ms.is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
