//! Durable cluster admin generations (Phase 117).
//!
//! Layout: `{data_dir}/__cluster_admin/state.json`
//!
//! Stores controller/applied BROKER-config and ACL generation counters so
//! restarts and controller failover do not reset gens to 0 (which would make
//! peers ignore subsequent pushes forever).

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

/// Directory under `data_dir` for durable admin generations.
pub const CLUSTER_ADMIN_DIR: &str = "__cluster_admin";
/// On-disk filename.
pub const CLUSTER_ADMIN_FILE: &str = "state.json";
/// File format version.
pub const CLUSTER_ADMIN_FILE_VERSION: u32 = 1;

/// Durable admin generation snapshot (Phase 117).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ClusterAdminFile {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Controller (or last known) BROKER config generation.
    #[serde(default)]
    pub config_generation: u64,
    /// Last applied BROKER config generation on this node.
    #[serde(default)]
    pub applied_config_generation: u64,
    /// Controller (or last known) ACL generation.
    #[serde(default)]
    pub acl_generation: u64,
    /// Last applied ACL generation on this node.
    #[serde(default)]
    pub applied_acl_generation: u64,
}

fn default_version() -> u32 {
    CLUSTER_ADMIN_FILE_VERSION
}

/// File-backed durable admin generation store.
#[derive(Debug)]
pub struct ClusterAdminStore {
    path: PathBuf,
}

impl ClusterAdminStore {
    /// Open (create dir) under `data_dir/__cluster_admin`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = data_dir.as_ref().join(CLUSTER_ADMIN_DIR);
        fs::create_dir_all(&dir).map_err(|e| {
            Error::Storage(format!(
                "create cluster admin dir {}: {e}",
                dir.display()
            ))
        })?;
        Ok(Self {
            path: dir.join(CLUSTER_ADMIN_FILE),
        })
    }

    /// Path to `state.json`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load snapshot; defaults if missing/empty.
    pub fn load(&self) -> Result<ClusterAdminFile> {
        if !self.path.exists() {
            return Ok(ClusterAdminFile {
                version: CLUSTER_ADMIN_FILE_VERSION,
                ..Default::default()
            });
        }
        let mut f = File::open(&self.path).map_err(|e| {
            Error::Storage(format!(
                "open cluster admin {}: {e}",
                self.path.display()
            ))
        })?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).map_err(|e| {
            Error::Storage(format!("read cluster admin: {e}"))
        })?;
        if buf.trim().is_empty() {
            return Ok(ClusterAdminFile {
                version: CLUSTER_ADMIN_FILE_VERSION,
                ..Default::default()
            });
        }
        let mut file: ClusterAdminFile = serde_json::from_str(&buf).map_err(|e| {
            Error::Storage(format!("parse cluster admin: {e}"))
        })?;
        if file.version == 0 {
            file.version = CLUSTER_ADMIN_FILE_VERSION;
        }
        Ok(file)
    }

    /// Atomically persist snapshot (tmp + fsync + rename).
    pub fn save(&self, state: &ClusterAdminFile) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|e| {
            Error::Storage(format!(
                "create cluster admin dir {}: {e}",
                parent.display()
            ))
        })?;
        let tmp = parent.join(format!("{CLUSTER_ADMIN_FILE}.tmp"));
        let mut out = state.clone();
        out.version = CLUSTER_ADMIN_FILE_VERSION;
        let json = serde_json::to_string_pretty(&out).map_err(|e| {
            Error::Storage(format!("encode cluster admin: {e}"))
        })?;
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|e| Error::Storage(format!("open cluster admin tmp: {e}")))?;
            f.write_all(json.as_bytes())
                .map_err(|e| Error::Storage(format!("write cluster admin: {e}")))?;
            f.sync_all()
                .map_err(|e| Error::Storage(format!("fsync cluster admin: {e}")))?;
        }
        fs::rename(&tmp, &self.path).map_err(|e| {
            Error::Storage(format!("rename cluster admin: {e}"))
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn roundtrip_admin_gens() {
        let dir = env::temp_dir().join(format!(
            "volant-admin-gens-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        let store = ClusterAdminStore::open(&dir).unwrap();
        let s = ClusterAdminFile {
            version: CLUSTER_ADMIN_FILE_VERSION,
            config_generation: 7,
            applied_config_generation: 7,
            acl_generation: 3,
            applied_acl_generation: 2,
        };
        store.save(&s).unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded, s);
        let _ = fs::remove_dir_all(&dir);
    }
}
