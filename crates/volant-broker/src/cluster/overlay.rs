//! Persisted dynamic membership overlay (v0.10).
//!
//! Layout: `{data_dir}/cluster/membership.json`
//!
//! When the file exists it is the membership source of truth (replaces
//! `cluster.toml` brokers for live membership, majority N, and `broker_addr`).
//! Absent → use toml. First successful add/remove seeds the file from the
//! current effective list.
//!
//! When `VOLANT_OPENRAFT_METADATA` is on (N≥2), the file is also the
//! **apply artifact** of openraft `EntryPayload::Membership` (v0.216):
//! followers and snapshot-install write it from voter ids + `BasicNode.addr`.
//! `MembershipPut` stays best-effort catch-up, not SoT. Flag off / N<2
//! keep this file as the v0.10 overlay SoT.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use volant_core::{Error, Result};

use super::config::BrokerEndpoint;

/// On-disk filename under `{data_dir}/cluster/`.
pub const MEMBERSHIP_OVERLAY_FILE: &str = "membership.json";

/// Persisted membership overlay (generation + broker list).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipOverlay {
    /// Monotonic generation. First write is `1`; peers apply only if `> local`.
    pub generation: u64,
    /// Effective broker endpoints (replaces `cluster.toml` `[[brokers]]`).
    pub brokers: Vec<BrokerEndpoint>,
}

/// Path to `{data_dir}/cluster/membership.json`.
pub fn membership_overlay_path(data_dir: impl AsRef<Path>) -> PathBuf {
    data_dir
        .as_ref()
        .join("cluster")
        .join(MEMBERSHIP_OVERLAY_FILE)
}

/// Validate overlay: at least one broker, unique ids.
pub fn validate_membership_overlay(overlay: &MembershipOverlay) -> Result<()> {
    if overlay.brokers.is_empty() {
        return Err(Error::InvalidArgument(
            "membership overlay must list at least one broker".into(),
        ));
    }
    let mut ids: Vec<u32> = overlay.brokers.iter().map(|b| b.id).collect();
    ids.sort_unstable();
    for w in ids.windows(2) {
        if w[0] == w[1] {
            return Err(Error::InvalidArgument(format!(
                "duplicate broker id {} in membership overlay",
                w[0]
            )));
        }
    }
    Ok(())
}

/// Load overlay if the file exists. `Ok(None)` when absent.
pub fn load_membership_overlay(data_dir: impl AsRef<Path>) -> Result<Option<MembershipOverlay>> {
    let path = membership_overlay_path(data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = File::open(&path)
        .map_err(|e| Error::Storage(format!("open membership overlay {}: {e}", path.display())))?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)
        .map_err(|e| Error::Storage(format!("read membership overlay: {e}")))?;
    if buf.trim().is_empty() {
        return Ok(None);
    }
    let overlay: MembershipOverlay = serde_json::from_str(&buf)
        .map_err(|e| Error::Storage(format!("parse membership overlay: {e}")))?;
    validate_membership_overlay(&overlay)?;
    Ok(Some(overlay))
}

/// Atomically persist overlay (tmp + fsync + rename).
pub fn save_membership_overlay(
    data_dir: impl AsRef<Path>,
    overlay: &MembershipOverlay,
) -> Result<()> {
    validate_membership_overlay(overlay)?;
    let path = membership_overlay_path(&data_dir);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|e| Error::Storage(format!("create cluster dir {}: {e}", parent.display())))?;
    let tmp = parent.join(format!("{MEMBERSHIP_OVERLAY_FILE}.tmp"));
    let json = serde_json::to_string_pretty(overlay)
        .map_err(|e| Error::Storage(format!("encode membership overlay: {e}")))?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| Error::Storage(format!("open membership overlay tmp: {e}")))?;
        f.write_all(json.as_bytes())
            .map_err(|e| Error::Storage(format!("write membership overlay: {e}")))?;
        f.sync_all()
            .map_err(|e| Error::Storage(format!("fsync membership overlay: {e}")))?;
    }
    fs::rename(&tmp, &path)
        .map_err(|e| Error::Storage(format!("rename membership overlay: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn roundtrip_overlay() {
        let dir = env::temp_dir().join(format!(
            "volant-membership-overlay-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        let overlay = MembershipOverlay {
            generation: 2,
            brokers: vec![
                BrokerEndpoint {
                    id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                },
                BrokerEndpoint {
                    id: 2,
                    host: "127.0.0.1".into(),
                    port: 9093,
                    rack: Some("r1".into()),
                },
            ],
        };
        save_membership_overlay(&dir, &overlay).unwrap();
        let loaded = load_membership_overlay(&dir).unwrap().expect("present");
        assert_eq!(loaded, overlay);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_overlay_is_none() {
        let dir = env::temp_dir().join(format!(
            "volant-membership-missing-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        assert!(load_membership_overlay(&dir).unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }
}
