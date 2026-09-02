//! Opt-in KRaft-shaped `__cluster_metadata` topic (v0.12).
//!
//! Local + ISR-replicated assignment snapshots. **Not** a Raft metadata log and
//! **not** Kafka KRaft record schemas.

use std::path::Path;

use volant_core::Offset;
use volant_storage::{PartitionLog, StorageConfig};

use super::state::AssignmentSnapshot;

/// Internal single-partition assignment snapshot topic.
pub const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";
/// Optional produce header name (`"1"` = this MVP format).
pub const CLUSTER_METADATA_HEADER: &str = "volant-cluster-metadata";
/// Header value for format version 1.
pub const CLUSTER_METADATA_HEADER_VALUE: &[u8] = b"1";

/// Replica set for `__cluster_metadata-0`: lowest ids, RF = min(3, N).
///
/// Leader is the first replica (lowest id) so the controller can produce.
pub fn cluster_metadata_replicas(broker_ids: &[u32]) -> Vec<u32> {
    let mut ids = broker_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let rf = 3.min(ids.len()).max(1);
    ids.into_iter().take(rf).collect()
}

/// Whether `VOLANT_CLUSTER_METADATA_TOPIC` is on (`1`/`true`/`yes`).
///
/// Default **off**.
pub fn cluster_metadata_topic_env_enabled() -> bool {
    match std::env::var("VOLANT_CLUSTER_METADATA_TOPIC") {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

/// Rebuild assignment from the last `__cluster_metadata-0` record, if any.
///
/// Opens the partition log under `{data_dir}/__cluster_metadata/0` without
/// creating it. Last-write-wins JSON [`AssignmentSnapshot`].
pub fn load_assignment_from_cluster_metadata(data_dir: &Path) -> Option<AssignmentSnapshot> {
    let log_dir = data_dir.join(CLUSTER_METADATA_TOPIC).join("0");
    if !log_dir.exists() {
        return None;
    }
    let cfg = StorageConfig {
        data_dir: log_dir,
        flush_every_n: 1,
        ..StorageConfig::default()
    };
    let log = PartitionLog::open(cfg).ok()?;
    let leo = log.log_end_offset().raw();
    if leo == 0 {
        return None;
    }
    let recs = log.read(Offset::ZERO, leo as usize).ok()?;
    let last = recs.last()?;
    serde_json::from_slice(&last.value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_rf_capped() {
        assert_eq!(cluster_metadata_replicas(&[3, 1, 2]), vec![1, 2, 3]);
        assert_eq!(cluster_metadata_replicas(&[7, 2]), vec![2, 7]);
        assert_eq!(cluster_metadata_replicas(&[4]), vec![4]);
        assert_eq!(cluster_metadata_replicas(&[9, 1, 5, 3]), vec![1, 3, 5]);
    }

    #[test]
    fn missing_log_is_none() {
        let dir = std::env::temp_dir().join(format!(
            "volant-cmeta-miss-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(load_assignment_from_cluster_metadata(&dir).is_none());
    }
}
