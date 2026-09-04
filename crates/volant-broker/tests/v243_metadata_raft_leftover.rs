//! v0.243: leftover `{data_dir}/__metadata_raft` is unread; warn once if present.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use volant_broker::broker::leftover_metadata_raft_warn_count;
use volant_broker::Broker;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-v243-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

fn storage(dir: &Path) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        ..StorageConfig::default()
    }
}

fn leftover_names(data_dir: &Path) -> Vec<String> {
    let leftover = data_dir.join("__metadata_raft");
    if !leftover.exists() {
        return Vec::new();
    }
    let mut names: Vec<String> = fs::read_dir(&leftover)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// Leftover dir absent: `Broker::new` succeeds and does not create the dir.
#[test]
fn leftover_absent_broker_new_ok() {
    let dir = temp_dir("absent");
    fs::create_dir_all(&dir).unwrap();
    let _broker = Broker::new(storage(&dir));
    assert!(
        !dir.join("__metadata_raft").exists(),
        "broker must not create leftover __metadata_raft"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Empty leftover dir present: boot succeeds, dir stays unused, warn once.
#[test]
fn leftover_present_broker_new_ok_unused() {
    let dir = temp_dir("present");
    let leftover = dir.join("__metadata_raft");
    fs::create_dir_all(&leftover).unwrap();
    assert!(leftover_names(&dir).is_empty());

    let before = leftover_metadata_raft_warn_count();
    let _broker = Broker::new(storage(&dir));
    let after = leftover_metadata_raft_warn_count();
    assert!(after >= 1, "leftover dir must warn at least once");
    if before == 0 {
        assert_eq!(after, 1, "first leftover boot warns exactly once");
    } else {
        assert_eq!(after, before, "already warned this process");
    }

    assert!(leftover.is_dir(), "must not delete leftover dir");
    assert!(
        leftover_names(&dir).is_empty(),
        "must not read/migrate leftover dir (no new files)"
    );

    // Second construct in the same process still ok and does not warn again.
    let _broker2 = Broker::new(storage(&dir));
    assert_eq!(leftover_metadata_raft_warn_count(), after);
    assert!(leftover.is_dir());
    assert!(leftover_names(&dir).is_empty());

    let _ = fs::remove_dir_all(&dir);
}

/// Known leftover 154 files are unread: garbage `log.json` is not parsed.
#[test]
fn leftover_known_files_unread() {
    let dir = temp_dir("files");
    let leftover = dir.join("__metadata_raft");
    fs::create_dir_all(&leftover).unwrap();
    let log = leftover.join("log.json");
    let hard = leftover.join("hard_state.json");
    fs::write(&log, b"not-a-raft-log").unwrap();
    fs::write(&hard, b"not-hard-state").unwrap();

    let _broker = Broker::new(storage(&dir));
    assert_eq!(fs::read(&log).unwrap(), b"not-a-raft-log");
    assert_eq!(fs::read(&hard).unwrap(), b"not-hard-state");
    assert!(leftover_metadata_raft_warn_count() >= 1);

    let _ = fs::remove_dir_all(&dir);
}
