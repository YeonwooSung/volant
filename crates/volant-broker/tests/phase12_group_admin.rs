//! Phase 12: ListGroups, DeleteOffsets, static membership.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, static_member_id, Broker};
use volant_client::Client;
use volant_protocol::{GroupState, OffsetCommitEntry, OffsetEntry};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p12-{label}-{}-{}",
        std::process::id(),
        nanos
    ))
}

async fn start_broker(dir: std::path::PathBuf) -> (String, Arc<Broker>) {
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        ..StorageConfig::default()
    }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = serve_listener(listener, b).await;
    });
    (format!("127.0.0.1:{}", addr.port()), broker)
}

#[tokio::test]
async fn list_groups_live_and_empty() {
    let dir = temp_dir("list");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.unwrap();
    client.create_topic("events", 2).await.unwrap();

    // Offset-only group
    client
        .commit_offsets(
            "g-empty",
            "",
            0,
            vec![OffsetCommitEntry {
                topic: "events".into(),
                partition: 0,
                offset: 3,
                metadata: String::new(),
            }],
        )
        .await
        .unwrap();

    // Live group
    let join = client
        .join_group("g-live", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert!(!join.member_id.is_empty());

    let list = client.list_groups().await.unwrap();
    let empty = list.iter().find(|g| g.group_id == "g-empty").unwrap();
    assert_eq!(empty.state, GroupState::Empty);
    assert_eq!(empty.member_count, 0);

    let live = list.iter().find(|g| g.group_id == "g-live").unwrap();
    assert_eq!(live.state, GroupState::CompletingRebalance);
    assert_eq!(live.member_count, 1);
    assert!(live.generation >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_offsets_resets_commits() {
    let dir = temp_dir("del");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.unwrap();
    client.create_topic("events", 1).await.unwrap();
    client
        .commit_offsets(
            "g1",
            "",
            0,
            vec![
                OffsetCommitEntry {
                    topic: "events".into(),
                    partition: 0,
                    offset: 10,
                    metadata: String::new(),
                },
            ],
        )
        .await
        .unwrap();

    let fetched = client
        .fetch_offsets(
            "g1",
            vec![OffsetEntry {
                topic: "events".into(),
                partition: 0,
            }],
        )
        .await
        .unwrap();
    assert_eq!(fetched[0].offset, 10);

    let del = client
        .delete_offsets(
            "g1",
            vec![OffsetEntry {
                topic: "events".into(),
                partition: 0,
            }],
        )
        .await
        .unwrap();
    assert_eq!(del.deleted_count, 1);

    let fetched = client
        .fetch_offsets(
            "g1",
            vec![OffsetEntry {
                topic: "events".into(),
                partition: 0,
            }],
        )
        .await
        .unwrap();
    assert_eq!(fetched[0].offset, u64::MAX);

    // delete-all path
    client
        .commit_offsets(
            "g2",
            "",
            0,
            vec![OffsetCommitEntry {
                topic: "events".into(),
                partition: 0,
                offset: 1,
                metadata: String::new(),
            }],
        )
        .await
        .unwrap();
    let del_all = client.delete_offsets("g2", vec![]).await.unwrap();
    assert_eq!(del_all.deleted_count, 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn static_membership_stable_member_id() {
    let dir = temp_dir("static");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.unwrap();
    client.create_topic("events", 4).await.unwrap();

    let j1 = client
        .join_group_with_instance("cg", "", 10_000, vec!["events".into()], "pod-a")
        .await
        .unwrap();
    assert_eq!(j1.member_id, static_member_id("pod-a"));
    let gen1 = j1.generation;
    let parts1: Vec<u32> = j1.assignment.iter().map(|a| a.partition).collect();

    // Re-join same instance without leaving — no generation bump, same assignment.
    let j2 = client
        .join_group_with_instance(
            "cg",
            &j1.member_id,
            10_000,
            vec!["events".into()],
            "pod-a",
        )
        .await
        .unwrap();
    assert_eq!(j2.member_id, j1.member_id);
    assert_eq!(j2.generation, gen1);
    let parts2: Vec<u32> = j2.assignment.iter().map(|a| a.partition).collect();
    assert_eq!(parts1, parts2);

    // Fresh client with only instance id (empty member_id) resolves same static id.
    let c2 = Client::connect_addr(&addr).await.unwrap();
    let j3 = c2
        .join_group_with_instance("cg", "", 10_000, vec!["events".into()], "pod-a")
        .await
        .unwrap();
    assert_eq!(j3.member_id, static_member_id("pod-a"));
    assert_eq!(j3.generation, gen1);

    let _ = std::fs::remove_dir_all(&dir);
}
