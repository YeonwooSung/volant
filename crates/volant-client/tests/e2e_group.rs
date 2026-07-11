//! End-to-end consumer group tests over localhost TCP.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, GroupConsumer};
use volant_core::Message;
use volant_protocol::OffsetCommitEntry;
use volant_storage::StorageConfig;

fn temp_data_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-e2e-group-{}-{}-{}",
        label,
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn boot_server(data_dir: std::path::PathBuf) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir,
        ..StorageConfig::default()
    }));
    let handle = tokio::spawn(async move {
        let _ = serve_listener(listener, broker).await;
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test]
async fn two_consumers_disjoint_partition_cover() {
    let dir = temp_data_dir("split");
    let (addr, server) = boot_server(dir.clone()).await;

    let admin = Client::connect_addr(&addr).await.expect("connect admin");
    admin
        .create_topic("events", 4)
        .await
        .expect("create topic");

    // Produce one message per partition.
    for p in 0..4u32 {
        admin
            .produce(
                "events",
                Some(p),
                vec![Message::from_value(Bytes::from(format!("p{p}")))],
            )
            .await
            .expect("produce");
    }

    let c1 = Arc::new(Client::connect_addr(&addr).await.expect("c1"));
    let c2 = Arc::new(Client::connect_addr(&addr).await.expect("c2"));

    let mut g1 = GroupConsumer::join(c1, "cg-split", vec!["events".into()], 10_000)
        .await
        .expect("join g1");
    let mut g2 = GroupConsumer::join(c2, "cg-split", vec!["events".into()], 10_000)
        .await
        .expect("join g2");

    // After g2 joins, g1's local assignment is stale until rebalance via heartbeat/poll.
    // Collect records from all polls (first poll also fetches after re-join).
    let mut seen = HashSet::new();
    for _ in 0..8 {
        for g in [&mut g1, &mut g2] {
            for r in g.poll().await.expect("poll") {
                seen.insert((r.partition, r.record.offset));
            }
        }
    }

    let a1: HashSet<(String, u32)> = g1.assignment().iter().cloned().collect();
    let a2: HashSet<(String, u32)> = g2.assignment().iter().cloned().collect();
    assert!(
        a1.is_disjoint(&a2),
        "assignments must be disjoint: {a1:?} vs {a2:?}"
    );
    let union: HashSet<_> = a1.union(&a2).cloned().collect();
    let expected: HashSet<_> = (0..4u32).map(|p| ("events".into(), p)).collect();
    assert_eq!(union, expected, "must cover all partitions");

    assert_eq!(seen.len(), 4, "expected 4 messages, got {seen:?}");

    g1.commit().await.expect("commit g1");
    g2.commit().await.expect("commit g2");
    g1.leave().await.expect("leave g1");
    g2.leave().await.expect("leave g2");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn commit_and_resume_from_committed_offset() {
    let dir = temp_data_dir("resume");
    let (addr, server) = boot_server(dir.clone()).await;

    let admin = Client::connect_addr(&addr).await.expect("connect");
    admin.create_topic("t", 2).await.expect("create");

    // Produce 3 messages on partition 0.
    for i in 0..3u32 {
        admin
            .produce(
                "t",
                Some(0),
                vec![Message::from_value(Bytes::from(format!("m{i}")))],
            )
            .await
            .expect("produce");
    }
    // One on partition 1.
    admin
        .produce(
            "t",
            Some(1),
            vec![Message::from_value(Bytes::from_static(b"other"))],
        )
        .await
        .expect("produce p1");

    // First consumer: join, poll, commit positions, leave.
    {
        let client = Arc::new(Client::connect_addr(&addr).await.expect("c1"));
        let mut g = GroupConsumer::join(client, "cg-resume", vec!["t".into()], 10_000)
            .await
            .expect("join");
        let mut count = 0;
        for _ in 0..5 {
            let recs = g.poll().await.expect("poll");
            count += recs.len();
            if count >= 4 {
                break;
            }
        }
        assert!(count >= 4, "first consumer should see all messages");
        g.commit().await.expect("commit");
        g.leave().await.expect("leave");
    }

    // Verify committed offsets via admin fetch.
    let admin2 = Client::connect_addr(&addr).await.expect("admin2");
    let offs = admin2
        .fetch_offsets("cg-resume", vec![])
        .await
        .expect("fetch_offsets");
    assert!(
        offs.iter().any(|e| e.topic == "t" && e.partition == 0 && e.offset >= 3),
        "expected p0 committed >= 3, got {offs:?}"
    );

    // Produce more on p0.
    admin2
        .produce(
            "t",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"new"))],
        )
        .await
        .expect("produce new");

    // New consumer resumes — should only see the new message on p0 (and nothing old).
    {
        let client = Arc::new(Client::connect_addr(&addr).await.expect("c2"));
        let mut g = GroupConsumer::join(client, "cg-resume", vec!["t".into()], 10_000)
            .await
            .expect("rejoin");
        let mut values = Vec::new();
        for _ in 0..5 {
            for r in g.poll().await.expect("poll") {
                values.push(String::from_utf8_lossy(&r.record.value).into_owned());
            }
        }
        assert!(
            values.contains(&"new".to_string()),
            "resume should see new message, got {values:?}"
        );
        assert!(
            !values.iter().any(|v| v == "m0" || v == "m1" || v == "m2"),
            "should not re-read committed messages, got {values:?}"
        );
        g.commit().await.expect("commit");
        g.leave().await.expect("leave");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn admin_commit_and_fetch_offsets() {
    let dir = temp_data_dir("admin");
    let (addr, server) = boot_server(dir.clone()).await;

    let client = Client::connect_addr(&addr).await.expect("connect");
    client.create_topic("t", 1).await.expect("create");

    // Admin commit (generation=0, empty member).
    client
        .commit_offsets(
            "admin-g",
            "",
            0,
            vec![OffsetCommitEntry {
                topic: "t".into(),
                partition: 0,
                offset: 99,
                metadata: "cli".into(),
            }],
        )
        .await
        .expect("commit");

    let fetched = client
        .fetch_offsets(
            "admin-g",
            vec![volant_protocol::OffsetEntry {
                topic: "t".into(),
                partition: 0,
            }],
        )
        .await
        .expect("fetch");
    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].offset, 99);
    assert_eq!(fetched[0].metadata, "cli");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
