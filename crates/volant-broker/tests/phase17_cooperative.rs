//! Phase 17: cooperative rebalance — revoked list + position handoff.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, Broker};
use volant_client::{Client, ClientConfig, GroupConsumer};
use volant_core::Message;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p17-{label}-{}-{}",
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
async fn join_group_revoked_on_resync_after_peer_join() {
    let dir = temp_dir("revoked");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let admin = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 4).await.unwrap();

    let c1 = Client::connect_addr(&addr).await.unwrap();
    let j1 = c1
        .join_group("cg-coop", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert_eq!(j1.assignment.len(), 4);
    assert!(j1.revoked.is_empty());
    let first: HashSet<u32> = j1.assignment.iter().map(|a| a.partition).collect();
    c1.sync_group("cg-coop", &j1.member_id, j1.generation)
        .await
        .unwrap();

    let c2 = Client::connect_addr(&addr).await.unwrap();
    let j2 = c2
        .join_group("cg-coop", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert!(j2.revoked.is_empty());

    let j1b = c1
        .join_group(
            "cg-coop",
            &j1.member_id,
            10_000,
            vec!["events".into()],
        )
        .await
        .unwrap();
    let now: HashSet<u32> = j1b.assignment.iter().map(|a| a.partition).collect();
    let revoked: HashSet<u32> = j1b.revoked.iter().map(|a| a.partition).collect();
    let expected: HashSet<u32> = first.difference(&now).copied().collect();
    assert_eq!(revoked, expected);
    assert!(!j1b.revoked.is_empty());
    assert!(now.is_subset(&first), "sticky retain subset");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn group_consumer_retains_positions_on_sticky_partitions() {
    let dir = temp_dir("positions");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let admin = Client::connect_addr(&addr).await.unwrap();
    admin.create_topic("events", 4).await.unwrap();

    // One message per partition so solo consumer advances all positions.
    for p in 0..4u32 {
        admin
            .produce(
                "events",
                Some(p),
                vec![Message::from_value(Bytes::from(format!("p{p}")))],
            )
            .await
            .unwrap();
    }

    let c1 = Arc::new(Client::connect_addr(&addr).await.unwrap());
    let mut g1 = GroupConsumer::join(Arc::clone(&c1), "cg-pos", vec!["events".into()], 10_000)
        .await
        .unwrap();
    assert_eq!(g1.assignment().len(), 4);

    // Poll until all four messages are seen; positions should be 1 on each.
    let mut seen = HashSet::new();
    for _ in 0..8 {
        for r in g1.poll().await.unwrap() {
            seen.insert(r.partition);
        }
        if seen.len() == 4 {
            break;
        }
    }
    assert_eq!(seen.len(), 4);
    let snapshot: HashMap<(String, u32), u64> = g1.positions().clone();
    for p in 0..4u32 {
        assert_eq!(
            snapshot.get(&("events".into(), p)),
            Some(&1),
            "expected position 1 on partition {p}"
        );
    }

    // Second member joins → rebalance.
    let c2 = Arc::new(Client::connect_addr(&addr).await.unwrap());
    let g2 = GroupConsumer::join(c2, "cg-pos", vec!["events".into()], 10_000)
        .await
        .unwrap();
    assert!(!g2.assignment().is_empty());

    // g1 heartbeat/poll triggers cooperative re-join.
    let _ = g1.poll().await.unwrap();

    let retained: Vec<_> = g1
        .assignment()
        .iter()
        .cloned()
        .collect();
    assert!(!retained.is_empty());
    assert!(
        !g1.last_revoked().is_empty(),
        "expected some revoked partitions after split"
    );

    // Sticky-retained partitions must keep in-memory positions (not reset to 0).
    for tp in &retained {
        assert_eq!(
            g1.positions().get(tp),
            snapshot.get(tp),
            "retained partition {tp:?} should keep position; snapshot={snapshot:?} now={:?}",
            g1.positions()
        );
    }

    // Revoked partitions must not appear in positions.
    for tp in g1.last_revoked() {
        assert!(
            !g1.positions().contains_key(&tp),
            "revoked {tp:?} should drop position"
        );
    }

    g1.leave().await.unwrap();
    g2.leave().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
