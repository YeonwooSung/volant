//! Phase 11: durable producer state across restart, DescribeGroup, sticky rebalance.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::TcpListener;
use volant_broker::{serve_listener, sticky_assign, Broker, IdempotentCheck};
use volant_client::{Client, ClientConfig};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "volant-p11-{label}-{}-{}",
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

#[test]
fn sticky_keeps_owned_partitions() {
    let members = vec!["a".into(), "b".into()];
    let prev = vec![vec![0, 1], vec![2, 3]];
    let next = sticky_assign(4, &members, &prev);
    assert_eq!(next[0], vec![0, 1]);
    assert_eq!(next[1], vec![2, 3]);
}

#[tokio::test]
async fn durable_producer_dedupe_across_restart() {
    let dir = temp_dir("dur");
    let _ = std::fs::remove_dir_all(&dir);

    let (pid, epoch, base_offset) = {
        let broker = Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        });
        let (pid, epoch) = broker.init_producer_id();
        assert!(matches!(
            broker.check_idempotent_produce(pid, epoch, "events", 0, 0, 1),
            IdempotentCheck::Accept
        ));
        broker.record_idempotent_produce(pid, epoch, "events", 0, 0, 1, 7);
        match broker.check_idempotent_produce(pid, epoch, "events", 0, 0, 1) {
            IdempotentCheck::Duplicate {
                base_offset,
                count,
            } => {
                assert_eq!(base_offset, 7);
                assert_eq!(count, 1);
            }
            other => panic!("expected Duplicate before restart, got {other:?}"),
        }
        (pid, epoch, 7u64)
    };

    // Re-open broker on same data_dir — PID map must reload.
    let broker2 = Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    });
    match broker2.check_idempotent_produce(pid, epoch, "events", 0, 0, 1) {
        IdempotentCheck::Duplicate {
            base_offset: bo,
            count,
        } => {
            assert_eq!(bo, base_offset);
            assert_eq!(count, 1);
        }
        other => panic!("expected Duplicate after restart, got {other:?}"),
    }
    // Next sequence still accepted.
    assert!(matches!(
        broker2.check_idempotent_produce(pid, epoch, "events", 0, 1, 1),
        IdempotentCheck::Accept
    ));
    // New init continues past previous id.
    let (pid2, _) = broker2.init_producer_id();
    assert!(pid2 > pid);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn describe_group_shows_members_and_assignment() {
    let dir = temp_dir("desc");
    let _ = std::fs::remove_dir_all(&dir);
    let (addr, _broker) = start_broker(dir.clone()).await;

    let admin = Client::connect_addr(&addr).await.unwrap();
    admin.create_topic("events", 4).await.unwrap();

    let c1 = Client::connect_addr(&addr).await.unwrap();
    let join = c1
        .join_group("cg-desc", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    assert!(!join.assignment.is_empty());

    let desc = admin.describe_group("cg-desc").await.unwrap();
    assert_eq!(desc.group_id, "cg-desc");
    assert_eq!(desc.generation, join.generation);
    assert_eq!(desc.members.len(), 1);
    assert_eq!(desc.members[0].member_id, join.member_id);
    assert!(desc.members[0].topics.contains(&"events".into()));
    assert_eq!(desc.members[0].assignment.len(), join.assignment.len());

    // Unknown group → NotFound
    assert!(admin.describe_group("no-such-group").await.is_err());

    // Empty group after leave
    c1.leave_group("cg-desc", &join.member_id).await.unwrap();
    assert!(admin.describe_group("cg-desc").await.is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sticky_rebalance_via_group_coordinator() {
    let dir = temp_dir("sticky");
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
        .join_group("cg-sticky", "", 10_000, vec!["events".into()])
        .await
        .unwrap();
    // Solo member gets all partitions.
    assert_eq!(j1.assignment.len(), 4);
    let first_parts: Vec<u32> = j1.assignment.iter().map(|a| a.partition).collect();

    let c2 = Client::connect_addr(&addr).await.unwrap();
    let j2 = c2
        .join_group("cg-sticky", "", 10_000, vec!["events".into()])
        .await
        .unwrap();

    // After second join, rebalance already ran. Re-join c1 to fetch sticky assignment.
    let j1b = c1
        .join_group(
            "cg-sticky",
            &j1.member_id,
            10_000,
            vec!["events".into()],
        )
        .await
        .unwrap();

    let p1: HashSet<u32> = j1b.assignment.iter().map(|a| a.partition).collect();
    let p2: HashSet<u32> = j2.assignment.iter().map(|a| a.partition).collect();
    assert!(p1.is_disjoint(&p2));
    assert_eq!(p1.len() + p2.len(), 4);

    // Sticky: c1 should retain a subset of its previous partitions when possible.
    let retained: Vec<_> = first_parts.iter().filter(|p| p1.contains(p)).collect();
    assert!(
        !retained.is_empty(),
        "sticky should keep some of c1's prior partitions; first={first_parts:?} now={p1:?}"
    );

    let desc = admin.describe_group("cg-sticky").await.unwrap();
    assert_eq!(desc.members.len(), 2);

    let _ = std::fs::remove_dir_all(&dir);
}
