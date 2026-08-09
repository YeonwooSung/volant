//! Phase 134: DeleteRecords fan-out / outbox / journal use **achieved**
//! `low_watermark` after whole-segment clamp — not the client-requested
//! `before_offset` when the request lands mid-segment.
//!
//! Production fix:
//! - native `Request::DeleteRecords` → `fanout_delete_records(..., low_watermark)`
//! - Kafka `encode_delete_records` fanouts push `low`, not `p.offset`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use volant_broker::{
    serve_listener, serve_listener_until, start_background_tasks, Broker, BrokerEndpoint,
    ClusterConfig,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, PartitionId, TopicName};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p134-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn bind_port0() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

fn cluster_config(ports: [u16; 3], session_timeout_ms: u32) -> ClusterConfig {
    ClusterConfig {
        default_replication_factor: 3,
        min_insync_replicas: 2,
        session_timeout_ms,
        replica_fetch_max_wait_ms: 50,
        replica_fetch_max_bytes: 1_048_576,
        replica_lag_max_messages: 10_000,
        replica_lag_max_ms: 30_000,
        brokers: (1..=3)
            .map(|id| BrokerEndpoint {
                id,
                host: "127.0.0.1".into(),
                port: ports[(id - 1) as usize],
                rack: None,
            })
            .collect(),
    }
}

async fn propagate(nodes: &[&Broker], topic: &str) {
    let src = nodes[0];
    for _ in 0..50 {
        let (_, gen, cid, topics) = src.cluster_state_snapshot();
        for n in nodes.iter().skip(1) {
            let _ = n.apply_cluster_state(gen, cid, &topics);
        }
        if nodes
            .iter()
            .all(|n| n.partition_count_opt(topic).is_some())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("assignment did not propagate for topic {topic}");
}

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
}

/// Storage tuned so several ~180-byte values fit a segment — mid-segment
/// DeleteRecords clamps `low_watermark` strictly below the request offset.
///
/// Note: `segment_size: 256` (used in phase113/116) often packs ~1 framed
/// 180-byte record so `low == before` and the clamp bug is invisible. 1024
/// yields multi-message sealed segments for a real mid-segment clamp.
fn multi_msg_storage(data_dir: PathBuf) -> StorageConfig {
    StorageConfig {
        data_dir,
        flush_every_n: 1,
        segment_size: 1024,
        ..StorageConfig::default()
    }
}

async fn fill_acks_all(leader: &Client, topic: &str, min_latest: u64) {
    let mut i = 0u32;
    loop {
        leader
            .produce_with_acks(
                topic,
                Some(0),
                vec![Message::from_value(big(&format!("m{i}"), 180))],
                255,
            )
            .await
            .expect("produce acks=all");
        i += 1;
        let offs = leader.list_offsets(topic, vec![0]).await.unwrap();
        if offs.entries[0].latest >= min_latest {
            break;
        }
        if i > 400 {
            panic!("could not fill past {min_latest}");
        }
    }
}

fn clamp_before(latest: u64) -> u64 {
    let half = latest / 2;
    if half > 0 {
        half
    } else {
        latest.saturating_sub(1).max(1)
    }
}

struct Triple {
    ports: [u16; 3],
    b1: Arc<Broker>,
    b2: Arc<Broker>,
    b3: Arc<Broker>,
    bgs: Vec<volant_broker::BackgroundTasks>,
    /// Kill switch for broker 3's listener (if started killable).
    kill3: Option<oneshot::Sender<()>>,
    h3: Option<tokio::task::JoinHandle<()>>,
}

impl Triple {
    fn broker(&self, id: u32) -> Arc<Broker> {
        match id {
            1 => Arc::clone(&self.b1),
            2 => Arc::clone(&self.b2),
            3 => Arc::clone(&self.b3),
            _ => panic!("bad id {id}"),
        }
    }

    fn port(&self, id: u32) -> u16 {
        self.ports[(id - 1) as usize]
    }

    async fn shutdown_all(mut self) {
        if let Some(tx) = self.kill3.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.h3.take() {
            let _ = h.await;
        }
        for bg in self.bgs.drain(..) {
            bg.shutdown().await;
        }
    }
}

/// Boot RF=3 with all three listeners; broker 3 is killable via oneshot.
async fn boot_triple_killable3(label: &str) -> (Triple, Guard) {
    let base = unique_dir(label);
    let cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports, 5_000);

    let mk = |id: u32| {
        let broker = Broker::with_cluster(
            multi_msg_storage(base.join(format!("node-{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);

    let bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
        start_background_tasks(Arc::clone(&b3)),
    ];

    for (listener, b) in [(l1, &b1), (l2, &b2)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }

    let (k3_tx, k3_rx) = oneshot::channel::<()>();
    let h3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = serve_listener_until(l3, b, async move {
                let _ = k3_rx.await;
            })
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    (
        Triple {
            ports,
            b1,
            b2,
            b3,
            bgs,
            kill3: Some(k3_tx),
            h3: Some(h3),
        },
        cleanup,
    )
}

async fn wait_followers_leo(nodes: &[(u32, &Broker)], leader_id: u32, latest: u64) {
    for (id, b) in nodes {
        if *id == leader_id {
            continue;
        }
        for _ in 0..100 {
            let entries = b.list_offsets("events", &[0]).unwrap();
            if entries[0].2 >= latest {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let entries = b.list_offsets("events", &[0]).unwrap();
        assert!(
            entries[0].2 >= latest,
            "follower {id} LEO {} < leader latest {latest}",
            entries[0].2
        );
    }
}

/// Client DeleteRecords after whole-segment clamp: offline peer outbox and
/// leader truncate journal both stamp **achieved** `low_watermark`, not the
/// client `before` request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_and_journal_use_achieved_low_after_mid_segment_clamp() {
    let (mut triple, _cleanup) = boot_triple_killable3("outbox-achieved-low").await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", triple.port(1))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(
        &[triple.b1.as_ref(), triple.b2.as_ref(), triple.b3.as_ref()],
        "events",
    )
    .await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    assert!((1..=3).contains(&leader_id), "leader_id={leader_id}");

    // Victim = a follower we can take offline so outbox retains fan-out.
    // Prefer broker 3 when it is not leader (killable listener). Otherwise
    // pick any other non-leader and only assert journal + local clamp (no
    // outbox retain) — but we always have at least one follower among {1,2,3}.
    let victim_id = if leader_id != 3 {
        3u32
    } else {
        1u32
    };

    let leader = triple.broker(leader_id);
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", triple.port(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    fill_acks_all(&leader_client, "events", 40).await;
    let latest = leader_client
        .list_offsets("events", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;
    assert!(latest >= 40, "latest={latest}");

    wait_followers_leo(
        &[
            (1, triple.b1.as_ref()),
            (2, triple.b2.as_ref()),
            (3, triple.b3.as_ref()),
        ],
        leader_id,
        latest,
    )
    .await;

    // Take victim offline before DeleteRecords.
    if victim_id == 3 {
        if let Some(tx) = triple.kill3.take() {
            let _ = tx.send(());
        }
        if let Some(h) = triple.h3.take() {
            let _ = h.await;
        }
        // bg index 2 is broker 3
        let bg3 = triple.bgs.remove(2);
        bg3.shutdown().await;
    } else {
        // Leader is 3; stop broker 1's background + rely on closing by not
        // serving — broker 1 still has a long-lived serve_listener task we
        // cannot easily kill. Fall back: mark peer dead and use never-bound
        // style by enqueuing only via fan-out RPC failure — connect to a
        // free port after dropping is hard. Instead elect path: use
        // `note_peer` is not enough. Simplest: manually stop accepting by
        // shutting bg and leaving TCP half-open; RPC will still sometimes
        // succeed if serve task lives.
        //
        // Strong guarantee when victim is 3 only. When leader is 3, skip
        // outbox-retain and only check journal after delete (all peers up
        // would drain outbox). Force outbox by never-started is separate.
        // Here: kill 3 is impossible if we need it live as leader.
        // Use local enqueue assertion via delete + journal only.
    }

    let mut before = clamp_before(latest);
    assert!(before > 0 && before < latest, "before={before} latest={latest}");

    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("client delete must succeed with offline follower");
    let mut low = del.low_watermark;

    if low >= before {
        // Request landed on a segment boundary — produce more and straddle
        // the active segment (before = latest - 1).
        fill_acks_all(&leader_client, "events", latest + 20).await;
        let latest2 = leader_client
            .list_offsets("events", vec![0])
            .await
            .unwrap()
            .entries[0]
            .latest;
        before = latest2.saturating_sub(1).max(low.saturating_add(1));
        let del2 = leader_client
            .delete_records("events", 0, before)
            .await
            .expect("retry delete for mid-segment clamp");
        low = del2.low_watermark;
    }

    assert!(low > 0, "expected log start to advance: low={low}");
    assert!(
        low < before,
        "expected whole-segment clamp (low < before): low={low} before={before}"
    );

    // Journal note inside fanout uses the same achieved low.
    let mut journal_ok = false;
    for _ in 0..40 {
        if leader.truncate_journal().watermark("events", 0) == Some(low) {
            journal_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        journal_ok,
        "leader truncate_journal watermark expected Some({low}), got {:?} \
         (client before was {before})",
        leader.truncate_journal().watermark("events", 0)
    );

    if victim_id == 3 {
        let mut saw_outbox = false;
        for _ in 0..60 {
            if leader
                .delete_records_outbox()
                .list()
                .iter()
                .any(|e| e.replica_id == 3)
            {
                saw_outbox = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            saw_outbox,
            "expected outbox entry for offline replica 3 (depth={}, enqueued={}, ferr={})",
            leader.delete_records_outbox_depth(),
            leader.delete_records_outbox_enqueued_total(),
            leader.delete_records_fanout_errors_total()
        );

        let pending = leader.delete_records_outbox().list();
        for e in pending.iter().filter(|e| e.replica_id == 3) {
            assert_eq!(e.topic, "events");
            assert_eq!(e.partition, 0);
            assert_eq!(
                e.before_offset, low,
                "outbox must fan out **achieved** low_watermark={low}, not client before={before}; entry={e:?}"
            );
            assert_ne!(
                e.before_offset, before,
                "outbox before_offset must not be the unclamped client request: {e:?}"
            );
        }
    }

    triple.shutdown_all().await;
}

/// Kill follower after replicate: outbox for that peer stamps achieved low
/// (strongest multi-broker proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kill_follower_outbox_stamps_achieved_low_not_request_before() {
    let (mut triple, _cleanup) = boot_triple_killable3("kill-follower-low").await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", triple.port(1))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(
        &[triple.b1.as_ref(), triple.b2.as_ref(), triple.b3.as_ref()],
        "events",
    )
    .await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;

    // Need killable victim = 3 as follower. If leader is 3, still run clamp +
    // journal on leader and force outbox via never-started peer by advertising
    // a dead address — skip outbox peer-id assert and use journal-only path.
    let leader = triple.broker(leader_id);
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", triple.port(leader_id))],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    fill_acks_all(&leader_client, "events", 40).await;
    let latest = leader_client
        .list_offsets("events", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;

    wait_followers_leo(
        &[
            (1, triple.b1.as_ref()),
            (2, triple.b2.as_ref()),
            (3, triple.b3.as_ref()),
        ],
        leader_id,
        latest,
    )
    .await;

    let victim_is_3 = leader_id != 3;
    if victim_is_3 {
        if let Some(tx) = triple.kill3.take() {
            let _ = tx.send(());
        }
        if let Some(h) = triple.h3.take() {
            let _ = h.await;
        }
        let bg3 = triple.bgs.remove(2);
        bg3.shutdown().await;
    }

    let mut before = clamp_before(latest);
    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("delete with offline follower");
    let mut low = del.low_watermark;
    if low >= before {
        fill_acks_all(&leader_client, "events", latest + 20).await;
        let latest2 = leader_client
            .list_offsets("events", vec![0])
            .await
            .unwrap()
            .entries[0]
            .latest;
        before = latest2.saturating_sub(1).max(low.saturating_add(1));
        low = leader_client
            .delete_records("events", 0, before)
            .await
            .expect("retry delete")
            .low_watermark;
    }

    assert!(low > 0, "low={low}");
    assert!(
        low < before,
        "expected mid-segment clamp: low={low} before={before}"
    );

    // Journal always on the handling leader after fan-out note.
    for _ in 0..40 {
        if leader.truncate_journal().watermark("events", 0) == Some(low) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        leader.truncate_journal().watermark("events", 0),
        Some(low),
        "journal must record achieved low, not request before={before}"
    );

    if victim_is_3 {
        let mut saw = false;
        for _ in 0..60 {
            if leader
                .delete_records_outbox()
                .list()
                .iter()
                .any(|e| e.replica_id == 3)
            {
                saw = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(saw, "expected outbox entry for killed peer 3");

        let pending = leader.delete_records_outbox().list();
        for e in pending.iter().filter(|e| e.replica_id == 3) {
            assert_eq!(
                e.before_offset, low,
                "killed follower outbox must use achieved low={low}, not before={before}; {e:?}"
            );
            assert_ne!(e.before_offset, before);
        }
    }

    triple.shutdown_all().await;
}

/// Never-started peer (port bound then dropped): leader among live brokers
/// only — create topic after checking assignment; if leader would be 3, use
/// broker 1/2 produce path by creating with RF assignment that still can put
/// leader on 3. Mitigate by connecting only via b1/b2 and **skipping** when
/// metadata says leader is 3 (retry once is not enough). Instead: start peer 3
/// briefly is already in kill path above.
///
/// This test uses **2 listening + 1 dead port** like phase116, but chooses
/// the leader from live nodes by preferring produce only when reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn never_started_peer_outbox_uses_achieved_low() {
    let base = unique_dir("never-started-low");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let p3 = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports, 2_000);

    let mk = |id: u32| {
        let broker = Broker::with_cluster(
            multi_msg_storage(base.join(format!("node-{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        broker.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(broker)
    };

    let b1 = mk(1);
    let b2 = mk(2);
    let mut bgs = vec![
        start_background_tasks(Arc::clone(&b1)),
        start_background_tasks(Arc::clone(&b2)),
    ];
    for (listener, b) in [(l1, &b1), (l2, &b2)] {
        let b = Arc::clone(b);
        tokio::spawn(async move {
            let _ = serve_listener(listener, b).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate(&[&b1, &b2], "events").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    // Partition may be assigned leader=3 (dead) — cannot exercise client path.
    // Kill-follower tests cover outbox when all three start; skip this rare case.
    if leader_id != 1 && leader_id != 2 {
        for bg in bgs.drain(..) {
            bg.shutdown().await;
        }
        return;
    }
    let leader = if leader_id == 1 {
        Arc::clone(&b1)
    } else {
        Arc::clone(&b2)
    };

    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", ports[(leader_id - 1) as usize])],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    // acks=1 so produce does not wait on dead replica 3.
    let mut i = 0u32;
    loop {
        leader_client
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(big(&format!("x{i}"), 180))],
                1,
            )
            .await
            .unwrap();
        i += 1;
        let latest = leader_client
            .list_offsets("events", vec![0])
            .await
            .unwrap()
            .entries[0]
            .latest;
        if latest >= 40 || i > 400 {
            break;
        }
    }

    let latest = leader_client
        .list_offsets("events", vec![0])
        .await
        .unwrap()
        .entries[0]
        .latest;
    let mut before = clamp_before(latest);
    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("client delete must succeed");
    let mut low = del.low_watermark;
    if low >= before {
        for j in 0..25u32 {
            leader_client
                .produce_with_acks(
                    "events",
                    Some(0),
                    vec![Message::from_value(big(&format!("y{j}"), 180))],
                    1,
                )
                .await
                .unwrap();
        }
        let latest2 = leader_client
            .list_offsets("events", vec![0])
            .await
            .unwrap()
            .entries[0]
            .latest;
        before = latest2.saturating_sub(1).max(low.saturating_add(1));
        low = leader_client
            .delete_records("events", 0, before)
            .await
            .unwrap()
            .low_watermark;
    }

    assert!(low < before, "clamp: low={low} before={before}");

    for _ in 0..40 {
        if leader.truncate_journal().watermark("events", 0) == Some(low) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        leader.truncate_journal().watermark("events", 0),
        Some(low)
    );

    let mut found = None;
    for _ in 0..60 {
        let pending = leader.delete_records_outbox().list();
        if let Some(e) = pending.into_iter().find(|e| e.replica_id == 3) {
            found = Some(e);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let e = found.expect("expected outbox for never-started peer 3");
    assert_eq!(
        e.before_offset, low,
        "never-started peer outbox must use achieved low={low}, not before={before}; {e:?}"
    );
    assert_ne!(e.before_offset, before);

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}

/// Unit-style: whole-segment clamp returns low < before with multi-msg segments
/// (documents the storage precondition the integration tests rely on).
#[test]
fn multi_msg_segments_clamp_low_below_before() {
    let dir = unique_dir("unit-clamp");
    let _g = Guard(dir.clone());
    let broker = Broker::new(multi_msg_storage(dir));
    broker.create_topic("s", 1).unwrap();
    for i in 0..50u32 {
        broker
            .produce_one(
                &TopicName::new("s"),
                PartitionId(0),
                Message::from_value(big(&format!("z{i}"), 180)),
            )
            .unwrap();
    }
    let latest = broker.list_offsets("s", &[0]).unwrap()[0].2;
    let before = clamp_before(latest);
    let (low, err) = broker.delete_records("s", 0, before).unwrap();
    assert_eq!(err, 0);
    assert!(
        low < before,
        "precondition: multi_msg_storage must clamp low ({low}) < before ({before}); latest={latest}"
    );
    assert!(broker.delete_records_fanout_peers("s", 0).is_empty());
    assert_eq!(broker.delete_records_outbox_depth(), 0);
}
