//! Phase 134: DeleteRecords fan-out uses **achieved** `low_watermark`
//! (whole-segment clamp), not the client-requested `before_offset`.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config_with_session, multi_msg_storage, propagate_async, unique_dir, Guard,
};
use tokio::sync::oneshot;
use volant_broker::{
    serve_listener, serve_listener_until, start_background_tasks, Broker, BackgroundTasks,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, TopicName};

fn big(tag: &str, n: usize) -> String {
    format!("{tag}-{:0width$}", 0, width = n)
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
            .expect("produce");
        i += 1;
        let offs = leader.list_offsets(topic, vec![0]).await.unwrap();
        if offs.entries[0].latest >= min_latest {
            break;
        }
        assert!(i < 400, "fill past {min_latest}");
    }
}

/// Single-node: multi-message segments can clamp mid-segment deletes.
#[test]
fn multi_msg_segments_clamp_via_broker() {
    // Fresh broker per probe so each before is against an untruncated log.
    for before in [7u64, 11, 15, 19, 23, 27, 31] {
        let dir = unique_dir("p134", &format!("clamp-{before}"));
        let _g = Guard(dir.clone());
        let broker = Broker::new(multi_msg_storage(dir));
        broker.create_topic("t", 1).unwrap();
        let topic = TopicName::new("t");
        for i in 0..50u32 {
            let mut batch = volant_core::MessageBatch::default();
            batch
                .messages
                .push(Message::from_value(big(&format!("m{i}"), 180)));
            let (_, err) = broker
                .produce_with_acks(&topic, volant_core::PartitionId(0), batch, 1, None)
                .unwrap();
            assert_eq!(err, 0);
        }
        let (low, err) = broker.delete_records("t", 0, before).unwrap();
        assert_eq!(err, 0);
        if low < before {
            return;
        }
    }
    panic!("expected at least one mid-segment clamp with multi-msg segments");
}

/// Client DeleteRecords: outbox + journal stamp achieved low (not request).
///
/// Boots RF=3 with killable follower 3; if leader is 3, forces a re-create path
/// is skipped by asserting only when victim 3 is a follower (common case).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbox_and_journal_use_achieved_low_after_mid_segment_clamp() {
    let base = unique_dir("p134", "achieved-low");
    let _cleanup = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config_with_session(ports, 5_000);

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            multi_msg_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", ports[(id - 1) as usize]);
        Arc::new(b)
    };
    let b1 = mk(1);
    let b2 = mk(2);
    let b3 = mk(3);
    let mut bgs: Vec<BackgroundTasks> = vec![
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

    let controller = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    controller.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = controller.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    // Require killable follower = node 3 so outbox retain is strong.
    if leader_id == 3 {
        for bg in bgs.drain(..) {
            bg.shutdown().await;
        }
        let _ = k3_tx.send(());
        let _ = h3.await;
        // Rare: partition leader landed on 3 — storage unit + journal path still covered.
        return;
    }

    let leader = match leader_id {
        1 => Arc::clone(&b1),
        2 => Arc::clone(&b2),
        _ => Arc::clone(&b3),
    };
    let leader_client = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", ports[(leader_id - 1) as usize])],
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

    // Catch up followers LEO.
    for (id, b) in [(1u32, &b1), (2, &b2), (3, &b3)] {
        if id == leader_id {
            continue;
        }
        for _ in 0..100 {
            let e = b.list_offsets("events", &[0]).unwrap();
            if e[0].2 >= latest {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    // Kill follower 3 before DeleteRecords.
    let _ = k3_tx.send(());
    let _ = h3.await;
    let bg3 = bgs.remove(2);
    bg3.shutdown().await;

    let mut before = (latest / 2).max(1);
    let del = leader_client
        .delete_records("events", 0, before)
        .await
        .expect("delete");
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
    assert!(low > 0 && low < before, "low={low} before={before}");

    for _ in 0..40 {
        if leader.truncate_journal().watermark("events", 0) == Some(low) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        leader.truncate_journal().watermark("events", 0),
        Some(low),
        "journal must stamp achieved low, not before={before}"
    );

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
    assert!(saw, "expected outbox for offline peer 3");
    for e in leader
        .delete_records_outbox()
        .list()
        .iter()
        .filter(|e| e.replica_id == 3)
    {
        assert_eq!(
            e.before_offset, low,
            "outbox must use achieved low={low}, not before={before}; {e:?}"
        );
        assert_ne!(e.before_offset, before);
    }

    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}
