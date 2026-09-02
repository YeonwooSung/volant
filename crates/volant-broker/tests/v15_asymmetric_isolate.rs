//! v0.15 asymmetric isolate — A→B only (in-process, not chaos-mesh).
//!
//! v0.5 `minority_isolate_leader_split_brain_honesty` aborts `serve_listener`
//! and blocks **all** outbound RPC (symmetric island). This test keeps every
//! listener up and dest-blocks **A→B** only via `test_block_inter_broker_peer`.
//!
//! Honesty (Phase 134 heartbeat mesh):
//! - A→B RPC fails; B→A and C stay open
//! - B does **not** expire A: `note_peer_live` on successful **outbound** B→A
//!   (unlike v0.5 symmetric isolate, which aborts the listener + all outbound)
//! - Controller stays lowest-id (1) on all three
//! - `acks=1` to a leader that still reaches a majority of ISR still appends

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config_with_session, default_storage, propagate_async, unique_dir, Guard,
};
use volant_broker::{
    inter_broker_rpc, serve_listener, start_background_tasks, BackgroundTasks, Broker,
};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, MessageBatch, Offset, PartitionId, TopicName};
use volant_protocol::{Request, Response};

fn batch_value(s: impl Into<String>) -> MessageBatch {
    let mut batch = MessageBatch::default();
    batch.messages.push(Message::from_value(s.into()));
    batch
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asymmetric_isolate_a_to_b_acks1_still_works() {
    let base = unique_dir("v15", "asym");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config_with_session(ports, 400);

    let mk = |id: u32| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
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
    let h1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    let h2 = {
        let b = Arc::clone(&b2);
        tokio::spawn(async move {
            let _ = serve_listener(l2, b).await;
        })
    };
    let h3 = {
        let b = Arc::clone(&b3);
        tokio::spawn(async move {
            let _ = serve_listener(l3, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;

    let addr_of = |id: u32| format!("127.0.0.1:{}", ports[(id - 1) as usize]);
    let broker_of = |id: u32| -> Arc<Broker> {
        match id {
            1 => Arc::clone(&b1),
            2 => Arc::clone(&b2),
            3 => Arc::clone(&b3),
            _ => panic!("bad id {id}"),
        }
    };

    let admin = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p1}")],
        acks: 1,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = admin.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    assert_eq!(meta.topics[0].partitions[0].replicas.len(), 3);

    let producer = Client::connect(ClientConfig {
        brokers: vec![addr_of(leader_id)],
        acks: 1,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    producer
        .produce_with_acks(
            "events",
            Some(0),
            vec![Message::from_value("pre-isolate")],
            1,
        )
        .await
        .expect("acks=1 before isolate");

    // A=1 cannot send RPC to B=2; B→A and C remain open. Listeners stay up.
    b1.test_block_inter_broker_peer(2, true);

    let probe = Request::ClusterState {
        known_generation: 0,
    };
    let a_to_b = inter_broker_rpc(&b1, &addr_of(2), &probe).await;
    assert!(a_to_b.is_err(), "A→B must be dest-blocked, got {a_to_b:?}");
    let b_to_a = inter_broker_rpc(&b2, &addr_of(1), &probe).await;
    assert!(b_to_a.is_ok(), "B→A must still work, got {b_to_a:?}");
    let c_to_a = inter_broker_rpc(&b3, &addr_of(1), &probe).await;
    let c_to_b = inter_broker_rpc(&b3, &addr_of(2), &probe).await;
    assert!(c_to_a.is_ok(), "C→A must work, got {c_to_a:?}");
    assert!(c_to_b.is_ok(), "C→B must work, got {c_to_b:?}");

    // Wait past session_timeout. One-way A→B must not look like v0.5 death:
    // B→A still succeeds, so B keeps A live via note_peer_live.
    tokio::time::sleep(Duration::from_millis(800)).await;
    for _ in 0..10 {
        b1.tick_cluster();
        b2.tick_cluster();
        b3.tick_cluster();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (id, b) in [(1u32, &b1), (2, &b2), (3, &b3)] {
        let live = b.live_brokers();
        assert!(
            live.contains(&1) && live.contains(&2) && live.contains(&3),
            "node {id} must keep the full live set after A→B dest-block (Phase 134 outbound live); live={live:?}"
        );
        assert_eq!(
            b.controller_id(),
            1,
            "node {id} controller must stay 1 (no split-brain expire); live={live:?}"
        );
    }

    // Leader still reaches C (majority of ISR).
    let snap = b1.metadata(None);
    let events = snap
        .topics
        .iter()
        .find(|t| t.name.as_str() == "events")
        .expect("events on A");
    let produce_leader = events.partitions[0].leader;
    assert!(
        produce_leader == 1 || produce_leader == 3,
        "A/C view leader should still be 1 or 3 (can reach C); got {produce_leader}"
    );

    let topic = TopicName::new("events");
    let leader = broker_of(produce_leader);
    let (recs, code) = leader
        .produce_with_acks(
            &topic,
            PartitionId(0),
            batch_value("post-isolate"),
            1,
            Some(Duration::from_secs(2)),
        )
        .expect("acks=1 produce must not error");
    assert_eq!(code, 0, "acks=1 to reachable-majority leader, got {code}");
    assert_eq!(recs.len(), 1);

    let via_client = Client::connect(ClientConfig {
        brokers: vec![addr_of(produce_leader)],
        acks: 1,
        max_redirects: 0,
        ..ClientConfig::default()
    })
    .await
    .expect("connect reachable leader");
    via_client
        .produce_with_acks(
            "events",
            Some(0),
            vec![Message::from_value("post-isolate-client")],
            1,
        )
        .await
        .expect("client acks=1 to reachable-majority leader");

    let mut got = Vec::new();
    for _ in 0..40 {
        let f = via_client
            .fetch("events", 0, Offset::ZERO, 100, 50)
            .await
            .unwrap();
        if f.records
            .iter()
            .any(|r| r.value.as_ref() == b"post-isolate")
            && f.records
                .iter()
                .any(|r| r.value.as_ref() == b"post-isolate-client")
        {
            got = f.records;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        got.iter().any(|r| r.value.as_ref() == b"pre-isolate"),
        "leader fetch must still see pre-isolate record"
    );
    assert!(
        got.iter().any(|r| r.value.as_ref() == b"post-isolate"),
        "leader fetch must see acks=1 append after A→B isolate"
    );
    assert!(
        got.iter()
            .any(|r| r.value.as_ref() == b"post-isolate-client"),
        "leader fetch must see client acks=1 after A→B isolate"
    );

    match &b_to_a {
        Ok(Response::ClusterState { .. }) => {}
        Ok(other) => panic!("B→A expected ClusterState, got {other:?}"),
        Err(e) => panic!("B→A failed: {e}"),
    }

    h1.abort();
    h2.abort();
    h3.abort();
    for bg in bgs.drain(..) {
        bg.shutdown().await;
    }
}
