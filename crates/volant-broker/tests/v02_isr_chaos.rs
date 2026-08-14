//! v0.2 ISR / chaos confidence — remaining gaps only.
//!
//! Already covered (do not duplicate):
//! - 3-node `acks=all` leader kill →
//!   `cluster_failover::three_node_acks_all_survives_leader_kill`
//! - Follower death ISR shrink + rolling restart while leader accepts `acks=all` →
//!   `phase8_redirect_restart::rolling_restart_follower_preserves_data` (Phase 108)
//! - Non-controller alive-set death → `phase110_alive_set_death`
//! - ISR rejoin + lag shrink → `phase118_isr_rejoin`
//! - Time-based ISR lag → `phase125_isr_time_lag`
//! - N=2 majority health gauges (in-process death) → `phase141_n2_majority_ops`
//!
//! This file: lowest-id controller death + produce/admin on the new controller;
//! N=2 `majority_impossible` observed on the CreateTopic wait path.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{
    bind_port0, cluster_config, cluster_config_n2, default_storage, propagate_async, rpc_seq,
    unique_dir, Guard,
};
use volant_broker::{render_metrics, serve_listener, start_background_tasks, Broker};
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset, PartitionId, TopicName};
use volant_protocol::{ErrorCode, Request, Response};

/// Kill the lowest-id controller; next-lowest becomes controller; `acks=all`
/// produce and CreateTopic continue on the surviving cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_death_lowest_id_failover_produce_continues() {
    let base = unique_dir("v02", "ctrl-death");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    let (l2, p2) = bind_port0().await;
    let (l3, p3) = bind_port0().await;
    let ports = [p1, p2, p3];
    let cfg = cluster_config(ports);

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
    assert!(
        b1.is_controller(),
        "id=1 is the initial lowest-id controller"
    );
    assert!(!b2.is_controller());
    assert!(!b3.is_controller());

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
    tokio::time::sleep(Duration::from_millis(120)).await;

    let port_of = |id: u32| ports[(id - 1) as usize];
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
        acks: 255,
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin.create_topic("events", 1).await.unwrap();
    propagate_async(&[&b1, &b2, &b3], "events").await;

    let meta = admin.metadata().await.unwrap();
    let leader_id = meta.topics[0].partitions[0].leader;
    // `events` is topic_hash start=2 → replicas [3,1,2]; this is controller-as-follower.
    assert_eq!(
        leader_id, 3,
        "events must place leader on 3 (not the controller)"
    );
    assert_eq!(meta.topics[0].partitions[0].replicas.len(), 3);

    let producer = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    const PRE: u32 = 3;
    for i in 0..PRE {
        let r = producer
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("pre-{i}"))],
                255,
            )
            .await
            .expect("acks=all before controller death");
        assert_eq!(r.count, 1);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Lowest-id controller process death (accept loop + membership).
    h1.abort();
    b2.test_kill_broker(1).unwrap();
    b3.test_kill_broker(1).unwrap();

    assert!(
        b2.is_controller(),
        "lowest remaining live id (2) must become controller"
    );
    assert_eq!(b2.controller_id(), 2);
    assert!(!b3.is_controller());

    let (_, gen, cid, topics) = b2.cluster_state_snapshot();
    let _ = b3.apply_cluster_state(gen, cid, &topics);
    tokio::time::sleep(Duration::from_millis(80)).await;

    let snap = b2.metadata(None);
    let events = snap
        .topics
        .iter()
        .find(|t| t.name.as_str() == "events")
        .expect("events topic on new controller");
    let new_leader_id = events.partitions[0].leader;
    assert_eq!(
        new_leader_id, leader_id,
        "controller-as-follower death must not move the partition leader"
    );
    assert!(
        !events.partitions[0].isr.contains(&1),
        "assignment ISR must drop dead controller: {:?}",
        events.partitions[0].isr
    );
    let local_isr = b3
        .local_partition_isr(&TopicName::new("events"), PartitionId(0))
        .unwrap();
    assert!(
        !local_isr.contains(&1),
        "leader-local ISR must drop dead controller: {local_isr:?}"
    );
    // Re-kill after the sleep so heartbeat-mesh cannot revive id=1 before
    // the live-set check (same window as the pre-CreateTopic re-kill).
    b2.test_kill_broker(1).unwrap();
    b3.test_kill_broker(1).unwrap();
    assert!(
        !b2.live_brokers().contains(&1) && !b3.live_brokers().contains(&1),
        "survivors must not list id=1 live"
    );

    let new_leader = broker_of(new_leader_id);
    let topic = TopicName::new("events");
    let leo = new_leader.log_end_offset(&topic, PartitionId(0)).unwrap();
    assert!(
        leo >= PRE as u64,
        "new/remaining leader LEO {leo} missing pre-death acks=all data"
    );
    // Seed remaining follower LEO so acks=all is not stuck if ReplicaFetch lags
    // across the controller handoff (same honesty as cluster_failover).
    let other = 2;
    let other_leo = broker_of(other)
        .log_end_offset(&topic, PartitionId(0))
        .unwrap_or(0);
    new_leader
        .test_set_follower_leo(&topic, PartitionId(0), other, other_leo)
        .unwrap();
    if new_leader.committed_hwm(&topic, PartitionId(0)).unwrap() < leo {
        new_leader
            .test_set_follower_leo(&topic, PartitionId(0), other, leo)
            .unwrap();
    }

    let after = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(new_leader_id))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .expect("connect surviving leader");

    const POST: u32 = 2;
    for i in 0..POST {
        after
            .produce_with_acks(
                "events",
                Some(0),
                vec![Message::from_value(format!("post-{i}"))],
                255,
            )
            .await
            .expect("acks=all must continue after controller death");
    }

    // Heartbeat-mesh can revive id=1 on a late RPC; re-kill immediately before
    // admin so CreateTopic cannot race NotController back to the dead node.
    b2.test_kill_broker(1).unwrap();
    b3.test_kill_broker(1).unwrap();
    assert!(
        b2.is_controller(),
        "id=2 must still be controller before admin"
    );

    // `survivor` is topic_hash start=1 → replicas [2,3,1], leader 2 (writable
    // on a survivor; `after-ctrl` would place leader on the dead id=1).
    let new_ctrl = Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{p2}")],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    create_topic_retry_not_controller(&new_ctrl, &[&b2, &b3], 1, "survivor").await;
    // New topic ISR is assigned from configured N (includes 1); shrink it.
    b2.test_kill_broker(1).unwrap();
    b3.test_kill_broker(1).unwrap();
    propagate_async(&[&b2, &b3], "survivor").await;
    assert!(b2.partition_count_opt("survivor").is_some());
    assert!(b3.partition_count_opt("survivor").is_some());

    let surv_snap = b2.metadata(None);
    let surv = surv_snap
        .topics
        .iter()
        .find(|t| t.name.as_str() == "survivor")
        .expect("survivor topic on new controller");
    let surv_leader = surv.partitions[0].leader;
    assert!(
        surv_leader == 2 || surv_leader == 3,
        "new topic leader must be a survivor, got {surv_leader}"
    );
    assert!(
        !surv.partitions[0].isr.contains(&1),
        "new topic ISR must drop dead controller: {:?}",
        surv.partitions[0].isr
    );

    let surv_other = if surv_leader == 2 { 3 } else { 2 };
    let surv_topic = TopicName::new("survivor");
    let surv_broker = broker_of(surv_leader);
    let surv_leo = surv_broker
        .log_end_offset(&surv_topic, PartitionId(0))
        .unwrap_or(0);
    surv_broker
        .test_set_follower_leo(&surv_topic, PartitionId(0), surv_other, surv_leo + 1)
        .unwrap();
    Client::connect(ClientConfig {
        brokers: vec![format!("127.0.0.1:{}", port_of(surv_leader))],
        acks: 255,
        max_redirects: 2,
        ..ClientConfig::default()
    })
    .await
    .unwrap()
    .produce_with_acks(
        "survivor",
        Some(0),
        vec![Message::from_value("on-survivor")],
        255,
    )
    .await
    .expect("new topic must be writable on a survivor after controller death");

    let want = (PRE + POST) as usize;
    let mut got = Vec::new();
    for _ in 0..40 {
        let f = after
            .fetch("events", 0, Offset::ZERO, 100, 50)
            .await
            .unwrap();
        if f.records.len() >= want {
            got = f.records;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        got.len(),
        want,
        "surviving leader must serve pre- and post-failover acks=all data"
    );
    for i in 0..PRE {
        assert_eq!(
            got[i as usize].value.as_ref(),
            format!("pre-{i}").as_bytes()
        );
    }
    for i in 0..POST {
        assert_eq!(
            got[(PRE + i) as usize].value.as_ref(),
            format!("post-{i}").as_bytes()
        );
    }

    h2.abort();
    h3.abort();
}

/// Re-kill `dead_id` and retry CreateTopic if heartbeat-mesh revival flipped
/// controller_id back to the dead lowest id (NotController on :p2).
async fn create_topic_retry_not_controller(
    client: &Client,
    survivors: &[&Broker],
    dead_id: u32,
    name: &str,
) {
    for attempt in 0..8 {
        for s in survivors {
            let _ = s.test_kill_broker(dead_id);
        }
        match client.create_topic(name, 1).await {
            Ok(_) => return,
            Err(e) => {
                let msg = e.to_string();
                if msg.to_ascii_lowercase().contains("not controller") && attempt + 1 < 8 {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    continue;
                }
                panic!("CreateTopic {name} on new controller failed: {e}");
            }
        }
    }
    panic!("CreateTopic {name} still NotController after retries");
}

fn assignment_json_has_topic(data_dir: &std::path::Path, topic: &str) -> bool {
    let path = data_dir.join("cluster").join("assignment.json");
    if !path.is_file() {
        return false;
    }
    let raw = std::fs::read_to_string(&path).unwrap_or_default();
    raw.contains(&format!("\"{topic}\"")) || raw.contains(&format!("\"name\": \"{topic}\""))
}

/// N=2 one-dead: `majority_impossible` gauge flips, and CreateTopic with wait
/// on returns `NotEnoughReplicas` (15) while still writing `assignment.json`.
#[tokio::test]
async fn n2_majority_impossible_create_topic_wait() {
    let base = unique_dir("v02", "n2-maj");
    let _g = Guard(base.clone());

    let (l1, p1) = bind_port0().await;
    // Peer never listens — death is explicit via on_broker_death below.
    let p2 = p1.saturating_add(100).max(33_000);
    let cfg = cluster_config_n2([p1, p2]);
    let data_dir = base.join("n1");
    let b1 = {
        let b = Broker::with_cluster(default_storage(data_dir.clone()), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        Arc::new(b)
    };
    assert!(
        !b1.majority_impossible(),
        "optimistic N=2 start is still majority-reachable"
    );
    assert!(!b1.assignment_consensus_wait());

    let _bg1 = start_background_tasks(Arc::clone(&b1));
    let s1 = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            serve_listener(l1, b).await.ok();
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    b1.on_broker_death(2).unwrap();
    assert!(
        b1.majority_impossible(),
        "N=2 one-dead must flip majority_impossible"
    );
    assert_eq!(b1.configured_broker_count(), 2);
    assert_eq!(b1.live_broker_count(), 1);
    assert_eq!(b1.majority_quorum_size(), 2);

    let text = render_metrics(&b1);
    assert!(
        text.contains("volant_cluster_majority_impossible 1\n"),
        "metrics must observe majority_impossible=1:\n{text}"
    );
    assert!(text.contains("volant_cluster_live_brokers 1\n"));
    assert!(text.contains("volant_cluster_majority_quorum 2\n"));
    assert!(text.contains("volant_cluster_configured_brokers 2\n"));

    b1.set_assignment_consensus_wait(true);
    assert!(b1.assignment_consensus_wait());

    let addr = format!("127.0.0.1:{p1}");
    let resps = rpc_seq(
        &addr,
        &[Request::CreateTopic {
            name: "blocked".into(),
            partitions: 1,
            configs: vec![],
        }],
    )
    .await;
    match &resps[0] {
        Response::Error { code, message } => {
            assert_eq!(
                *code,
                ErrorCode::NotEnoughReplicas as u16,
                "CreateTopic wait must surface 15, got {code} ({message})"
            );
        }
        Response::CreateTopic { error_code, .. } => {
            panic!("expected Error 15 on wait, got CreateTopic error_code={error_code}");
        }
        other => panic!("CreateTopic wait expected Error 15, got {other:?}"),
    }
    assert!(
        assignment_json_has_topic(&data_dir, "blocked"),
        "mutate-first: assignment.json is written even when wait fails"
    );

    s1.abort();
}
