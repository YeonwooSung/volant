//! v0.10 dynamic membership overlay — add/remove, restart, stale gen, produce.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{cluster_config_n2, default_storage, unique_dir, Guard};
use volant_broker::{
    load_membership_overlay, Broker, BrokerEndpoint, MembershipOverlay, TruncateJournal,
};
use volant_core::{Message, PartitionId, TopicName};

fn boot_n2(base: &std::path::Path, ports: [u16; 2]) -> (Arc<Broker>, Arc<Broker>) {
    let cfg = cluster_config_n2(ports);
    let mk = |id: u32, port: u16| {
        let b = Broker::with_cluster(
            default_storage(base.join(format!("n{id}"))),
            id,
            cfg.clone(),
        )
        .unwrap();
        b.set_advertised("127.0.0.1", port);
        Arc::new(b)
    };
    (mk(1, ports[0]), mk(2, ports[1]))
}

fn overlay_on(b: &Broker) -> MembershipOverlay {
    load_membership_overlay(&b.cluster_state().unwrap().data_dir)
        .unwrap()
        .expect("membership.json")
}

#[test]
fn add_broker_writes_overlay_and_bumps_n() {
    let base = unique_dir("v10", "add");
    let _g = Guard(base.clone());
    let (b1, _b2) = boot_n2(&base, [19101, 19102]);
    assert_eq!(b1.configured_broker_count(), 2);
    assert_eq!(b1.cluster_member_count(), 2);
    assert_eq!(TruncateJournal::majority(b1.cluster_member_count()), 2);

    let gen = b1.add_broker(3, "127.0.0.1".into(), 19103, None).unwrap();
    assert!(gen >= 1, "generation={gen}");

    let overlay = overlay_on(&b1);
    assert_eq!(overlay.generation, gen);
    assert_eq!(overlay.brokers.len(), 3);
    assert!(overlay.brokers.iter().any(|b| b.id == 3));
    assert_eq!(b1.configured_broker_count(), 3);
    assert_eq!(b1.cluster_member_count(), 3);
    assert_eq!(TruncateJournal::majority(b1.cluster_member_count()), 2);
    assert_eq!(b1.majority_quorum_size(), 2);

    // Endpoint is configured immediately; new id is not live until heartbeat.
    assert!(!b1.live_brokers().contains(&3));
    assert!(b1.broker_addr(3).is_some());

    assert!(b1
        .add_broker(3, "127.0.0.1".into(), 19103, None)
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
}

#[test]
fn remove_broker_shrinks_overlay_and_rejects_self_and_last() {
    let base = unique_dir("v10", "remove");
    let _g = Guard(base.clone());
    let (b1, _b2) = boot_n2(&base, [19111, 19112]);
    b1.add_broker(3, "127.0.0.1".into(), 19113, None).unwrap();
    assert_eq!(b1.configured_broker_count(), 3);

    b1.remove_broker(3).unwrap();
    let overlay = overlay_on(&b1);
    assert_eq!(overlay.brokers.len(), 2);
    assert!(!overlay.brokers.iter().any(|b| b.id == 3));
    assert_eq!(b1.configured_broker_count(), 2);
    assert!(!b1.live_brokers().contains(&3));

    let err_self = b1.remove_broker(1).unwrap_err().to_string();
    assert!(err_self.contains("self"), "remove self: {err_self}");

    b1.remove_broker(2).unwrap();
    assert_eq!(b1.configured_broker_count(), 1);
    let err_last = b1.remove_broker(1).unwrap_err().to_string();
    assert!(
        err_last.contains("last remaining"),
        "remove last: {err_last}"
    );
}

#[test]
fn restart_loads_overlay_not_toml() {
    let base = unique_dir("v10", "restart");
    let _g = Guard(base.clone());
    let ports = [19121, 19122];
    let cfg_n2 = cluster_config_n2(ports);
    {
        let b1 = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg_n2.clone()).unwrap();
        b1.add_broker(3, "127.0.0.1".into(), 19123, None).unwrap();
        assert_eq!(b1.configured_broker_count(), 3);
    }

    // Toml still lists 2; overlay on disk has 3.
    assert_eq!(cfg_n2.brokers.len(), 2);
    let restarted = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg_n2).unwrap();
    assert_eq!(restarted.configured_broker_count(), 3);
    assert_eq!(restarted.membership_generation(), 1);
    let ids: Vec<u32> = restarted.cluster_config().unwrap().broker_ids();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn stale_generation_does_not_shrink() {
    let base = unique_dir("v10", "stale");
    let _g = Guard(base.clone());
    let (b1, _b2) = boot_n2(&base, [19131, 19132]);
    b1.add_broker(3, "127.0.0.1".into(), 19133, None).unwrap();
    let gen = b1.membership_generation();
    assert!(gen >= 1);
    assert_eq!(b1.configured_broker_count(), 3);

    let stale = vec![BrokerEndpoint {
        id: 1,
        host: "127.0.0.1".into(),
        port: 19131,
        rack: None,
    }];
    let applied = b1.apply_membership_put(0, stale).unwrap();
    assert_eq!(applied, gen);
    assert_eq!(b1.configured_broker_count(), 3);
    assert_eq!(overlay_on(&b1).brokers.len(), 3);

    // Equal generation is also ignored.
    let applied_eq = b1
        .apply_membership_put(
            gen,
            vec![BrokerEndpoint {
                id: 1,
                host: "127.0.0.1".into(),
                port: 19131,
                rack: None,
            }],
        )
        .unwrap();
    assert_eq!(applied_eq, gen);
    assert_eq!(b1.configured_broker_count(), 3);
}

#[test]
fn produce_acks1_still_works_after_add() {
    let base = unique_dir("v10", "produce");
    let _g = Guard(base.clone());
    let (b1, b2) = boot_n2(&base, [19141, 19142]);
    let topic = TopicName::new("events");
    b1.create_topic(topic.clone(), 1).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    let _ = b2.apply_cluster_state(gen, cid, &topics);

    let leader = b1
        .metadata(Some(&[topic.clone()]))
        .topics
        .iter()
        .find(|t| t.name.as_str() == "events")
        .and_then(|t| t.partitions.first())
        .map(|p| p.leader)
        .unwrap();
    let producer: &Broker = if leader == b1.node_id() { &b1 } else { &b2 };

    let rec = producer
        .produce_one(&topic, PartitionId(0), Message::from_value("before"))
        .unwrap();
    assert_eq!(rec.offset.raw(), 0);

    b1.add_broker(3, "127.0.0.1".into(), 19143, None).unwrap();
    assert_eq!(b1.configured_broker_count(), 3);

    let rec2 = producer
        .produce_one(&topic, PartitionId(0), Message::from_value("after"))
        .unwrap();
    assert_eq!(rec2.offset.raw(), 1);
}

#[tokio::test]
async fn native_admin_roundtrip_via_tcp() {
    use common::cluster::{bind_port0, rpc_seq};
    use volant_broker::serve_listener;
    use volant_protocol::{Request, Response};

    let base = unique_dir("v10", "tcp");
    let _g = Guard(base.clone());
    let (l1, p1) = bind_port0().await;
    let (_l2, p2) = bind_port0().await;
    let cfg = cluster_config_n2([p1, p2]);
    let b1 = Arc::new({
        let b = Broker::with_cluster(default_storage(base.join("n1")), 1, cfg).unwrap();
        b.set_advertised("127.0.0.1", p1);
        b
    });
    let server = {
        let b = Arc::clone(&b1);
        tokio::spawn(async move {
            let _ = serve_listener(l1, b).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    let addr = format!("127.0.0.1:{p1}");

    let resps = rpc_seq(
        &addr,
        &[
            Request::AddBroker {
                id: 3,
                host: "127.0.0.1".into(),
                port: 19153,
                rack: None,
            },
            Request::ListMembers,
        ],
    )
    .await;
    match &resps[0] {
        Response::AddBroker {
            error_code,
            generation,
        } => {
            assert_eq!(*error_code, 0);
            assert!(*generation >= 1);
        }
        other => panic!("add: {other:?}"),
    }
    match &resps[1] {
        Response::ListMembers {
            error_code,
            brokers,
            ..
        } => {
            assert_eq!(*error_code, 0);
            assert_eq!(brokers.len(), 3);
        }
        other => panic!("list: {other:?}"),
    }
    server.abort();
}
