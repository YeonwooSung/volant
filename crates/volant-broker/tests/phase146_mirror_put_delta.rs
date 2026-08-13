//! Phase 146: incremental/delta MirrorPut wire (MVP).
//!
//! JSON `mode` field selects full vs delta; opcode 90 unchanged. Deltas carry
//! topic upserts + `remove_topic_keys`; full remains the durable/export default.

use std::collections::HashMap;

use volant_broker::kafka::fetch_session::{
    FetchSession, FetchSessionManager, SessionPartition, SessionTopic, TopicWireId,
};

fn part(offset: i64) -> SessionPartition {
    SessionPartition::new(offset, -1, -1, 1_000_000)
}

fn topic_map(name: &str, offset: i64) -> HashMap<String, SessionTopic> {
    let mut topics = HashMap::new();
    let mut parts = HashMap::new();
    parts.insert(0, part(offset));
    topics.insert(
        name.into(),
        SessionTopic {
            wire: TopicWireId::Name(name.into()),
            name: name.into(),
            partitions: parts,
        },
    );
    topics
}

fn json_mode(bytes: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    v.get("mode")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_owned()
}

/// Full put (Phase 138 path) still installs mirror + promotes.
#[test]
fn full_put_still_works() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(topic_map("orders", 3), 1_000);
    let bytes = owner.export_session_bytes(id).expect("export");
    assert_eq!(json_mode(&bytes), "full");

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(&bytes).unwrap();
    assert!(peer.mirror_contains(id));
    assert_eq!(peer.mirror_puts_applied_total(), 1);
    assert_eq!(peer.mirror_delta_puts_total(), 0);
    assert!(peer.promote_from_mirror(id));
    assert!(peer.snapshot_topics(id).contains_key("orders"));
}

/// Delta upsert adds a topic to an existing mirror without wiping others.
#[test]
fn delta_upsert_adds_topic() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(topic_map("orders", 1), 1_000);
    let full = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(&full).unwrap();

    let prev = FetchSession {
        epoch: 1,
        topics: topic_map("orders", 1),
        last_activity_ms: 1_000,
        mirror_gen: 1,
        promoted_by: 0,
    };

    owner.merge_topics(id, &topic_map("payments", 0));
    let delta = owner
        .export_session_delta_bytes(id, Some(&prev))
        .expect("delta");
    assert_eq!(json_mode(&delta), "delta");

    peer.apply_mirror_put(&delta).unwrap();
    assert_eq!(peer.mirror_delta_puts_total(), 1);
    assert!(peer.promote_from_mirror(id));
    let snap = peer.snapshot_topics(id);
    assert!(snap.contains_key("orders"), "orders kept");
    assert!(snap.contains_key("payments"), "payments upserted");
}

/// Delta `remove_topic_keys` drops a topic from the mirror.
#[test]
fn delta_remove_topic_keys() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let mut topics = topic_map("a", 1);
    topics.extend(topic_map("b", 2));
    let id = owner.create_at(topics, 1_000);
    let full = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(&full).unwrap();

    let prev = FetchSession {
        epoch: 1,
        topics: {
            let mut t = topic_map("a", 1);
            t.extend(topic_map("b", 2));
            t
        },
        last_activity_ms: 1_000,
        mirror_gen: 1,
        promoted_by: 0,
    };

    owner.forget(id, &[("b".into(), vec![])]);
    let delta = owner
        .export_session_delta_bytes(id, Some(&prev))
        .unwrap();
    assert_eq!(json_mode(&delta), "delta");
    let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
    let removes = v
        .get("remove_topic_keys")
        .and_then(|r| r.as_array())
        .unwrap();
    assert!(removes.iter().any(|x| x.as_str() == Some("b")));

    peer.apply_mirror_put(&delta).unwrap();
    peer.promote_from_mirror(id);
    let snap = peer.snapshot_topics(id);
    assert!(snap.contains_key("a"));
    assert!(!snap.contains_key("b"));
}

/// Pre-146 JSON without `mode` still applies as full (serde default).
#[test]
fn old_clients_full_json_without_mode() {
    let raw = r#"{"id":7,"epoch":1,"last_activity_ms":100,"mirror_gen":1,"topics":[{"key":"legacy","wire_kind":"name","wire_name":"legacy","name":"legacy","partitions":[{"id":0,"fetch_offset":1,"current_leader_epoch":-1,"last_fetched_epoch":-1,"max_bytes":1000}]}]}"#;
    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(raw.as_bytes()).unwrap();
    assert!(peer.mirror_contains(7));
    assert_eq!(peer.mirror_delta_puts_total(), 0);
    peer.promote_from_mirror(7);
    assert!(peer.snapshot_topics(7).contains_key("legacy"));
}

/// Metadata-only delta bumps activity without wiping topics.
#[test]
fn metadata_only_delta_keeps_topics() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(topic_map("sticky", 5), 1_000);
    let full = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.apply_mirror_put(&full).unwrap();

    let prev = FetchSession {
        epoch: 1,
        topics: topic_map("sticky", 5),
        last_activity_ms: 1_000,
        mirror_gen: 1,
        promoted_by: 0,
    };

    assert!(owner.begin_incremental_at(id, 1, 2_000).is_ok());
    let delta = owner
        .export_session_delta_bytes(id, Some(&prev))
        .unwrap();
    assert_eq!(json_mode(&delta), "delta");
    let v: serde_json::Value = serde_json::from_slice(&delta).unwrap();
    assert_eq!(
        v.get("topics").and_then(|t| t.as_array()).map(|a| a.len()),
        Some(0)
    );
    assert_eq!(
        v.get("last_activity_ms").and_then(|x| x.as_i64()),
        Some(2_000)
    );

    peer.apply_mirror_put(&delta).unwrap();
    assert_eq!(peer.mirror_delta_puts_total(), 1);
    peer.promote_from_mirror(id);
    let snap = peer.snapshot_topics(id);
    assert!(snap.contains_key("sticky"));
    assert_eq!(snap["sticky"].partitions[&0].fetch_offset, 5);
}

/// Fan-out helper: first export full, after `note_last_mirrored` subsequent is delta.
#[test]
fn export_mirror_put_bytes_delta_after_cache() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(topic_map("t", 0), 1_000);

    let (b1, is_delta1) = owner.export_mirror_put_bytes(id).unwrap();
    assert!(!is_delta1);
    assert_eq!(json_mode(&b1), "full");
    owner.note_last_mirrored(id);

    owner.merge_topics(id, &topic_map("u", 1));
    let (b2, is_delta2) = owner.export_mirror_put_bytes(id).unwrap();
    assert!(is_delta2);
    assert_eq!(json_mode(&b2), "delta");
    owner.note_last_mirrored(id);
    owner.record_mirror_delta_put_sent();
    assert_eq!(owner.mirror_delta_puts_total(), 1);
}
