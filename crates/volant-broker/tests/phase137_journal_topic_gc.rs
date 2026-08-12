//! Phase 137: truncate-journal topic GC hygiene.
//!
//! - Local `delete_topic` still prunes journal watermarks.
//! - `apply_cluster_state` removing a topic prunes peer journal entries.
//! - Push apply filters unknown topics (anti-resurrection).

#[path = "common/mod.rs"]
mod common;

use common::cluster::{boot_triple_inprocess, new_single_broker, propagate, unique_dir, Guard};
use volant_broker::{TruncateJournalEntry, TruncateJournalFile, TRUNCATE_JOURNAL_FILE_VERSION};
use volant_core::TopicName;
use volant_protocol::ClusterTopicState;

/// Local delete_topic prunes truncate-journal watermarks (regression).
#[test]
fn delete_topic_prunes_local_journal() {
    let (broker, _g) = new_single_broker("p137", "delete-prune");
    broker.create_topic("doomed", 1).unwrap();
    let gen = broker.local_note_truncate_journal("doomed", 0, 50, 1);
    assert!(gen >= 1);
    assert_eq!(broker.truncate_journal().watermark("doomed", 0), Some(50));

    broker
        .delete_topic(&TopicName::new("doomed"))
        .expect("delete_topic");
    assert_eq!(
        broker.truncate_journal().watermark("doomed", 0),
        None,
        "delete_topic must prune journal watermarks"
    );
    assert!(
        broker
            .truncate_journal()
            .list()
            .iter()
            .all(|e| e.topic != "doomed"),
        "list must not retain deleted topic"
    );
}

/// apply_cluster_state that drops a topic prunes peer journal (no local delete_topic).
#[test]
fn apply_cluster_state_prunes_journal_for_dropped_topic() {
    let base = unique_dir("p137", "asg-prune");
    let _g = Guard(base.clone());
    let (b1, b2, b3) = boot_triple_inprocess(&base, [29701, 29702, 29703]);

    b1.create_topic("keep", 1).unwrap();
    b1.create_topic("drop_me", 1).unwrap();
    propagate(&[&b1, &b2, &b3], "keep");
    propagate(&[&b1, &b2, &b3], "drop_me");

    // Peer notes a watermark for drop_me without going through delete_topic later.
    b2.local_note_truncate_journal("drop_me", 0, 77, 1);
    b2.local_note_truncate_journal("keep", 0, 11, 1);
    assert_eq!(b2.truncate_journal().watermark("drop_me", 0), Some(77));
    assert_eq!(b2.truncate_journal().watermark("keep", 0), Some(11));

    // Controller deletes drop_me and bumps assignment generation.
    b1.delete_topic(&TopicName::new("drop_me")).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    assert!(
        !topics.iter().any(|t| t.name == "drop_me"),
        "controller snapshot must omit deleted topic"
    );
    assert!(
        topics.iter().any(|t| t.name == "keep"),
        "keep must remain in snapshot"
    );

    b2.apply_cluster_state(gen, cid, &topics).unwrap();
    assert_eq!(
        b2.truncate_journal().watermark("drop_me", 0),
        None,
        "assignment drop must prune peer journal for removed topic"
    );
    assert_eq!(
        b2.truncate_journal().watermark("keep", 0),
        Some(11),
        "kept topic watermark must survive"
    );

    // Empty assignment at higher gen also prunes remaining topics' journal keys.
    let empty: Vec<ClusterTopicState> = vec![];
    b2.apply_cluster_state(gen.saturating_add(1), cid, &empty)
        .unwrap();
    assert_eq!(b2.truncate_journal().watermark("keep", 0), None);
}

/// Push cannot reintroduce watermarks for unknown/deleted topics.
#[test]
fn push_anti_resurrection_skips_unknown_topic() {
    let (broker, _g) = new_single_broker("p137", "anti-res");
    // Broker has "alive" only.
    broker.create_topic("alive", 1).unwrap();
    broker.local_note_truncate_journal("alive", 0, 10, 0);
    assert_eq!(broker.truncate_journal().watermark("alive", 0), Some(10));
    assert_eq!(broker.truncate_journal().watermark("gone", 0), None);

    let file = TruncateJournalFile {
        version: TRUNCATE_JOURNAL_FILE_VERSION,
        generation: 9,
        entries: vec![
            TruncateJournalEntry {
                topic: "gone".into(),
                partition: 0,
                before_offset: 99,
                leader_epoch: 1,
            },
            TruncateJournalEntry {
                topic: "alive".into(),
                partition: 0,
                before_offset: 40,
                leader_epoch: 2,
            },
        ],
    };
    let snap = serde_json::to_vec(&file).unwrap();

    let code = broker.handle_truncate_journal_push(9, &snap);
    assert_eq!(code, 0, "push apply should succeed");
    assert_eq!(
        broker.truncate_journal().watermark("gone", 0),
        None,
        "unknown/deleted topic must not resurrect via push"
    );
    assert_eq!(
        broker.truncate_journal().watermark("alive", 0),
        Some(40),
        "known topic still max-merges"
    );
    assert!(broker.truncate_journal_applied_generation() >= 9);

    // After delete_topic, a push reintroducing the watermark is still skipped.
    broker
        .delete_topic(&TopicName::new("alive"))
        .expect("delete alive");
    assert_eq!(broker.truncate_journal().watermark("alive", 0), None);
    let revive = TruncateJournalFile {
        version: TRUNCATE_JOURNAL_FILE_VERSION,
        generation: 10,
        entries: vec![TruncateJournalEntry {
            topic: "alive".into(),
            partition: 0,
            before_offset: 100,
            leader_epoch: 3,
        }],
    };
    let snap2 = serde_json::to_vec(&revive).unwrap();
    assert_eq!(broker.handle_truncate_journal_push(10, &snap2), 0);
    assert_eq!(
        broker.truncate_journal().watermark("alive", 0),
        None,
        "deleted topic must not resurrect after local delete"
    );
}

/// Cluster peer: assignment remove then push cannot resurrect the dropped topic.
#[test]
fn cluster_push_anti_resurrection_after_assignment_drop() {
    let base = unique_dir("p137", "cluster-anti");
    let _g = Guard(base.clone());
    let (b1, b2, _b3) = boot_triple_inprocess(&base, [29711, 29712, 29713]);

    b1.create_topic("t", 1).unwrap();
    propagate(&[&b1, &b2], "t");

    b2.local_note_truncate_journal("t", 0, 55, 1);
    assert_eq!(b2.truncate_journal().watermark("t", 0), Some(55));

    b1.delete_topic(&TopicName::new("t")).unwrap();
    let (_, gen, cid, topics) = b1.cluster_state_snapshot();
    b2.apply_cluster_state(gen, cid, &topics).unwrap();
    assert_eq!(b2.truncate_journal().watermark("t", 0), None);

    let file = TruncateJournalFile {
        version: TRUNCATE_JOURNAL_FILE_VERSION,
        generation: 42,
        entries: vec![TruncateJournalEntry {
            topic: "t".into(),
            partition: 0,
            before_offset: 999,
            leader_epoch: 1,
        }],
    };
    let snap = serde_json::to_vec(&file).unwrap();
    assert_eq!(b2.handle_truncate_journal_push(42, &snap), 0);
    assert_eq!(
        b2.truncate_journal().watermark("t", 0),
        None,
        "push after assignment drop must not reintroduce watermark"
    );
}
