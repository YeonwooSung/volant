//! Phase 143: fetch-session promote claim fence (lowest non-zero `promoted_by`).
//!
//! Dual-promote of equal-freshness mirrors converges via MirrorPut exchange so
//! only the lowest claimer remains primary SoT.

use std::collections::HashMap;

use volant_broker::kafka::fetch_session::{
    session_claim_wins, session_is_newer, FetchSession, FetchSessionManager,
};

fn primary_promoted_by(mgr: &FetchSessionManager, id: i32) -> u32 {
    let bytes = mgr.export_session_bytes(id).expect("primary export");
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v.get("promoted_by")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32
}

fn primary_mirror_gen(mgr: &FetchSessionManager, id: i32) -> u64 {
    let bytes = mgr.export_session_bytes(id).expect("primary export");
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    v.get("mirror_gen")
        .and_then(|x| x.as_u64())
        .unwrap_or(0)
}

/// Owner creates a session; peers install the same equal-gen mirror and both promote.
/// After exchanging MirrorPut exports, only lowest claimer (2) remains SoT.
#[test]
fn dual_promote_lowest_claim_wins_after_exchange() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    let snapshot = owner.export_session_bytes(id).expect("export");

    let peer2 = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    let peer3 = FetchSessionManager::with_limits_and_owner(0, 0, 3);

    peer2.apply_mirror_put(&snapshot).unwrap();
    peer3.apply_mirror_put(&snapshot).unwrap();
    assert!(peer2.promote_from_mirror(id));
    assert!(peer3.promote_from_mirror(id));

    assert_eq!(primary_promoted_by(&peer2, id), 2);
    assert_eq!(primary_promoted_by(&peer3, id), 3);

    let export2 = peer2.export_session_bytes(id).unwrap();
    let export3 = peer3.export_session_bytes(id).unwrap();

    // 2's claim applied onto 3 → 3 supersedes to claim 2.
    let rejects3_before = peer3.promote_claim_reject_total();
    peer3.apply_mirror_put(&export2).unwrap();
    assert_eq!(primary_promoted_by(&peer3, id), 2);
    assert!(peer3.promote_claim_reject_total() == rejects3_before);

    // 3's claim applied onto 2 → rejected; 2 keeps SoT.
    let rejects2_before = peer2.promote_claim_reject_total();
    peer2.apply_mirror_put(&export3).unwrap();
    assert_eq!(primary_promoted_by(&peer2, id), 2);
    assert!(
        peer2.promote_claim_reject_total() > rejects2_before,
        "higher claimer put should claim-reject"
    );
}

/// Same dual-promote; apply order reversed (higher claim put first) still ends on 2.
#[test]
fn lower_id_wins_regardless_of_apply_order() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 2_000);
    let snapshot = owner.export_session_bytes(id).unwrap();

    let a = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 5);
    a.apply_mirror_put(&snapshot).unwrap();
    b.apply_mirror_put(&snapshot).unwrap();
    assert!(a.promote_from_mirror(id));
    assert!(b.promote_from_mirror(id));

    let export_low = a.export_session_bytes(id).unwrap();
    let export_high = b.export_session_bytes(id).unwrap();

    // Higher claim put first → primary claim 5; then low claim put wins.
    let peer2 = FetchSessionManager::with_limits_and_owner(0, 0, 9);
    peer2.apply_mirror_put(&export_high).unwrap();
    assert!(peer2.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&peer2, id), 5);
    peer2.apply_mirror_put(&export_low).unwrap();
    assert_eq!(primary_promoted_by(&peer2, id), 2);

    // Reverse: start with low, then high loses.
    let peer3 = FetchSessionManager::with_limits_and_owner(0, 0, 9);
    peer3.apply_mirror_put(&export_low).unwrap();
    assert!(peer3.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&peer3, id), 2);
    let reject_before = peer3.promote_claim_reject_total();
    peer3.apply_mirror_put(&export_high).unwrap();
    assert_eq!(primary_promoted_by(&peer3, id), 2);
    assert!(peer3.promote_claim_reject_total() > reject_before);
}

/// Strictly newer `mirror_gen` still wins even if the claimer id is higher (Phase 139 preserved).
#[test]
fn newer_mirror_gen_beats_lower_claim() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    let v1 = owner.export_session_bytes(id).unwrap();
    assert!(owner.begin_incremental_at(id, 1, 1_100).is_ok());
    let v2 = owner.export_session_bytes(id).unwrap();

    // Peer with low claim promotes older snapshot.
    let low = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    low.apply_mirror_put(&v1).unwrap();
    assert!(low.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&low, id), 2);
    let gen_old = primary_mirror_gen(&low, id);

    // Higher claimer promotes newer gen.
    let high = FetchSessionManager::with_limits_and_owner(0, 0, 9);
    high.apply_mirror_put(&v2).unwrap();
    assert!(high.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&high, id), 9);
    let gen_new = primary_mirror_gen(&high, id);
    assert!(gen_new > gen_old);

    // Newer gen from high claimer wins on low's primary.
    high.export_session_bytes(id).unwrap();
    let export_high = high.export_session_bytes(id).unwrap();
    low.apply_mirror_put(&export_high).unwrap();
    assert_eq!(primary_promoted_by(&low, id), 9);
    assert_eq!(primary_mirror_gen(&low, id), gen_new);

    // Older equal-claim put does not clobber.
    let export_low_old = {
        // Rebuild old claim snapshot: re-export from a peer with old gen claim 2.
        let tmp = FetchSessionManager::with_limits_and_owner(0, 0, 2);
        tmp.apply_mirror_put(&v1).unwrap();
        assert!(tmp.promote_from_mirror(id));
        tmp.export_session_bytes(id).unwrap()
    };
    let stale_before = low.mirror_stale_put_rejects_total();
    low.apply_mirror_put(&export_low_old).unwrap();
    assert_eq!(primary_promoted_by(&low, id), 9);
    assert!(
        low.mirror_stale_put_rejects_total() > stale_before,
        "older gen should stale-reject, not claim-reject"
    );
}

/// Equal-gen lower claim put replaces higher claim primary; higher claim put rejects.
#[test]
fn equal_gen_claim_put_supersede_and_reject() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    let snapshot = owner.export_session_bytes(id).unwrap();

    let high = FetchSessionManager::with_limits_and_owner(0, 0, 8);
    high.apply_mirror_put(&snapshot).unwrap();
    assert!(high.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&high, id), 8);

    let low = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    low.apply_mirror_put(&snapshot).unwrap();
    assert!(low.promote_from_mirror(id));
    let better = low.export_session_bytes(id).unwrap();
    let worse = high.export_session_bytes(id).unwrap();

    high.apply_mirror_put(&better).unwrap();
    assert_eq!(primary_promoted_by(&high, id), 2);

    let reject_before = high.promote_claim_reject_total();
    high.apply_mirror_put(&worse).unwrap();
    assert_eq!(primary_promoted_by(&high, id), 2);
    assert!(high.promote_claim_reject_total() > reject_before);
}

/// Unit: `session_claim_wins` pure rules (also covered in module tests).
#[test]
fn session_claim_wins_unit() {
    let base = FetchSession {
        epoch: 1,
        topics: HashMap::new(),
        last_activity_ms: 50,
        mirror_gen: 2,
        promoted_by: 0,
    };
    let a = FetchSession {
        promoted_by: 2,
        ..base.clone()
    };
    let b = FetchSession {
        promoted_by: 3,
        ..base.clone()
    };
    assert!(session_claim_wins(&a, &b));
    assert!(!session_claim_wins(&b, &a));
    assert!(session_is_newer(
        &FetchSession {
            mirror_gen: 3,
            promoted_by: 99,
            ..base.clone()
        },
        &a
    ));
    assert!(session_claim_wins(
        &FetchSession {
            mirror_gen: 3,
            promoted_by: 99,
            ..base
        },
        &a
    ));
}

/// Original create leaves `promoted_by=0`; promote stamps owner.
#[test]
fn create_unclaimed_promote_stamps_owner() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    assert_eq!(primary_promoted_by(&owner, id), 0);

    let peer = FetchSessionManager::with_limits_and_owner(0, 0, 4);
    let snap = owner.export_session_bytes(id).unwrap();
    peer.apply_mirror_put(&snap).unwrap();
    assert!(peer.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&peer, id), 4);

    // Single-node owner 0: promote leaves claim 0.
    let single = FetchSessionManager::with_limits(0, 0);
    single.apply_mirror_put(&snap).unwrap();
    assert!(single.promote_from_mirror(id));
    assert_eq!(primary_promoted_by(&single, id), 0);
}
