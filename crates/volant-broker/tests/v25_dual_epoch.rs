//! v0.25: fetch-session dual-epoch converge (unclaimed dual-primary).
//!
//! Winner: higher `mirror_gen`, then higher epoch, then lowest non-zero
//! `promoted_by` / owner id (`0` = no claim). Loser demotes to mirror.

use std::collections::HashMap;

use volant_broker::kafka::fetch_session::{
    both_unclaimed_primary_ish, converge_dual_epoch_pair, session_dual_epoch_wins, FetchSession,
    FetchSessionManager,
};

fn sess(epoch: i32, mirror_gen: u64, promoted_by: u32) -> FetchSession {
    FetchSession {
        epoch,
        topics: HashMap::new(),
        last_activity_ms: 1_000,
        mirror_gen,
        promoted_by,
    }
}

fn assert_one_owner(winner: &FetchSessionManager, loser: &FetchSessionManager, id: i32) {
    assert!(winner.contains(id), "winner must remain primary owner");
    assert!(
        !winner.mirror_contains(id),
        "winner must not also hold a mirror"
    );
    assert!(!loser.contains(id), "loser must not remain owner");
    assert!(loser.mirror_contains(id), "loser must be demoted to mirror");
}

/// 1. Two in-process unclaimed primaries, different gens → one MirrorPut /
///    helper leaves exactly one owner; loser is mirror; metric increments.
#[test]
fn dual_primary_converge_one_owner() {
    let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    a.set_dual_epoch_converge(true);
    b.set_dual_epoch_converge(true);
    let id = 42;
    a.install_primary(id, sess(2, 3, 0));
    b.install_primary(id, sess(8, 1, 0));
    assert!(a.contains(id) && b.contains(id));
    assert!(both_unclaimed_primary_ish(
        &a.primary_session_clone(id).unwrap(),
        &b.primary_session_clone(id).unwrap()
    ));

    let before_b = b.dual_epoch_converge_total();
    assert!(converge_dual_epoch_pair(&a, &b, id));
    assert_one_owner(&a, &b, id);
    assert_eq!(b.dual_epoch_converge_total(), before_b + 1);
    assert_eq!(a.dual_epoch_converge_total(), 0);
    let mirror = b.mirror_session_clone(id).unwrap();
    assert_eq!(mirror.mirror_gen, 3);
    assert_eq!(mirror.epoch, 2);

    // Same setup via one inbound MirrorPut onto the loser.
    let c = FetchSessionManager::with_limits_and_owner(0, 0, 3);
    let d = FetchSessionManager::with_limits_and_owner(0, 0, 4);
    c.set_dual_epoch_converge(true);
    d.set_dual_epoch_converge(true);
    c.install_primary(id, sess(2, 3, 0));
    d.install_primary(id, sess(8, 1, 0));
    let put = c.export_session_bytes(id).expect("export winner");
    let before_d = d.dual_epoch_converge_total();
    d.apply_mirror_put(&put).unwrap();
    assert_one_owner(&c, &d, id);
    assert_eq!(d.dual_epoch_converge_total(), before_d + 1);
}

/// 2. Higher `mirror_gen` beats a higher epoch (documented order).
#[test]
fn higher_mirror_gen_beats_higher_epoch() {
    let high_gen = sess(1, 9, 0);
    let high_epoch = sess(99, 2, 0);
    assert!(
        session_dual_epoch_wins(&high_gen, 0, &high_epoch, 0),
        "higher mirror_gen must beat higher epoch"
    );
    assert!(!session_dual_epoch_wins(&high_epoch, 0, &high_gen, 0));

    let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    a.set_dual_epoch_converge(true);
    b.set_dual_epoch_converge(true);
    let id = 11;
    // B has a much higher epoch but a stale gen.
    a.install_primary(id, high_gen);
    b.install_primary(id, high_epoch);
    assert!(converge_dual_epoch_pair(&a, &b, id));
    assert_one_owner(&a, &b, id);
    assert_eq!(b.dual_epoch_converge_total(), 1);
    assert_eq!(b.mirror_session_clone(id).unwrap().mirror_gen, 9);
}

/// 3. Phase 147 serve-from-mirror (single owner-miss) still works: mirror
///    stays foreign, no promote, no dual-epoch demote.
#[test]
fn serve_from_mirror_owner_miss_unchanged() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    let bytes = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.set_dual_epoch_converge(true);
    peer.apply_mirror_put(&bytes).unwrap();
    assert!(peer.has_servable_session(id));
    assert!(!peer.contains(id));
    assert!(peer.mirror_contains(id));

    assert!(peer.serve_mirror_without_promote());
    assert!(!peer.promote_on_miss());
    let serve_before = peer.serve_from_mirror_total();
    let converge_before = peer.dual_epoch_converge_total();
    assert!(peer.try_owner_miss_local_serve(id));
    assert_eq!(peer.serve_from_mirror_total(), serve_before + 1);
    assert_eq!(peer.promote_total(), 0);
    assert_eq!(peer.dual_epoch_converge_total(), converge_before);
    assert!(peer.mirror_contains(id));
    assert!(!peer.contains(id));

    assert!(peer.begin_incremental_from_any_at(id, 1, 1_100).is_ok());
    assert!(!peer.contains(id));
    assert!(peer.mirror_contains(id));
    assert_eq!(peer.mirror_session_clone(id).unwrap().epoch, 2);
}

/// 4. Knob off: helper is a no-op; both unclaimed primaries remain.
#[test]
fn env_off_helper_is_noop() {
    std::env::set_var("VOLANT_SESSION_DUAL_EPOCH_CONVERGE", "0");
    let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    std::env::remove_var("VOLANT_SESSION_DUAL_EPOCH_CONVERGE");
    a.set_dual_epoch_converge(false);
    b.set_dual_epoch_converge(false);
    let id = 99;
    a.install_primary(id, sess(1, 4, 0));
    b.install_primary(id, sess(7, 1, 0));

    assert!(!a.dual_epoch_converge_enabled());
    assert!(!converge_dual_epoch_pair(&a, &b, id));
    assert!(a.contains(id) && b.contains(id));
    assert!(!a.mirror_contains(id) && !b.mirror_contains(id));
    assert_eq!(a.dual_epoch_converge_total(), 0);
    assert_eq!(b.dual_epoch_converge_total(), 0);

    // Inbound MirrorPut with knob off keeps today's replace-primary (no demote).
    let put = a.export_session_bytes(id).unwrap();
    b.apply_mirror_put(&put).unwrap();
    assert!(
        b.contains(id),
        "env off: apply_mirror_put must not demote (139 replace-primary)"
    );
    assert!(!b.mirror_contains(id));
    assert_eq!(b.dual_epoch_converge_total(), 0);
}
