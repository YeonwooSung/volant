//! v0.30: fetch-session mirror-only self-converge (no primary).
//!
//! Winner: same as v0.25 — higher `mirror_gen`, then higher epoch, then
//! lowest non-zero `promoted_by` / owner id. Loser overwrites its mirror.
//! Does not promote.

use std::collections::HashMap;

use volant_broker::kafka::fetch_session::{
    converge_dual_mirror_pair, FetchSession, FetchSessionManager,
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

fn assert_both_mirrors(a: &FetchSessionManager, b: &FetchSessionManager, id: i32, gen: u64) {
    assert!(!a.contains(id), "must not promote A");
    assert!(!b.contains(id), "must not promote B");
    assert!(a.mirror_contains(id) && b.mirror_contains(id));
    assert_eq!(a.mirror_session_clone(id).unwrap().mirror_gen, gen);
    assert_eq!(b.mirror_session_clone(id).unwrap().mirror_gen, gen);
    assert_eq!(a.promote_total(), 0);
    assert_eq!(b.promote_total(), 0);
}

/// 1. Two in-process managers, same session id, mirrors only, different
///    `mirror_gen` → after helper/put, both hold the higher gen snapshot;
///    metric increments.
#[test]
fn mirror_only_converge_higher_gen() {
    let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    a.set_mirror_converge(true);
    b.set_mirror_converge(true);
    let id = 42;
    a.install_mirror(id, sess(2, 5, 0));
    b.install_mirror(id, sess(8, 1, 0));
    assert!(!a.contains(id) && !b.contains(id));
    assert!(a.mirror_contains(id) && b.mirror_contains(id));

    let before_b = b.mirror_converge_total();
    assert!(converge_dual_mirror_pair(&a, &b, id));
    assert_both_mirrors(&a, &b, id, 5);
    assert_eq!(b.mirror_converge_total(), before_b + 1);
    assert_eq!(a.mirror_converge_total(), 0);
    assert_eq!(b.mirror_session_clone(id).unwrap().epoch, 2);

    // Same setup via one inbound MirrorPut onto the loser.
    let c = FetchSessionManager::with_limits_and_owner(0, 0, 3);
    let d = FetchSessionManager::with_limits_and_owner(0, 0, 4);
    c.set_mirror_converge(true);
    d.set_mirror_converge(true);
    c.install_mirror(id, sess(2, 5, 0));
    d.install_mirror(id, sess(8, 1, 0));
    let put = c.export_session_bytes(id).expect("export winner mirror");
    let before_d = d.mirror_converge_total();
    d.apply_mirror_put(&put).unwrap();
    assert_both_mirrors(&c, &d, id, 5);
    assert_eq!(d.mirror_converge_total(), before_d + 1);
    assert_eq!(c.mirror_converge_total(), 0);
}

/// 2. Env off: helper is a no-op (both keep their gens).
#[test]
fn env_off_helper_is_noop() {
    std::env::set_var("VOLANT_SESSION_MIRROR_CONVERGE", "0");
    let a = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let b = FetchSessionManager::with_limits_and_owner(0, 0, 2);
    std::env::remove_var("VOLANT_SESSION_MIRROR_CONVERGE");
    a.set_mirror_converge(false);
    b.set_mirror_converge(false);
    let id = 99;
    a.install_mirror(id, sess(1, 4, 0));
    b.install_mirror(id, sess(7, 1, 0));

    assert!(!a.mirror_converge_enabled());
    assert!(!converge_dual_mirror_pair(&a, &b, id));
    assert!(!a.contains(id) && !b.contains(id));
    assert_eq!(a.mirror_session_clone(id).unwrap().mirror_gen, 4);
    assert_eq!(b.mirror_session_clone(id).unwrap().mirror_gen, 1);
    assert_eq!(a.mirror_converge_total(), 0);
    assert_eq!(b.mirror_converge_total(), 0);
}

/// 3. Regression: Phase 147 serve-from-mirror (single owner-miss) still works.
#[test]
fn serve_from_mirror_owner_miss_unchanged() {
    let owner = FetchSessionManager::with_limits_and_owner(0, 0, 1);
    let id = owner.create_at(HashMap::new(), 1_000);
    let bytes = owner.export_session_bytes(id).unwrap();

    let peer = FetchSessionManager::with_limits(0, 0);
    peer.set_mirror_converge(true);
    peer.apply_mirror_put(&bytes).unwrap();
    assert!(peer.has_servable_session(id));
    assert!(!peer.contains(id));
    assert!(peer.mirror_contains(id));

    assert!(peer.serve_mirror_without_promote());
    assert!(!peer.promote_on_miss());
    let serve_before = peer.serve_from_mirror_total();
    let converge_before = peer.mirror_converge_total();
    assert!(peer.try_owner_miss_local_serve(id));
    assert_eq!(peer.serve_from_mirror_total(), serve_before + 1);
    assert_eq!(peer.promote_total(), 0);
    assert_eq!(peer.mirror_converge_total(), converge_before);
    assert!(peer.mirror_contains(id));
    assert!(!peer.contains(id));

    assert!(peer.begin_incremental_from_any_at(id, 1, 1_100).is_ok());
    assert!(!peer.contains(id));
    assert!(peer.mirror_contains(id));
    assert_eq!(peer.mirror_session_clone(id).unwrap().epoch, 2);
}
