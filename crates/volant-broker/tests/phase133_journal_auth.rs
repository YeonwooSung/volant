//! Phase 133: TruncateJournalNote (86) / TruncateJournalPush (88) auth + ACL gates.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::cluster::{boot_listener, new_single_broker, rpc_seq};
use volant_broker::net::dispatch_request_as;
use volant_broker::{
    inter_broker_rpc, AclEntry, AclOperation, AclPermission, Broker, ResourceType, CLUSTER_RESOURCE,
};
use volant_protocol::{ErrorCode, Request, Response};

fn note_req(topic: &str, before_offset: u64, leader_epoch: i32) -> Request {
    Request::TruncateJournalNote {
        topic: topic.into(),
        partition: 0,
        before_offset,
        leader_epoch,
    }
}

fn push_req(generation: u64, snapshot: Bytes) -> Request {
    Request::TruncateJournalPush {
        generation,
        snapshot,
    }
}

fn cluster_alter(principal: &str) -> AclEntry {
    AclEntry {
        principal: principal.into(),
        resource_type: ResourceType::Cluster,
        resource: CLUSTER_RESOURCE.into(),
        operation: AclOperation::Alter,
        permission: AclPermission::Allow,
    }
}

#[tokio::test]
async fn note_unauth_denied_when_token_required() {
    let (broker, _g) = new_single_broker("p133", "unauth");
    broker.set_auth_token(Some("secret".into()));
    broker.create_topic("t", 1).unwrap();
    let (addr, _h) = boot_listener(Arc::clone(&broker)).await;

    let resps = rpc_seq(&addr, &[note_req("t", 40, 0)]).await;
    match &resps[0] {
        Response::Error { code, .. } => {
            assert_eq!(*code, ErrorCode::AuthenticationRequired as u16);
        }
        other => panic!("expected Error 18, got {other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), None);
}

#[tokio::test]
async fn note_wrong_token_auth_failed() {
    let (broker, _g) = new_single_broker("p133", "wrong-token");
    broker.set_auth_token(Some("secret".into()));
    let (addr, _h) = boot_listener(Arc::clone(&broker)).await;

    let resps = rpc_seq(
        &addr,
        &[Request::Auth {
            token: "wrong".into(),
        }],
    )
    .await;
    match &resps[0] {
        Response::Auth { error_code } => {
            assert_eq!(*error_code, ErrorCode::AuthenticationFailed as u16);
        }
        other => panic!("expected Auth 17, got {other:?}"),
    }
}

#[tokio::test]
async fn note_inter_broker_principal_allows_without_cluster_alter() {
    let (broker, _g) = new_single_broker("p133", "ib");
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker.create_topic("t", 1).unwrap();
    let (addr, _h) = boot_listener(Arc::clone(&broker)).await;

    let resp = inter_broker_rpc(&broker, &addr, &note_req("t", 40, 0))
        .await
        .expect("rpc");
    match resp {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, 0);
            assert!(generation >= 1);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), Some(40));
}

#[tokio::test]
async fn note_wrong_principal_denied_no_watermark() {
    let (broker, _g) = new_single_broker("p133", "eve");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker.create_topic("t", 1).unwrap();
    let gen_before = broker.truncate_journal().generation();

    let resp = dispatch_request_as(&broker, note_req("t", 77, 0), Some("eve")).await;
    match resp {
        Response::Error { code, message } => {
            assert_eq!(code, ErrorCode::AuthorizationFailed as u16);
            assert!(message.contains("not authorized"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), None);
    assert_eq!(broker.truncate_journal().generation(), gen_before);
}

#[tokio::test]
async fn note_cluster_alter_allows_non_ib_principal() {
    let (broker, _g) = new_single_broker("p133", "alice");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker.acls().create(vec![cluster_alter("alice")]).unwrap();
    broker.create_topic("t", 1).unwrap();

    let resp = dispatch_request_as(&broker, note_req("t", 55, 0), Some("alice")).await;
    match resp {
        Response::TruncateJournalNote { error_code, .. } => assert_eq!(error_code, 0),
        other => panic!("{other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), Some(55));
}

#[tokio::test]
async fn push_wrong_principal_denied() {
    let (broker, _g) = new_single_broker("p133", "push-eve");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let resp = dispatch_request_as(
        &broker,
        push_req(1, Bytes::from_static(b"{}")),
        Some("eve"),
    )
    .await;
    match resp {
        Response::Error { code, .. } => {
            assert_eq!(code, ErrorCode::AuthorizationFailed as u16);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(broker.truncate_journal().generation(), 0);
}

#[tokio::test]
async fn push_inter_broker_principal_allows() {
    let (src, _gs) = new_single_broker("p133", "push-src");
    src.create_topic("t", 1).unwrap();
    let gen = src.local_note_truncate_journal("t", 0, 64, 0);
    let snap = Bytes::from(src.truncate_journal().snapshot_bytes());

    let (dst, _gd) = new_single_broker("p133", "push-dst");
    // Phase 137: push filters unknown topics; topic must exist on dst for watermark apply.
    dst.create_topic("t", 1).unwrap();
    dst.set_auth_token(Some("secret".into()));
    dst.configure_acls(true, None, vec![], "token".into())
        .unwrap();
    let (addr, _h) = boot_listener(Arc::clone(&dst)).await;

    let resp = inter_broker_rpc(&dst, &addr, &push_req(gen, snap))
        .await
        .expect("rpc");
    match resp {
        Response::TruncateJournalPush { error_code } => assert_eq!(error_code, 0),
        other => panic!("{other:?}"),
    }
    assert_eq!(dst.truncate_journal().watermark("t", 0), Some(64));
}

// Keep Broker import used via type inference from helpers.
#[allow(dead_code)]
fn _types(_: Arc<Broker>) {}
