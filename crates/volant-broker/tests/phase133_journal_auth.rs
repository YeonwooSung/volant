//! Phase 133: TruncateJournalNote (86) / TruncateJournalPush (88) auth + ACL gates.
//!
//! Proves production gates in `net::dispatch_with_auth` + `authorize_request`:
//! - unauth when token required → AuthenticationRequired (18); no watermark
//! - wrong Auth token → AuthenticationFailed (17)
//! - inter-broker principal (token Auth → `auth_principal_name`) allowed even
//!   with ACLs on and **no** Cluster Alter entries
//! - wrong principal → AuthorizationFailed (23); watermark/gen unchanged
//! - Cluster Alter Allow for a non-ib principal succeeds
//! - Push mirrors deny/allow for wrong principal and ib principal

use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::net::dispatch_request_as;
use volant_broker::{
    inter_broker_rpc, serve_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType,
    CLUSTER_RESOURCE,
};
use volant_protocol::{
    codec::{decode_frame, encode_frame},
    decode_response, pack_request, ErrorCode, Request, Response,
};
use volant_storage::StorageConfig;

fn unique_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "volant-p133-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Guard(PathBuf);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn new_broker(label: &str) -> (Arc<Broker>, Guard) {
    let dir = unique_dir(label);
    let guard = Guard(dir.clone());
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir,
        flush_every_n: 1,
        ..StorageConfig::default()
    }));
    (broker, guard)
}

async fn boot(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        let _ = serve_listener(listener, broker).await;
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), h)
}

/// Raw framed multi-request RPC on one TCP connection (for unauth / Auth sequences).
async fn rpc_seq(addr: &str, reqs: &[Request]) -> Vec<Response> {
    let mut stream = TcpStream::connect(addr).await.expect("tcp connect");
    let mut out_all = BytesMut::new();
    for (i, req) in reqs.iter().enumerate() {
        let frame = pack_request(i as u32, req).expect("pack_request");
        encode_frame(&frame, &mut out_all).expect("encode_frame");
    }
    stream.write_all(&out_all).await.expect("write");

    let mut buf = BytesMut::with_capacity(8 * 1024);
    let mut resps = Vec::with_capacity(reqs.len());
    while resps.len() < reqs.len() {
        if let Some(frame) = decode_frame(&mut buf).expect("decode_frame") {
            let resp = decode_response(frame.header.opcode, &frame.payload).expect("decode_response");
            resps.push(resp);
            continue;
        }
        let n = stream.read_buf(&mut buf).await.expect("read");
        if n == 0 {
            panic!(
                "connection closed after {} of {} responses",
                resps.len(),
                reqs.len()
            );
        }
    }
    resps
}

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

/// Unauthenticated TruncateJournalNote is denied when a shared token is set;
/// watermark must not rise (handler never runs).
#[tokio::test]
async fn note_unauth_denied_when_token_required() {
    let (broker, _g) = new_broker("note-unauth");
    broker.set_auth_token(Some("secret".into()));
    broker.create_topic("t", 1).unwrap();
    let (addr, _h) = boot(Arc::clone(&broker)).await;

    let resps = rpc_seq(
        &addr,
        &[note_req("t", 40, -1)],
    )
    .await;
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        Response::Error { code, .. } => {
            assert_eq!(
                *code,
                ErrorCode::AuthenticationRequired as u16,
                "unauth note must be AuthenticationRequired(18)"
            );
        }
        other => panic!("expected Error 18, got {other:?}"),
    }
    assert_eq!(
        broker.truncate_journal().watermark("t", 0),
        None,
        "watermark must not rise when auth gate denies"
    );
}

/// Wrong shared-token Auth → AuthenticationFailed (17).
#[tokio::test]
async fn note_wrong_token_auth_failed() {
    let (broker, _g) = new_broker("wrong-token");
    broker.set_auth_token(Some("secret".into()));
    let (addr, _h) = boot(Arc::clone(&broker)).await;

    let resps = rpc_seq(
        &addr,
        &[Request::Auth {
            token: "wrong".into(),
        }],
    )
    .await;
    assert_eq!(resps.len(), 1);
    match &resps[0] {
        Response::Auth { error_code } => {
            assert_eq!(
                *error_code,
                ErrorCode::AuthenticationFailed as u16,
                "wrong token must be AuthenticationFailed(17)"
            );
        }
        other => panic!("expected Auth 17, got {other:?}"),
    }
}

/// Token Auth principal (`auth_principal_name`) is allowed for Note even when
/// ACLs are enabled with no Cluster Alter entries (empty rules = default deny).
#[tokio::test]
async fn note_inter_broker_principal_allows_without_cluster_alter() {
    let (broker, _g) = new_broker("note-ib");
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker.create_topic("t", 1).unwrap();
    let (addr, _h) = boot(Arc::clone(&broker)).await;

    let resp = inter_broker_rpc(
        &broker,
        &addr,
        &note_req("t", 40, 0),
    )
    .await
    .expect("inter_broker_rpc TruncateJournalNote");
    match resp {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, 0, "ib principal must be allowed");
            assert!(generation >= 1, "generation should advance on note");
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), Some(40));
}

/// Non-ib principal without Cluster Alter → AuthorizationFailed (23);
/// watermark None and generation unchanged (handler never runs).
#[tokio::test]
async fn note_wrong_principal_denied_no_watermark() {
    let (broker, _g) = new_broker("note-eve");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker.create_topic("t", 1).unwrap();

    let gen_before = broker.truncate_journal().generation();
    assert_eq!(broker.truncate_journal().watermark("t", 0), None);

    let resp = dispatch_request_as(
        &broker,
        note_req("t", 77, -1),
        Some("eve"),
    )
    .await;
    match resp {
        Response::Error { code, message } => {
            assert_eq!(
                code,
                ErrorCode::AuthorizationFailed as u16,
                "wrong principal must be AuthorizationFailed(23)"
            );
            assert!(
                message.contains("not authorized"),
                "deny message: {message}"
            );
        }
        other => panic!("expected Error 23, got {other:?}"),
    }
    assert_eq!(
        broker.truncate_journal().watermark("t", 0),
        None,
        "watermark must not rise on ACL deny"
    );
    assert_eq!(
        broker.truncate_journal().generation(),
        gen_before,
        "generation must not change on ACL deny"
    );
}

/// Cluster Alter Allow for a non-ib principal authorizes TruncateJournalNote.
#[tokio::test]
async fn note_cluster_alter_allows_non_ib_principal() {
    let (broker, _g) = new_broker("note-alice");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker
        .acls()
        .create(vec![AclEntry {
            principal: "alice".into(),
            resource_type: ResourceType::Cluster,
            resource: CLUSTER_RESOURCE.into(),
            operation: AclOperation::Alter,
            permission: AclPermission::Allow,
        }])
        .unwrap();
    broker.create_topic("t", 1).unwrap();

    let resp = dispatch_request_as(
        &broker,
        note_req("t", 55, 0),
        Some("alice"),
    )
    .await;
    match resp {
        Response::TruncateJournalNote {
            error_code,
            generation,
        } => {
            assert_eq!(error_code, 0, "alice with Cluster Alter must succeed");
            assert!(generation >= 1);
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(broker.truncate_journal().watermark("t", 0), Some(55));
}

/// TruncateJournalPush denied for wrong principal (handler never applies).
#[tokio::test]
async fn push_wrong_principal_denied() {
    let (broker, _g) = new_broker("push-eve");
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    // Snapshot content is irrelevant: ACL deny happens before handle_request.
    let resp = dispatch_request_as(
        &broker,
        push_req(1, Bytes::from_static(b"{}")),
        Some("eve"),
    )
    .await;
    match resp {
        Response::Error { code, message } => {
            assert_eq!(code, ErrorCode::AuthorizationFailed as u16);
            assert!(message.contains("not authorized"), "{message}");
        }
        other => panic!("expected Error 23, got {other:?}"),
    }
    assert_eq!(
        broker.truncate_journal().watermark("t", 0),
        None,
        "push ACL deny must not install watermark"
    );
    assert_eq!(
        broker.truncate_journal().generation(),
        0,
        "push ACL deny must not advance generation"
    );
}

/// Inter-broker principal may push a journal snapshot when ACLs are on without
/// Cluster Alter entries (token Auth → auth_principal_name).
#[tokio::test]
async fn push_inter_broker_principal_allows() {
    let (src, _gs) = new_broker("push-ib-src");
    // Trusted local note to produce a real snapshot (no ACL/auth on in-process path).
    src.create_topic("t", 1).unwrap();
    let gen = src.local_note_truncate_journal("t", 0, 64, 0);
    assert!(gen >= 1);
    let snap = Bytes::from(src.truncate_journal().snapshot_bytes());
    assert!(!snap.is_empty());

    let (dst, _gd) = new_broker("push-ib-dst");
    dst.set_auth_token(Some("secret".into()));
    dst.configure_acls(true, None, vec![], "token".into())
        .unwrap();
    // Topic need not exist for push max-merge of snapshot keys, but keep clean.
    let (addr, _h) = boot(Arc::clone(&dst)).await;

    assert_eq!(dst.truncate_journal().watermark("t", 0), None);

    let resp = inter_broker_rpc(
        &dst,
        &addr,
        &push_req(gen, snap),
    )
    .await
    .expect("inter_broker_rpc TruncateJournalPush");
    match resp {
        Response::TruncateJournalPush { error_code } => {
            assert_eq!(error_code, 0, "ib principal push must succeed");
        }
        other => panic!("unexpected response: {other:?}"),
    }
    assert_eq!(
        dst.truncate_journal().watermark("t", 0),
        Some(64),
        "ib push must install snapshot watermark"
    );
}
