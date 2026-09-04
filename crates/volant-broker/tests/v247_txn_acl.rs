//! v0.247: ACL TransactionalId on txn APIs.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use common::{boot_kafka, rpc, temp_dir};
use tokio::net::TcpListener;
use volant_broker::kafka::codec::{
    encode_request, encode_request_flexible, get_compact_array_len, get_compact_nullable_string,
    put_compact_array_len, put_compact_string, put_empty_tag_buffer, put_nullable_string,
    skip_tag_buffer,
};
use volant_broker::{
    serve_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType, CLUSTER_RESOURCE,
};
use volant_client::{Client, ClientConfig};
use volant_protocol::AclBinding;
use volant_storage::StorageConfig;

const KAFKA_RT_TRANSACTIONAL_ID: i8 = 5;
const NATIVE_RT_TRANSACTIONAL_ID: u8 = 4;
const KAFKA_OP_WRITE: i8 = 4;
const KAFKA_PERM_ALLOW: i8 = 3;

fn seed_cluster_admin(broker: &Broker) {
    broker
        .acls()
        .create(vec![
            AclEntry {
                principal: "*".into(),
                resource_type: ResourceType::Cluster,
                resource: CLUSTER_RESOURCE.into(),
                operation: AclOperation::Alter,
                permission: AclPermission::Allow,
            },
            AclEntry {
                principal: "*".into(),
                resource_type: ResourceType::Cluster,
                resource: CLUSTER_RESOURCE.into(),
                operation: AclOperation::Describe,
                permission: AclPermission::Allow,
            },
            AclEntry {
                principal: "*".into(),
                resource_type: ResourceType::Cluster,
                resource: CLUSTER_RESOURCE.into(),
                operation: AclOperation::Write,
                permission: AclPermission::Allow,
            },
        ])
        .unwrap();
}

fn create_acls_flex(
    resource_type: i8,
    resource: &str,
    principal: &str,
    op: i8,
    perm: i8,
) -> BytesMut {
    let mut body = BytesMut::new();
    put_compact_array_len(&mut body, 1);
    body.put_i8(resource_type);
    put_compact_string(&mut body, resource);
    body.put_i8(3); // LITERAL
    put_compact_string(&mut body, principal);
    put_compact_string(&mut body, "*");
    body.put_i8(op);
    body.put_i8(perm);
    put_empty_tag_buffer(&mut body);
    put_empty_tag_buffer(&mut body);
    body
}

fn init_txn_body(txn_id: &str) -> BytesMut {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, Some(txn_id));
    body.put_i32(60_000);
    body
}

fn init_error(src: &mut impl Buf, corr: i32) -> i16 {
    assert_eq!(src.get_i32(), corr);
    assert_eq!(src.get_i32(), 0); // throttle
    src.get_i16()
}

fn is_txn_auth_failed(err: i16) -> bool {
    // TopicAuthorizationFailed (29) or TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53).
    err == 29 || err == 53
}

async fn boot_native(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

#[tokio::test]
async fn kafka_init_producer_id_denied_without_transactional_id_grant() {
    let dir = temp_dir("v247", "deny");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    // CreateAcls accepts Kafka ResourceType TransactionalId (5) even when
    // the binding is for a different id (no grant on "txn-1").
    let created = rpc(
        &addr,
        encode_request_flexible(
            30,
            2,
            1,
            Some("a"),
            &create_acls_flex(
                KAFKA_RT_TRANSACTIONAL_ID,
                "other-txn",
                "User:kafka-anonymous",
                KAFKA_OP_WRITE,
                KAFKA_PERM_ALLOW,
            ),
        ),
    )
    .await;
    let mut cs = created.freeze();
    assert_eq!(cs.get_i32(), 1);
    skip_tag_buffer(&mut cs).unwrap();
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut cs).unwrap(), Some(1));
    assert_eq!(cs.get_i16(), 0);

    let listed = broker
        .acls()
        .list(None, Some(ResourceType::TransactionalId), Some("other-txn"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_type, ResourceType::TransactionalId);

    let resp = rpc(
        &addr,
        encode_request(22, 0, 2, Some("p"), &init_txn_body("txn-1")),
    )
    .await;
    let mut src = resp.freeze();
    let err = init_error(&mut src, 2);
    assert!(is_txn_auth_failed(err), "expected 29 or 53, got {err}");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn kafka_init_producer_id_ok_with_transactional_id_grant() {
    let dir = temp_dir("v247", "allow");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    seed_cluster_admin(&broker);
    let (addr, server) = boot_kafka(Arc::clone(&broker)).await;

    let created = rpc(
        &addr,
        encode_request_flexible(
            30,
            2,
            10,
            Some("a"),
            &create_acls_flex(
                KAFKA_RT_TRANSACTIONAL_ID,
                "txn-1",
                "User:kafka-anonymous",
                KAFKA_OP_WRITE,
                KAFKA_PERM_ALLOW,
            ),
        ),
    )
    .await;
    let mut cs = created.freeze();
    assert_eq!(cs.get_i32(), 10);
    skip_tag_buffer(&mut cs).unwrap();
    assert_eq!(cs.get_i32(), 0);
    assert_eq!(get_compact_array_len(&mut cs).unwrap(), Some(1));
    assert_eq!(cs.get_i16(), 0);
    let _ = get_compact_nullable_string(&mut cs);

    let listed = broker
        .acls()
        .list(None, Some(ResourceType::TransactionalId), Some("txn-1"));
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_type, ResourceType::TransactionalId);
    assert_eq!(listed[0].resource, "txn-1");

    let resp = rpc(
        &addr,
        encode_request(22, 0, 11, Some("p"), &init_txn_body("txn-1")),
    )
    .await;
    let mut src = resp.freeze();
    let err = init_error(&mut src, 11);
    assert_eq!(err, 0);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn native_list_create_acl_type_name_transactional_id() {
    assert_eq!(
        ResourceType::parse("transactionalid").unwrap(),
        ResourceType::TransactionalId
    );
    assert_eq!(
        ResourceType::parse("TransactionalId").unwrap(),
        ResourceType::TransactionalId
    );
    assert_eq!(ResourceType::TransactionalId.as_str(), "TransactionalId");
    assert_eq!(
        ResourceType::TransactionalId.as_u8(),
        NATIVE_RT_TRANSACTIONAL_ID
    );

    let dir = temp_dir("v247", "native");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec!["admin".into()], "admin".into())
        .unwrap();
    let (addr, server) = boot_native(Arc::clone(&broker)).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    client
        .create_acls(vec![AclBinding {
            principal: "alice".into(),
            resource_type: NATIVE_RT_TRANSACTIONAL_ID,
            resource: "txn-native".into(),
            operation: 2,  // Write
            permission: 1, // Allow
        }])
        .await
        .expect("native CreateAcls TransactionalId");

    let listed = client
        .list_acls("alice", NATIVE_RT_TRANSACTIONAL_ID, "txn-native")
        .await
        .expect("native ListAcls TransactionalId");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].resource_type, NATIVE_RT_TRANSACTIONAL_ID);
    let rt = ResourceType::from_u8(listed[0].resource_type).unwrap();
    assert_eq!(rt.as_str(), "TransactionalId");
    assert_eq!(ResourceType::parse("transactionalid").unwrap(), rt);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
