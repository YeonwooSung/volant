//! Phase 20: principal-based ACLs.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType};
use volant_client::{Client, ClientConfig};
use volant_core::Message;
use volant_protocol::AclBinding;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p20-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn boot(broker: Arc<Broker>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        serve_listener(listener, broker).await.ok();
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

fn entry(
    principal: &str,
    rt: ResourceType,
    resource: &str,
    op: AclOperation,
    perm: AclPermission,
) -> AclEntry {
    AclEntry {
        principal: principal.into(),
        resource_type: rt,
        resource: resource.into(),
        operation: op,
        permission: perm,
    }
}

fn binding(
    principal: &str,
    rt: u8,
    resource: &str,
    op: u8,
    perm: u8,
) -> AclBinding {
    AclBinding {
        principal: principal.into(),
        resource_type: rt,
        resource: resource.into(),
        operation: op,
        permission: perm,
    }
}

#[tokio::test]
async fn acl_deny_without_allow() {
    let dir = temp_dir("deny");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec![], "alice".into())
        .unwrap();
    // No allow rules — default deny.
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("auth ok");

    let err = client.create_topic("t", 1).await;
    assert!(err.is_err(), "expected authorization failure");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("23") || msg.to_lowercase().contains("authoriz") || msg.contains("not authorized"),
        "unexpected error: {msg}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acl_allow_produce_and_deny_other_topic() {
    let dir = temp_dir("allow");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec![], "alice".into())
        .unwrap();
    // Super path: create topic before ACLs bite admin… use super-user for setup.
    broker
        .acls()
        .create(vec![
            entry(
                "alice",
                ResourceType::Topic,
                "events",
                AclOperation::Create,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Topic,
                "events",
                AclOperation::Write,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Topic,
                "events",
                AclOperation::Read,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Cluster,
                "volant",
                AclOperation::Describe,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Cluster,
                "volant",
                AclOperation::Write,
                AclPermission::Allow,
            ),
        ])
        .unwrap();

    let (addr, server) = boot(Arc::clone(&broker)).await;
    let client = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    client.create_topic("events", 1).await.expect("create events");
    client
        .produce(
            "events",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"hi"))],
        )
        .await
        .expect("produce events");

    let err = client.create_topic("other", 1).await;
    assert!(err.is_err(), "other topic create should fail");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acl_deny_overrides_allow_and_super_user() {
    let dir = temp_dir("deny-over");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec!["root".into()], "alice".into())
        .unwrap();
    broker
        .acls()
        .create(vec![
            entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Create,
                AclPermission::Allow,
            ),
            entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Create,
                AclPermission::Deny,
            ),
            entry(
                "alice",
                ResourceType::Topic,
                "t",
                AclOperation::Write,
                AclPermission::Allow,
            ),
            entry(
                "root",
                ResourceType::Topic,
                "*",
                AclOperation::All,
                AclPermission::Allow,
            ),
            entry(
                "root",
                ResourceType::Cluster,
                "volant",
                AclOperation::All,
                AclPermission::Allow,
            ),
        ])
        .unwrap();

    let (addr, server) = boot(Arc::clone(&broker)).await;

    let alice = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    assert!(alice.create_topic("t", 1).await.is_err());

    // Super-user via auth principal override: reconfigure principal name to root.
    // Use a second broker connection path: set auth principal on same broker.
    // Instead create topic via super by temporarily using broker API + then test list.
    // Root as token principal:
    drop(alice);
    broker
        .configure_acls(true, None, vec!["root".into()], "root".into())
        .unwrap();
    let root = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    root.create_topic("t", 1).await.expect("super-user create");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acl_create_list_delete_roundtrip() {
    let dir = temp_dir("crud");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(true, None, vec!["admin".into()], "admin".into())
        .unwrap();
    // admin is super-user so can manage ACLs without Cluster Alter allow.
    let (addr, server) = boot(Arc::clone(&broker)).await;
    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    let b = binding("bob", 0, "logs", 1, 1); // Topic Read Allow
    client.create_acls(vec![b.clone()]).await.expect("create");
    let listed = client.list_acls("bob", 255, "").await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0], b);
    let n = client.delete_acls(vec![b]).await.expect("delete");
    assert_eq!(n, 1);
    let listed = client.list_acls("", 255, "").await.expect("list empty");
    assert!(listed.is_empty());

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn acl_file_load() {
    let dir = temp_dir("file");
    let acl_path = dir.join("acls.json");
    std::fs::write(
        &acl_path,
        r#"[
          {"principal":"alice","resource_type":"Topic","resource":"t","operation":"Create","permission":"Allow"},
          {"principal":"alice","resource_type":"Topic","resource":"t","operation":"Write","permission":"Allow"},
          {"principal":"alice","resource_type":"Cluster","resource":"volant","operation":"Describe","permission":"Allow"},
          {"principal":"alice","resource_type":"Cluster","resource":"volant","operation":"Write","permission":"Allow"}
        ]"#,
    )
    .unwrap();

    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.join("data"),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("secret".into()));
    broker
        .configure_acls(false, Some(&acl_path), vec![], "alice".into())
        .unwrap();
    assert!(broker.acls().is_enabled());

    let (addr, server) = boot(Arc::clone(&broker)).await;
    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.create_topic("t", 1).await.expect("create from file acl");
    client
        .produce(
            "t",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"x"))],
        )
        .await
        .expect("produce");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
