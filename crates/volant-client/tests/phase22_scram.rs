//! Phase 22: SCRAM-SHA-256 authentication.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::{serve_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType};
use volant_client::{Client, ClientConfig};
use volant_core::Message;
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p22-{label}-{}-{}",
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

#[tokio::test]
async fn scram_wrong_password_fails() {
    let dir = temp_dir("bad-pass");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    assert!(broker.auth_required());
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let err = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        scram_username: Some("alice".into()),
        scram_password: Some("wrong".into()),
        ..ClientConfig::default()
    })
    .await;
    assert!(err.is_err(), "wrong password must fail");

    // Correct password works.
    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        scram_username: Some("alice".into()),
        scram_password: Some("s3cret".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("scram ok");
    client.create_topic("t", 1).await.expect("create");
    client
        .produce(
            "t",
            None,
            vec![Message {
                key: None,
                value: Bytes::from_static(b"hi"),
                timestamp_ms: None,
                headers: vec![],
            }],
        )
        .await
        .expect("produce");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn scram_users_survive_restart() {
    let dir = temp_dir("restart");
    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: dir.clone(),
            ..StorageConfig::default()
        }));
        broker.upsert_scram_user("bob", "pw").unwrap();
        assert_eq!(broker.scram().list_usernames(), vec!["bob".to_string()]);
    }
    let broker2 = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    assert!(broker2.scram().has_users());
    assert_eq!(broker2.scram().list_usernames(), vec!["bob".to_string()]);

    let (addr, server) = boot(Arc::clone(&broker2)).await;
    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        scram_username: Some("bob".into()),
        scram_password: Some("pw".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("scram after restart");
    client.create_topic("t2", 1).await.expect("create");
    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn bootstrap_create_scram_user_when_empty() {
    let dir = temp_dir("bootstrap");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    assert!(!broker.auth_required());
    let (addr, server) = boot(Arc::clone(&broker)).await;

    // Unauthenticated bootstrap create.
    let admin = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin
        .create_scram_user("carol", "secret", 0)
        .await
        .expect("bootstrap create");
    assert!(broker.scram().has_users());

    // Unauthenticated ops now fail.
    let bare = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    let err = bare.create_topic("x", 1).await;
    assert!(err.is_err(), "auth required after users exist");

    // SCRAM login works.
    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        scram_username: Some("carol".into()),
        scram_password: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("scram");
    client.create_topic("x", 1).await.expect("create as carol");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn scram_principal_feeds_acls() {
    let dir = temp_dir("acl");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    broker
        .configure_acls(true, None, vec![], "token".into())
        .unwrap();
    broker
        .acls()
        .create(vec![AclEntry {
            principal: "alice".into(),
            resource_type: ResourceType::Topic,
            resource: "allowed".into(),
            operation: AclOperation::All,
            permission: AclPermission::Allow,
        }])
        .unwrap();
    // Cluster create for topic create needs Create on topic (already All on allowed only).
    // Also allow CreateTopic on "allowed" — Create uses topic name.
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        scram_username: Some("alice".into()),
        scram_password: Some("s3cret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();

    client
        .create_topic("allowed", 1)
        .await
        .expect("alice can create allowed");
    let deny = client.create_topic("denied", 1).await;
    assert!(deny.is_err(), "alice cannot create denied topic");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn shared_token_still_works_with_scram_users() {
    let dir = temp_dir("token");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_auth_token(Some("shared".into()));
    broker.upsert_scram_user("alice", "s3cret").unwrap();
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let via_token = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        auth_token: Some("shared".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("token auth");
    via_token.create_topic("t", 1).await.expect("create via token");

    let via_scram = Client::connect(ClientConfig {
        brokers: vec![addr],
        scram_username: Some("alice".into()),
        scram_password: Some("s3cret".into()),
        ..ClientConfig::default()
    })
    .await
    .expect("scram auth");
    via_scram
        .produce(
            "t",
            Some(0),
            vec![Message {
                key: None,
                value: Bytes::from_static(b"x"),
                timestamp_ms: None,
                headers: vec![],
            }],
        )
        .await
        .expect("produce via scram");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn list_create_delete_scram_admin() {
    let dir = temp_dir("admin");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    let (addr, server) = boot(Arc::clone(&broker)).await;

    let admin = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    admin
        .create_scram_user("u1", "p1", 0)
        .await
        .expect("bootstrap");

    // After users exist, admin needs SCRAM (or token).
    let admin2 = Client::connect(ClientConfig {
        brokers: vec![addr.clone()],
        scram_username: Some("u1".into()),
        scram_password: Some("p1".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    let listed = admin2.list_scram_users().await.expect("list");
    assert_eq!(listed, vec!["u1".to_string()]);
    admin2
        .create_scram_user("u2", "p2", 4096)
        .await
        .expect("create second");
    admin2.delete_scram_user("u2").await.expect("delete u2");
    let listed = admin2.list_scram_users().await.unwrap();
    assert_eq!(listed, vec!["u1".to_string()]);

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
