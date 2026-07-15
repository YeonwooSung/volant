//! Phase 21: durable ACLs across restart + metrics Bearer auth.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use volant_broker::{
    run_metrics_server, serve_listener, AclEntry, AclOperation, AclPermission, Broker, ResourceType,
};
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
        "volant-p21-{label}-{}-{}",
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

fn binding(principal: &str, resource: &str, op: u8) -> AclBinding {
    AclBinding {
        principal: principal.into(),
        resource_type: 0, // Topic
        resource: resource.into(),
        operation: op,
        permission: 1, // Allow
    }
}

#[tokio::test]
async fn acls_survive_broker_restart() {
    let dir = temp_dir("restart");
    let data = dir.join("data");

    {
        let broker = Arc::new(Broker::new(StorageConfig {
            data_dir: data.clone(),
            ..StorageConfig::default()
        }));
        broker.set_auth_token(Some("secret".into()));
        broker
            .configure_acls(true, None, vec!["admin".into()], "admin".into())
            .unwrap();
        let (addr, server) = boot(Arc::clone(&broker)).await;
        let client = Client::connect(ClientConfig {
            brokers: vec![addr],
            auth_token: Some("secret".into()),
            ..ClientConfig::default()
        })
        .await
        .unwrap();

        client
            .create_acls(vec![
                binding("alice", "events", 3), // Create
                binding("alice", "events", 2), // Write
                binding("alice", "events", 1), // Read
            ])
            .await
            .unwrap();
        // Also need Cluster Describe/Write for metadata/init if used — alice will use own principal later.
        client
            .create_acls(vec![
                AclBinding {
                    principal: "alice".into(),
                    resource_type: 2,
                    resource: "volant".into(),
                    operation: 5, // Describe
                    permission: 1,
                },
                AclBinding {
                    principal: "alice".into(),
                    resource_type: 2,
                    resource: "volant".into(),
                    operation: 2, // Write (InitProducerId path)
                    permission: 1,
                },
            ])
            .await
            .unwrap();

        let listed = client.list_acls("alice", 255, "").await.unwrap();
        assert!(listed.len() >= 3);
        server.abort();
    }

    // Restart: new Broker on same data_dir loads __acls/acls.json
    let broker2 = Arc::new(Broker::new(StorageConfig {
        data_dir: data.clone(),
        ..StorageConfig::default()
    }));
    assert!(
        broker2.acls().is_enabled(),
        "ACLs should stay enabled after reload"
    );
    assert!(
        !broker2.acls().list(Some("alice"), None, None).is_empty(),
        "entries should reload"
    );
    // Super-users are runtime-only — re-apply for this process.
    broker2.set_auth_token(Some("secret".into()));
    broker2
        .configure_acls(false, None, vec![], "alice".into())
        .unwrap();

    let (addr, server) = boot(Arc::clone(&broker2)).await;
    let alice = Client::connect(ClientConfig {
        brokers: vec![addr],
        auth_token: Some("secret".into()),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    alice
        .create_topic("events", 1)
        .await
        .expect("create allowed after restart");
    alice
        .produce(
            "events",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"persisted"))],
        )
        .await
        .expect("produce allowed after restart");

    server.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn delete_acls_persists() {
    let dir = temp_dir("del");
    let data = dir.join("data");
    let e = AclEntry {
        principal: "bob".into(),
        resource_type: ResourceType::Topic,
        resource: "logs".into(),
        operation: AclOperation::Read,
        permission: AclPermission::Allow,
    };

    {
        let b = Broker::new(StorageConfig {
            data_dir: data.clone(),
            ..StorageConfig::default()
        });
        b.acls().create(vec![e.clone()]).unwrap();
        assert_eq!(b.acls().list(None, None, None).len(), 1);
        assert_eq!(b.acls().delete(&[e.clone()]).unwrap(), 1);
    }

    let b2 = Broker::new(StorageConfig {
        data_dir: data,
        ..StorageConfig::default()
    });
    assert!(b2.acls().list(None, None, None).is_empty());
    // enabled may still be true after deletes — enforcement on with empty = deny all
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn metrics_requires_bearer_when_configured() {
    let dir = temp_dir("metrics");
    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir: dir.clone(),
        ..StorageConfig::default()
    }));
    broker.set_metrics_token(Some("metrics-s3cret".into()));

    let mlistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let maddr = mlistener.local_addr().unwrap();
    drop(mlistener);
    let b = Arc::clone(&broker);
    tokio::spawn(async move {
        let _ = run_metrics_server(maddr, b).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // No auth → 401
    let mut stream = TcpStream::connect(maddr).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.starts_with("HTTP/1.1 401") || text.contains("401"),
        "expected 401, got: {text}"
    );
    assert!(text.contains("WWW-Authenticate") || text.contains("unauthorized"));

    // Wrong token → 401
    let mut stream = TcpStream::connect(maddr).await.unwrap();
    stream
        .write_all(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer wrong\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("401"), "expected 401 wrong token: {text}");

    // Correct Bearer → 200
    let mut stream = TcpStream::connect(maddr).await.unwrap();
    stream
        .write_all(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer metrics-s3cret\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let mut body = Vec::new();
    stream.read_to_end(&mut body).await.unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("200"), "expected 200: {text}");
    assert!(
        text.contains("volant_"),
        "expected metrics body: {text}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
