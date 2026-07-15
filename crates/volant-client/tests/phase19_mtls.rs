//! Phase 19: mTLS client certificate authentication.
//!
//! Requires openssl on PATH and `--features tls`.

#![cfg(feature = "tls")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::net::TcpListener;
use volant_broker::Broker;
use volant_client::{Client, ClientConfig};
use volant_core::{Message, Offset};
use volant_storage::StorageConfig;

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "volant-p19-{label}-{}-{}",
        std::process::id(),
        nanos
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct CertBundle {
    dir: PathBuf,
    ca_crt: PathBuf,
    server_crt: PathBuf,
    server_key: PathBuf,
    client_crt: PathBuf,
    client_key: PathBuf,
    other_crt: PathBuf,
    other_key: PathBuf,
}

fn openssl_ok() -> bool {
    Command::new("openssl")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn gen_certs() -> Option<CertBundle> {
    if !openssl_ok() {
        eprintln!("openssl not available; skipping mTLS tests");
        return None;
    }
    let dir = temp_dir("certs");
    let ca_key = dir.join("ca.key");
    let ca_crt = dir.join("ca.crt");
    let server_key = dir.join("server.key");
    let server_csr = dir.join("server.csr");
    let server_crt = dir.join("server.crt");
    let client_key = dir.join("client.key");
    let client_csr = dir.join("client.csr");
    let client_crt = dir.join("client.crt");
    let other_key = dir.join("other.key");
    let other_csr = dir.join("other.csr");
    let other_crt = dir.join("other.crt");

    run_openssl(&[
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", ca_key.to_str().unwrap(),
        "-out", ca_crt.to_str().unwrap(),
        "-days", "1", "-subj", "/CN=volant-test-ca",
    ])?;

    // Server cert signed by CA (also usable as inter-broker client cert).
    run_openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", server_key.to_str().unwrap(),
        "-out", server_csr.to_str().unwrap(),
        "-subj", "/CN=localhost",
    ])?;
    run_openssl(&[
        "x509", "-req",
        "-in", server_csr.to_str().unwrap(),
        "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-out", server_crt.to_str().unwrap(),
        "-days", "1",
    ])?;

    run_openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", client_key.to_str().unwrap(),
        "-out", client_csr.to_str().unwrap(),
        "-subj", "/CN=alice",
    ])?;
    run_openssl(&[
        "x509", "-req",
        "-in", client_csr.to_str().unwrap(),
        "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-out", client_crt.to_str().unwrap(),
        "-days", "1",
    ])?;

    run_openssl(&[
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", other_key.to_str().unwrap(),
        "-out", other_csr.to_str().unwrap(),
        "-subj", "/CN=bob-denied",
    ])?;
    run_openssl(&[
        "x509", "-req",
        "-in", other_csr.to_str().unwrap(),
        "-CA", ca_crt.to_str().unwrap(),
        "-CAkey", ca_key.to_str().unwrap(),
        "-CAcreateserial",
        "-out", other_crt.to_str().unwrap(),
        "-days", "1",
    ])?;

    Some(CertBundle {
        dir,
        ca_crt,
        server_crt,
        server_key,
        client_crt,
        client_key,
        other_crt,
        other_key,
    })
}

fn run_openssl(args: &[&str]) -> Option<()> {
    let status = Command::new("openssl").args(args).status().ok()?;
    if status.success() {
        Some(())
    } else {
        None
    }
}

/// Minimal mTLS server using the same path as volant-server TLS module logic.
async fn boot_mtls(
    data_dir: PathBuf,
    certs: &CertBundle,
    allowlist: Option<&[&str]>,
) -> (String, tokio::task::JoinHandle<()>) {
    use std::collections::HashSet;
    use std::sync::Arc as StdArc;

    // Inline server acceptor build (mirrors volant-server tls::build_acceptor).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server_certs = load_certs(&certs.server_crt);
    let server_key = load_key(&certs.server_key);
    let mut roots = rustls::RootCertStore::empty();
    for c in load_certs(&certs.ca_crt) {
        roots.add(c).unwrap();
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(StdArc::new(roots))
        .build()
        .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certs, server_key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(StdArc::new(config));

    let allow: Option<HashSet<String>> =
        allowlist.map(|list| list.iter().map(|s| (*s).to_owned()).collect());

    let broker = Arc::new(Broker::new(StorageConfig {
        data_dir,
        ..StorageConfig::default()
    }));
    // No shared token — auth is mTLS only.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let b = Arc::clone(&broker);
    let handle = tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                break;
            };
            let acc = acceptor.clone();
            let b = Arc::clone(&b);
            let allow = allow.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acc.accept(tcp).await else {
                    return;
                };
                // Identity + auth gate (simplified copy of server path).
                let principal = {
                    let (_, conn) = tls.get_ref();
                    conn.peer_certificates()
                        .and_then(|c| c.first())
                        .and_then(|leaf| principal_cn(leaf.as_ref()))
                };
                let mtls_ok = match (&allow, &principal) {
                    (None, Some(_)) => true,
                    (Some(list), Some(_cn)) if list.is_empty() => true,
                    (Some(list), Some(cn)) => list.contains(cn),
                    _ => false,
                };
                let _ = handle_conn(&mut tls, b, mtls_ok).await;
            });
        }
    });
    tokio::task::yield_now().await;
    (format!("127.0.0.1:{}", addr.port()), handle)
}

fn load_certs(path: &Path) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn load_key(path: &Path) -> rustls::pki_types::PrivateKeyDer<'static> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .unwrap()
        .expect("key")
}

fn principal_cn(der: &[u8]) -> Option<String> {
    // Minimal CN extract via openssl asn1parse is heavy; use rustls leaf + openssl x509 -noout -subject
    // Prefer parsing with a tiny approach: shell out once.
    let dir = temp_dir("cn");
    let p = dir.join("leaf.der");
    std::fs::write(&p, der).ok()?;
    let out = Command::new("openssl")
        .args(["x509", "-inform", "DER", "-noout", "-subject", "-nameopt", "RFC2253"])
        .arg("-in")
        .arg(&p)
        .output()
        .ok()?;
    let _ = std::fs::remove_dir_all(&dir);
    let s = String::from_utf8_lossy(&out.stdout);
    // subject=CN=alice
    for part in s.split([',', '/', '\n']) {
        let part = part.trim();
        if let Some(cn) = part.strip_prefix("CN=") {
            return Some(cn.trim().to_owned());
        }
        if let Some(cn) = part.strip_prefix("subject=CN=") {
            return Some(cn.trim().to_owned());
        }
    }
    None
}

async fn handle_conn<S>(
    stream: &mut S,
    broker: Arc<Broker>,
    mut authenticated: bool,
) -> volant_core::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use bytes::BytesMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use volant_protocol::codec::{decode_frame, encode_frame};
    use volant_protocol::{decode_request, pack_response, ErrorCode, Request, Response};

    let mut buf = BytesMut::with_capacity(8 * 1024);
    let auth_required = true; // mTLS mode
    loop {
        loop {
            match decode_frame(&mut buf)? {
                Some(frame) => {
                    let corr = frame.header.correlation_id;
                    let req = match decode_request(frame.header.opcode, &frame.payload) {
                        Ok(r) => r,
                        Err(e) => {
                            let response = Response::Error {
                                code: ErrorCode::Protocol as u16,
                                message: e.to_string(),
                            };
                            let packed = pack_response(corr, &response)?;
                            let mut out = BytesMut::new();
                            encode_frame(&packed, &mut out)?;
                            stream.write_all(&out).await?;
                            continue;
                        }
                    };
                    if let Request::Auth { .. } = &req {
                        // No token configured in this test server.
                        authenticated = true;
                        let response = Response::Auth { error_code: 0 };
                        let packed = pack_response(corr, &response)?;
                        let mut out = BytesMut::new();
                        encode_frame(&packed, &mut out)?;
                        stream.write_all(&out).await?;
                        continue;
                    }
                    let response = if auth_required && !authenticated {
                        Response::Error {
                            code: ErrorCode::AuthenticationRequired as u16,
                            message: "mTLS identity not allowlisted".into(),
                        }
                    } else {
                        volant_broker::net::dispatch_request(&broker, req).await
                    };
                    let packed = pack_response(corr, &response)?;
                    let mut out = BytesMut::new();
                    encode_frame(&packed, &mut out)?;
                    stream.write_all(&out).await?;
                }
                None => break,
            }
        }
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn mtls_client_can_produce_without_token() {
    let Some(certs) = gen_certs() else {
        return;
    };
    let dir = temp_dir("ok");
    let (addr, server) = boot_mtls(dir.clone(), &certs, None).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        tls: true,
        tls_insecure: true, // lab: skip server name verify against self-signed
        tls_ca: Some(certs.ca_crt.clone()),
        tls_cert: Some(certs.client_crt.clone()),
        tls_key: Some(certs.client_key.clone()),
        ..ClientConfig::default()
    })
    .await
    .expect("connect with client cert");

    client.create_topic("t", 1).await.expect("create");
    client
        .produce(
            "t",
            Some(0),
            vec![Message::from_value(Bytes::from_static(b"hello"))],
        )
        .await
        .expect("produce");
    let f = client
        .fetch("t", 0, Offset::ZERO, 10, 0)
        .await
        .expect("fetch");
    assert_eq!(f.records.len(), 1);

    server.abort();
    let _ = std::fs::remove_dir_all(&certs.dir);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mtls_without_client_cert_fails_handshake() {
    let Some(certs) = gen_certs() else {
        return;
    };
    let dir = temp_dir("nocert");
    let (addr, server) = boot_mtls(dir.clone(), &certs, None).await;

    // TLS 1.3: client may finish connect() before the server rejects a missing
    // client cert (server Finished is earlier in the flight). Failure surfaces
    // on the first application read/write, or occasionally on connect itself.
    let connect = Client::connect(ClientConfig {
        brokers: vec![addr],
        tls: true,
        tls_insecure: true,
        tls_ca: Some(certs.ca_crt.clone()),
        // no client cert
        ..ClientConfig::default()
    })
    .await;

    match connect {
        Err(_) => { /* handshake rejected promptly — ok */ }
        Ok(client) => {
            let err = client.create_topic("t", 1).await;
            assert!(
                err.is_err(),
                "expected failure without client cert (connect ok under TLS 1.3, RPC must fail)"
            );
        }
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&certs.dir);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mtls_allowlist_rejects_unknown_cn() {
    let Some(certs) = gen_certs() else {
        return;
    };
    let dir = temp_dir("deny");
    // Only alice allowed; connect as bob-denied.
    let (addr, server) = boot_mtls(dir.clone(), &certs, Some(&["alice"])).await;

    let client = Client::connect(ClientConfig {
        brokers: vec![addr],
        tls: true,
        tls_insecure: true,
        tls_ca: Some(certs.ca_crt.clone()),
        tls_cert: Some(certs.other_crt.clone()),
        tls_key: Some(certs.other_key.clone()),
        ..ClientConfig::default()
    })
    .await
    .expect("handshake ok (cert valid) but not allowlisted");

    let err = client.create_topic("t", 1).await;
    assert!(err.is_err(), "expected auth failure for non-allowlisted CN");

    server.abort();
    let _ = std::fs::remove_dir_all(&certs.dir);
    let _ = std::fs::remove_dir_all(&dir);
}
