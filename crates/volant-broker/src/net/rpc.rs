//! Inter-broker RPC client, TLS connect, and timeout knobs.

use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use volant_core::{Error, Result};
use volant_protocol::codec::{decode_frame, encode_frame};
use volant_protocol::{pack_request, Request, Response};

use crate::broker::Broker;

/// Default per inter-broker RPC timeout: **5 seconds**.
///
/// Override with `VOLANT_INTER_BROKER_RPC_TIMEOUT_MS` (milliseconds). Values
/// are clamped to `[1ms, 10min]`; `0` is not “disable” — it becomes 1ms so a
/// deadline always exists.
pub const DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS: u64 = 5_000;

/// Minimum clamp for env-derived timeouts (ms). `0` and empty-invalid still become this.
pub const MIN_INTER_BROKER_TIMEOUT_MS: u64 = 1;
/// Maximum clamp for per-RPC and fan-out budget env values (10 minutes).
pub const MAX_INTER_BROKER_TIMEOUT_MS: u64 = 600_000;

/// Default overall budget for DeleteRecords peer fan-out (journal note + push
/// + ReplicaDeleteRecords): **20 seconds** (≥ 3 × default 5s RPC + margin).
///
/// Override with `VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS` (milliseconds).
/// Values are clamped to `[1ms, 10min]`; `0` is not “disable”. When the env is
/// **unset**, the effective budget is
/// `max(DEFAULT, 3 * inter_broker_rpc_timeout_ms + 2000)` (also clamped to
/// the max) so a raised per-RPC timeout still leaves room for all three phases.
pub const DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS: u64 = 20_000;

fn env_duration_ms(var: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(default_ms)
        .clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Effective per-RPC timeout (default 5s; env `VOLANT_INTER_BROKER_RPC_TIMEOUT_MS`).
///
/// Clamped to [`MIN_INTER_BROKER_TIMEOUT_MS`]..=[`MAX_INTER_BROKER_TIMEOUT_MS`].
pub fn inter_broker_rpc_timeout() -> Duration {
    env_duration_ms(
        "VOLANT_INTER_BROKER_RPC_TIMEOUT_MS",
        DEFAULT_INTER_BROKER_RPC_TIMEOUT_MS,
    )
}

/// Effective DeleteRecords fan-out overall budget.
///
/// - Env `VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS` set → that value, clamped to
///   `[1ms, 10min]` (`0` is not disable).
/// - Else → `max(DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS,
///   3 * inter_broker_rpc_timeout_ms + 2000)`, also clamped to the max.
pub fn delete_records_fanout_budget() -> Duration {
    if let Ok(s) = std::env::var("VOLANT_DELETE_RECORDS_FANOUT_BUDGET_MS") {
        if let Ok(ms) = s.parse::<u64>() {
            return Duration::from_millis(
                ms.clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS),
            );
        }
    }
    let rpc_ms = inter_broker_rpc_timeout().as_millis() as u64;
    let floor = (3u64.saturating_mul(rpc_ms)).saturating_add(2_000);
    let ms = DEFAULT_DELETE_RECORDS_FANOUT_BUDGET_MS
        .max(floor)
        .clamp(MIN_INTER_BROKER_TIMEOUT_MS, MAX_INTER_BROKER_TIMEOUT_MS);
    Duration::from_millis(ms)
}

/// Inter-broker RPC over a short-lived connection (plain TCP or optional TLS).
///
/// When the local broker has an auth token configured, sends Auth first.
/// When [`Broker::inter_broker_tls`] is set (and the `tls` feature is enabled),
/// the connection is upgraded to TLS before the RPC.
///
/// Bounded by [`inter_broker_rpc_timeout`] (default **5s**) so a black-holed
/// peer cannot stall client paths that await fan-out.
pub async fn inter_broker_rpc(broker: &Broker, addr: &str, req: &Request) -> Result<Response> {
    if broker.test_inter_broker_blocked() {
        return Err(Error::Protocol("inter-broker rpc blocked".into()));
    }
    inter_broker_rpc_owned(addr, req, broker.auth_token(), broker.inter_broker_tls()).await
}

/// Owned-credentials RPC (Send-safe for parallel `JoinSet` fan-out).
pub(super) async fn inter_broker_rpc_owned(
    addr: &str,
    req: &Request,
    auth_token: Option<String>,
    inter_broker_tls: Option<crate::broker::InterBrokerTls>,
) -> Result<Response> {
    let timeout = inter_broker_rpc_timeout();
    match tokio::time::timeout(
        timeout,
        inter_broker_rpc_inner(addr, req, auth_token, inter_broker_tls),
    )
    .await
    {
        Ok(r) => r,
        Err(_) => Err(Error::Protocol(format!(
            "inter-broker rpc to {addr} timed out after {}ms",
            timeout.as_millis()
        ))),
    }
}

async fn inter_broker_rpc_inner(
    addr: &str,
    req: &Request,
    auth_token: Option<String>,
    inter_broker_tls: Option<crate::broker::InterBrokerTls>,
) -> Result<Response> {
    let tcp = TcpStream::connect(addr).await?;

    #[cfg(feature = "tls")]
    if let Some(tls_cfg) = inter_broker_tls {
        let mut stream = connect_inter_broker_tls(tcp, addr, &tls_cfg).await?;
        return inter_broker_rpc_on(&mut stream, req, auth_token).await;
    }

    #[cfg(not(feature = "tls"))]
    if inter_broker_tls.is_some() {
        return Err(Error::InvalidArgument(
            "inter-broker TLS configured but volant-broker was built without `--features tls`"
                .into(),
        ));
    }

    let mut stream = tcp;
    inter_broker_rpc_on(&mut stream, req, auth_token).await
}

async fn inter_broker_rpc_on<S>(
    stream: &mut S,
    req: &Request,
    auth_token: Option<String>,
) -> Result<Response>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if let Some(token) = auth_token {
        let auth = Request::Auth { token };
        let frame = pack_request(0, &auth)?;
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out)?;
        stream.write_all(&out).await?;
        let auth_resp = read_one_response(stream).await?;
        match auth_resp {
            Response::Auth { error_code } if error_code == 0 => {}
            Response::Auth { error_code } => {
                return Err(Error::Protocol(format!(
                    "inter-broker auth failed: error_code={error_code}"
                )));
            }
            Response::Error { code, message } => {
                return Err(Error::Protocol(format!(
                    "inter-broker auth error {code}: {message}"
                )));
            }
            other => {
                return Err(Error::Protocol(format!(
                    "unexpected inter-broker auth response: {other:?}"
                )));
            }
        }
    }

    let frame = pack_request(1, req)?;
    let mut out = BytesMut::new();
    encode_frame(&frame, &mut out)?;
    stream.write_all(&out).await?;
    read_one_response(stream).await
}

async fn read_one_response<S>(stream: &mut S) -> Result<Response>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(Error::Protocol("peer closed during rpc".into()));
        }
        if let Some(frame) = decode_frame(&mut buf)? {
            return volant_protocol::decode_response(frame.header.opcode, &frame.payload);
        }
    }
}

/// Build a TLS client stream for inter-broker RPC.
#[cfg(feature = "tls")]
async fn connect_inter_broker_tls(
    tcp: TcpStream,
    addr: &str,
    cfg: &crate::broker::InterBrokerTls,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use rustls::pki_types::ServerName;
    use rustls::ClientConfig as RustlsClientConfig;
    use rustls::RootCertStore;
    use tokio_rustls::TlsConnector;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);

    let builder = if cfg.insecure {
        RustlsClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
    } else {
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(ca_path) = &cfg.ca_path {
            let file = File::open(ca_path).map_err(|e| {
                Error::InvalidArgument(format!(
                    "open inter-broker tls_ca {}: {e}",
                    ca_path.display()
                ))
            })?;
            let mut reader = BufReader::new(file);
            let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    Error::InvalidArgument(format!("parse inter-broker tls_ca PEM: {e}"))
                })?;
            for cert in certs {
                roots.add(cert).map_err(|e| {
                    Error::InvalidArgument(format!("add inter-broker tls_ca cert: {e}"))
                })?;
            }
        }
        RustlsClientConfig::builder().with_root_certificates(roots)
    };

    // Phase 19: optional client cert for mTLS to peers.
    let rustls_config = match (&cfg.client_cert, &cfg.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_pem_certs(cert_path)?;
            let key = load_pem_key(key_path)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::InvalidArgument(format!("inter-broker client cert: {e}")))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(Error::InvalidArgument(
                "inter-broker TLS client_cert and client_key must both be set or both unset".into(),
            ));
        }
    };

    let connector = TlsConnector::from(Arc::new(rustls_config));
    let server_name = ServerName::try_from(host.to_owned()).map_err(|e| {
        Error::InvalidArgument(format!(
            "invalid inter-broker TLS server name '{host}': {e}"
        ))
    })?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))
}

#[cfg(feature = "tls")]
fn load_pem_certs(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path)
        .map_err(|e| Error::InvalidArgument(format!("open cert {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::InvalidArgument(format!("parse cert PEM {}: {e}", path.display())))
}

#[cfg(feature = "tls")]
fn load_pem_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use std::fs::File;
    use std::io::BufReader;
    let file = File::open(path)
        .map_err(|e| Error::InvalidArgument(format!("open key {}: {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::InvalidArgument(format!("parse key PEM {}: {e}", path.display())))?
        .ok_or_else(|| Error::InvalidArgument(format!("no private key in {}", path.display())))
}

#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoCertVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
