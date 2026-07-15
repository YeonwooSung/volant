//! Volant broker server entrypoint.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tracing::info;
use volant_broker::{run_metrics_server, run_server, serve_kafka_listener, Broker, ClusterConfig};
use volant_storage::StorageConfig;

/// Volant — lightweight, high-performance streaming message broker.
#[derive(Debug, Parser)]
#[command(name = "volant-server", version, about)]
struct Args {
    /// Directory for log segments and metadata.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Listen address (`host:port`).
    #[arg(long, default_value = "0.0.0.0:9092")]
    listen: String,

    /// Default number of partitions for auto-created topics (reserved).
    #[arg(long, default_value_t = 1)]
    default_partitions: u32,

    /// Static node id (required when `--cluster-config` is set).
    #[arg(long)]
    node_id: Option<u32>,

    /// Path to static `cluster.toml`. Omit for single-node mode.
    #[arg(long)]
    cluster_config: Option<PathBuf>,

    /// Optional advertised host override (defaults to listen host / cluster.toml).
    #[arg(long)]
    advertised_host: Option<String>,

    /// Optional advertised port override.
    #[arg(long)]
    advertised_port: Option<u16>,

    /// Prometheus metrics listen address (disabled if unset). Example: `127.0.0.1:9102`.
    #[arg(long)]
    metrics_addr: Option<String>,

    /// Shared token for `GET /metrics` (Phase 21). Prefer env `VOLANT_METRICS_TOKEN`.
    /// When set, scrapers must send `Authorization: Bearer <token>`.
    #[arg(long, env = "VOLANT_METRICS_TOKEN")]
    metrics_token: Option<String>,

    /// Log format: `text` (default) or `json`.
    #[arg(long, default_value = "text")]
    log_format: String,

    /// Shared auth token. Prefer env `VOLANT_AUTH_TOKEN` in production.
    #[arg(long, env = "VOLANT_AUTH_TOKEN")]
    auth_token: Option<String>,

    /// TLS certificate PEM path (requires `--features tls`).
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS private key PEM path (requires `--features tls`).
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// When server TLS is enabled, also use TLS for inter-broker RPC (default).
    /// Pass `--no-tls-inter-broker` to keep inter-broker plaintext.
    #[arg(long, default_value_t = false)]
    no_tls_inter_broker: bool,

    /// Skip inter-broker TLS peer certificate verification (default true for
    /// self-signed lab clusters). Set `--tls-peer-insecure=false` with
    /// `--tls-ca` for verified peers.
    #[arg(long, default_value_t = true)]
    tls_peer_insecure: bool,

    /// PEM CA file trusted for inter-broker (and documented for clients) when
    /// peer verification is enabled.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// PEM CA that must sign client certificates (Phase 19 mTLS). When set,
    /// clients must present a cert; verified CN authenticates the connection.
    #[arg(long)]
    tls_client_ca: Option<PathBuf>,

    /// Optional comma-separated client cert CN allowlist (Phase 19).
    /// Empty / omitted = any client cert signed by `--tls-client-ca`.
    #[arg(long)]
    tls_client_allow: Option<String>,

    /// Enable principal ACL enforcement (Phase 20). Default deny when on.
    #[arg(long, default_value_t = false)]
    acl_enable: bool,

    /// JSON file of ACL entries loaded at startup (implies ACL enable).
    #[arg(long)]
    acl_file: Option<PathBuf>,

    /// Comma-separated principals that bypass ACLs (Phase 20).
    #[arg(long)]
    acl_super_users: Option<String>,

    /// Principal name after successful shared-token Auth (default `token`).
    #[arg(long, default_value = "token")]
    auth_principal: String,

    /// Upsert a SCRAM-SHA-256 user at startup (`user:password`). Repeatable (Phase 22).
    #[arg(long = "scram-user", value_name = "USER:PASS")]
    scram_users: Vec<String>,

    /// Optional Kafka wire protocol listen address (Phase 23–27). Example: `127.0.0.1:9093`.
    /// Disabled when unset. Native Volant protocol remains on `--listen`.
    /// Produce/fetch, admin, consumer groups, configs, CreatePartitions.
    #[arg(long)]
    kafka_listen: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing(&args.log_format)?;

    #[cfg(feature = "thread-per-core")]
    let runtime = affinity::build_runtime()?;
    #[cfg(not(feature = "thread-per-core"))]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")?;

    runtime.block_on(async_main(args))
}

fn init_tracing(log_format: &str) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "volant=info".into());

    match log_format {
        "text" => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_current_span(true)
                .with_span_list(true)
                .init();
        }
        other => bail!("invalid --log-format '{other}'; expected text|json"),
    }
    Ok(())
}

async fn async_main(args: Args) -> Result<()> {
    // TLS flag validation without the feature.
    #[cfg(not(feature = "tls"))]
    {
        if args.tls_cert.is_some()
            || args.tls_key.is_some()
            || args.tls_ca.is_some()
            || args.tls_client_ca.is_some()
            || args.tls_client_allow.is_some()
            || args.no_tls_inter_broker
        {
            bail!(
                "--tls-cert/--tls-key/--tls-ca/--tls-client-ca/--tls-client-allow/\
                 --no-tls-inter-broker require building with `--features tls` \
                 (default build is plaintext-only for broad platform support)"
            );
        }
    }

    #[cfg(feature = "tls")]
    let tls_setup = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let allowlist = args.tls_client_allow.as_ref().map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_owned())
                    .filter(|p| !p.is_empty())
                    .collect::<std::collections::HashSet<_>>()
            });
            Some(tls::TlsSetup {
                acceptor: tls::build_acceptor(cert, key, args.tls_client_ca.as_deref())?,
                mtls: args.tls_client_ca.is_some(),
                allowlist,
            })
        }
        (None, None) => {
            if args.tls_client_ca.is_some() || args.tls_client_allow.is_some() {
                bail!("--tls-client-ca/--tls-client-allow require --tls-cert and --tls-key");
            }
            None
        }
        _ => bail!("both --tls-cert and --tls-key are required for TLS"),
    };

    let storage = StorageConfig {
        data_dir: args.data_dir.clone(),
        ..StorageConfig::default()
    };

    let broker = if let Some(cfg_path) = &args.cluster_config {
        let node_id = args.node_id.ok_or_else(|| {
            anyhow::anyhow!("--node-id is required when --cluster-config is set")
        })?;
        let config = ClusterConfig::load(cfg_path)
            .with_context(|| format!("load cluster config {}", cfg_path.display()))?;
        if config.broker(node_id).is_none() {
            bail!("node-id {node_id} not found in cluster config");
        }
        info!(
            node_id,
            brokers = config.brokers.len(),
            rf = config.default_replication_factor,
            "starting multi-node broker"
        );
        Arc::new(
            Broker::with_cluster(storage, node_id, config)
                .context("initialize clustered broker")?,
        )
    } else {
        if args.node_id.is_some() {
            info!("--node-id ignored without --cluster-config (single-node mode)");
        }
        Arc::new(Broker::new(storage))
    };

    if let Some(token) = args.auth_token.clone() {
        if token.is_empty() {
            bail!("--auth-token / VOLANT_AUTH_TOKEN must not be empty when set");
        }
        info!("shared-token auth enabled");
        broker.set_auth_token(Some(token));
    }

    // Phase 22 SCRAM users from flags (and any already durable under data_dir).
    for spec in &args.scram_users {
        let (user, pass) = spec.split_once(':').ok_or_else(|| {
            anyhow::anyhow!("--scram-user must be USER:PASS (got '{spec}')")
        })?;
        if user.is_empty() || pass.is_empty() {
            bail!("--scram-user USER and PASS must be non-empty");
        }
        broker
            .upsert_scram_user(user, pass)
            .with_context(|| format!("upsert SCRAM user '{user}'"))?;
        info!(username = %user, "SCRAM-SHA-256 user upserted");
    }
    if broker.scram().has_users() {
        info!(
            users = broker.scram().user_count(),
            "SCRAM-SHA-256 authentication enabled"
        );
    }

    // Phase 20 ACLs.
    {
        let supers: Vec<String> = args
            .acl_super_users
            .as_ref()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_owned())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        broker
            .configure_acls(
                args.acl_enable,
                args.acl_file.as_deref(),
                supers,
                args.auth_principal.clone(),
            )
            .context("configure ACLs")?;
        if broker.acls().is_enabled() {
            info!(
                auth_principal = %args.auth_principal,
                "principal ACL enforcement enabled"
            );
        }
    }

    // Inter-broker TLS: on when server TLS is active unless opted out.
    #[cfg(feature = "tls")]
    {
        let server_tls = tls_setup.is_some();
        let mtls = args.tls_client_ca.is_some();
        if server_tls && !args.no_tls_inter_broker {
            if !args.tls_peer_insecure && args.tls_ca.is_none() {
                info!(
                    "inter-broker TLS peer verification enabled without --tls-ca; \
                     using webpki roots only"
                );
            }
            // Phase 19: when mTLS is on, peers must present a client cert — use
            // the server identity cert (must be signed by --tls-client-ca in lab).
            let (client_cert, client_key) = if mtls {
                (args.tls_cert.clone(), args.tls_key.clone())
            } else {
                (None, None)
            };
            broker.set_inter_broker_tls(Some(volant_broker::InterBrokerTls {
                insecure: args.tls_peer_insecure,
                ca_path: args.tls_ca.clone(),
                client_cert,
                client_key,
            }));
            info!(
                peer_insecure = args.tls_peer_insecure,
                mtls,
                "inter-broker TLS enabled"
            );
        } else if server_tls && args.no_tls_inter_broker {
            info!("inter-broker TLS disabled (--no-tls-inter-broker); peers use plaintext");
        }
        if mtls {
            info!("mTLS client certificate auth enabled (--tls-client-ca)");
        }
    }

    // Silence unused when tls feature is off (fields exist for clap).
    #[cfg(not(feature = "tls"))]
    {
        let _ = (
            &args.no_tls_inter_broker,
            &args.tls_peer_insecure,
            &args.tls_ca,
            &args.tls_client_ca,
            &args.tls_client_allow,
        );
    }

    let _ = args.default_partitions;

    let addr: SocketAddr = args
        .listen
        .parse()
        .with_context(|| format!("invalid listen address: {}", args.listen))?;

    if let Some(host) = args.advertised_host {
        let port = args.advertised_port.unwrap_or(addr.port());
        broker.set_advertised(host, port);
    } else if let Some(port) = args.advertised_port {
        let host = broker.metadata(None).host;
        broker.set_advertised(host, port);
    }

    if let Some(token) = args.metrics_token.clone() {
        if token.is_empty() {
            bail!("--metrics-token / VOLANT_METRICS_TOKEN must not be empty when set");
        }
        broker.set_metrics_token(Some(token));
        info!("metrics endpoint authentication enabled");
    }

    if let Some(metrics_addr) = &args.metrics_addr {
        let maddr: SocketAddr = metrics_addr
            .parse()
            .with_context(|| format!("invalid --metrics-addr: {metrics_addr}"))?;
        let b = Arc::clone(&broker);
        let metrics_auth = broker.metrics_token().is_some();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(maddr, b).await {
                tracing::error!(error = %e, "metrics server exited");
            }
        });
        info!(%maddr, metrics_auth, "metrics endpoint enabled");
    } else if args.metrics_token.is_some() {
        info!("--metrics-token set but --metrics-addr unset; metrics auth unused");
    }

    if let Some(kafka_addr) = &args.kafka_listen {
        let kaddr: SocketAddr = kafka_addr
            .parse()
            .with_context(|| format!("invalid --kafka-listen: {kafka_addr}"))?;
        let listener = tokio::net::TcpListener::bind(kaddr)
            .await
            .with_context(|| format!("bind --kafka-listen {kaddr}"))?;
        let b = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = serve_kafka_listener(listener, b).await {
                tracing::error!(error = %e, "kafka shim server exited");
            }
        });
        info!(%kaddr, "kafka wire protocol shim enabled (Phase 23–27)");
    }

    info!(
        data_dir = %args.data_dir.display(),
        listen = %addr,
        node_id = broker.node_id(),
        "starting volant broker"
    );

    #[cfg(feature = "tls")]
    if let Some(setup) = tls_setup {
        return tls::run_tls_server(addr, broker, setup).await;
    }

    run_server(addr, broker).await.map_err(Into::into)
}

/// Optional TLS accept path (feature `tls`).
#[cfg(feature = "tls")]
mod tls {
    use std::collections::HashSet;
    use std::fs::File;
    use std::io::BufReader;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::server::WebPkiClientVerifier;
    use rustls::RootCertStore;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tracing::{debug, error, info};
    use volant_broker::{start_background_tasks, Broker};
    use volant_core::Error;
    use volant_protocol::{Frame, Response};

    /// TLS listener configuration (Phase 7/19).
    pub struct TlsSetup {
        pub acceptor: TlsAcceptor,
        /// True when `--tls-client-ca` was set (mTLS required).
        pub mtls: bool,
        /// Optional CN allowlist; `None` or empty = allow any verified client.
        pub allowlist: Option<HashSet<String>>,
    }

    pub fn build_acceptor(
        cert_path: &Path,
        key_path: &Path,
        client_ca: Option<&Path>,
    ) -> Result<TlsAcceptor> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;

        let config = if let Some(ca_path) = client_ca {
            let mut roots = RootCertStore::empty();
            let ca_certs = load_certs(ca_path)?;
            for c in ca_certs {
                roots
                    .add(c)
                    .map_err(|e| anyhow::anyhow!("add client CA cert: {e}"))?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .context("build client cert verifier")?;
            rustls::ServerConfig::builder()
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .context("build rustls ServerConfig (mTLS)")?
        } else {
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .context("build rustls ServerConfig")?
        };
        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let file = File::open(path)
            .with_context(|| format!("open TLS cert {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("parse TLS cert PEM")?;
        if certs.is_empty() {
            bail!("no certificates found in {}", path.display());
        }
        Ok(certs)
    }

    fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
        let file = File::open(path)
            .with_context(|| format!("open TLS key {}", path.display()))?;
        let mut reader = BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader)
            .context("parse TLS key PEM")?
            .ok_or_else(|| anyhow::anyhow!("no private key found in {}", path.display()))?;
        Ok(key)
    }

    /// Extract principal from a client leaf certificate (CN, else first DNS SAN).
    pub fn principal_from_cert(der: &[u8]) -> Option<String> {
        use x509_parser::prelude::*;
        let (_, cert) = X509Certificate::from_der(der).ok()?;
        if let Ok(cn) = cert.subject().iter_common_name().next()?.as_str() {
            if !cn.is_empty() {
                return Some(cn.to_owned());
            }
        }
        // Fallback: first DNS SAN.
        if let Ok(Some(sans)) = cert.subject_alternative_name() {
            for name in &sans.value.general_names {
                if let GeneralName::DNSName(dns) = name {
                    if !dns.is_empty() {
                        return Some((*dns).to_owned());
                    }
                }
            }
        }
        None
    }

    /// TLS-only listen when certs are provided (no dual plain/TLS).
    pub async fn run_tls_server(
        addr: SocketAddr,
        broker: Arc<Broker>,
        setup: TlsSetup,
    ) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        broker.set_advertised(local.ip().to_string(), local.port());
        info!(%local, mtls = setup.mtls, "volant broker listening (TLS)");
        start_background_tasks(Arc::clone(&broker));

        let setup = Arc::new(setup);
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    broker.metrics().record_connection();
                    debug!(%peer, "accepted TLS connection");
                    let b = Arc::clone(&broker);
                    let setup = Arc::clone(&setup);
                    tokio::spawn(async move {
                        match setup.acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                if let Err(e) =
                                    handle_tls_connection(tls_stream, b, &setup).await
                                {
                                    debug!(%peer, error = %e, "TLS connection closed");
                                }
                            }
                            Err(e) => {
                                debug!(%peer, error = %e, "TLS handshake failed");
                            }
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "TLS accept failed");
                    return Err(Error::Io(e).into());
                }
            }
        }
    }

    async fn handle_tls_connection(
        mut stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        broker: Arc<Broker>,
        setup: &TlsSetup,
    ) -> volant_core::Result<()> {
        use bytes::BytesMut;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tracing::info_span;
        use tracing::Instrument;
        use volant_protocol::codec::{decode_frame, encode_frame};
        use volant_protocol::pack_response;

        // Phase 19: map verified client cert → principal / auth.
        let mut principal: Option<String> = None;
        let mut mtls_authenticated = false;
        if setup.mtls {
            let peer_certs = {
                let (_, conn) = stream.get_ref();
                conn.peer_certificates()
                    .map(|c| c.to_vec())
                    .unwrap_or_default()
            };
            if let Some(leaf) = peer_certs.first() {
                principal = principal_from_cert(leaf.as_ref());
                let allowed = match (&setup.allowlist, &principal) {
                    (None, _) => true,
                    (Some(list), _) if list.is_empty() => true,
                    (Some(list), Some(cn)) => list.contains(cn),
                    (Some(_), None) => false,
                };
                mtls_authenticated = allowed && principal.is_some();
                debug!(
                    principal = ?principal,
                    mtls_authenticated,
                    "mTLS client identity"
                );
            }
        }

        let mut buf = BytesMut::with_capacity(8 * 1024);
        // Authenticated if mTLS identity ok, or if neither token/SCRAM nor mTLS is required.
        let mut authenticated = if setup.mtls {
            mtls_authenticated
        } else {
            !broker.auth_required()
        };
        let mut scram_challenge: Option<volant_broker::ScramChallenge> = None;
        let mtls_enabled = setup.mtls;

        loop {
            loop {
                match decode_frame(&mut buf)? {
                    Some(frame) => {
                        let corr = frame.header.correlation_id;
                        let opcode = frame.header.opcode;
                        let span = info_span!(
                            "rpc",
                            opcode,
                            correlation_id = corr,
                            authenticated,
                            principal = principal.as_deref().unwrap_or("")
                        );
                        let response = async {
                            dispatch_tls(
                                &broker,
                                frame,
                                &mut authenticated,
                                &mut principal,
                                &mut scram_challenge,
                                mtls_enabled,
                            )
                            .await
                        }
                        .instrument(span)
                        .await;
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

    /// Shared Auth/SCRAM dispatch plus an mTLS-only gate when no token/SCRAM users exist.
    async fn dispatch_tls(
        broker: &Broker,
        frame: Frame,
        authenticated: &mut bool,
        principal: &mut Option<String>,
        scram_challenge: &mut Option<volant_broker::ScramChallenge>,
        mtls_enabled: bool,
    ) -> Response {
        use volant_protocol::{decode_request, ErrorCode, Request};

        if mtls_enabled && !*authenticated && !broker.auth_required() {
            if let Ok(req) = decode_request(frame.header.opcode, &frame.payload) {
                let is_auth_op = matches!(
                    req,
                    Request::Auth { .. }
                        | Request::ScramFirst { .. }
                        | Request::ScramFinal { .. }
                );
                let is_bootstrap_create = matches!(req, Request::CreateScramUser { .. })
                    && !broker.scram().has_users();
                if !is_auth_op && !is_bootstrap_create {
                    broker
                        .metrics()
                        .record_error(ErrorCode::AuthenticationRequired as u16);
                    return Response::Error {
                        code: ErrorCode::AuthenticationRequired as u16,
                        message:
                            "authentication required; present mTLS client cert or send Auth/SCRAM"
                                .into(),
                    };
                }
            }
        }

        volant_broker::net::dispatch_with_auth(
            broker,
            frame,
            authenticated,
            principal,
            scram_challenge,
        )
        .await
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn principal_from_openssl_self_signed() {
            // Generate a throwaway cert with openssl if available.
            let dir = std::env::temp_dir().join(format!(
                "volant-mtls-unit-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::create_dir_all(&dir);
            let key = dir.join("t.key");
            let crt = dir.join("t.crt");
            let status = std::process::Command::new("openssl")
                .args([
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                ])
                .arg(&key)
                .arg("-out")
                .arg(&crt)
                .args(["-days", "1", "-subj", "/CN=alice-mtls"])
                .status();
            let Ok(status) = status else {
                eprintln!("openssl not available; skip principal parse test");
                let _ = std::fs::remove_dir_all(&dir);
                return;
            };
            if !status.success() {
                eprintln!("openssl failed; skip");
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
            let certs = load_certs(&crt).expect("load cert");
            let p = principal_from_cert(certs[0].as_ref());
            assert_eq!(p.as_deref(), Some("alice-mtls"));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// Optional CPU affinity / thread-per-core helpers (feature `thread-per-core`).
#[cfg(feature = "thread-per-core")]
mod affinity {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{Context, Result};
    use tracing::{info, warn};

    pub fn build_runtime() -> Result<tokio::runtime::Runtime> {
        let cpus = parse_cpu_list(std::env::var_os("VOLANT_CPU_LIST"));

        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();

        match cpus {
            Some(cpus) if !cpus.is_empty() => {
                let n = cpus.len();
                info!(
                    cpus = ?cpus,
                    workers = n,
                    "thread-per-core: pinning Tokio workers to VOLANT_CPU_LIST"
                );
                builder.worker_threads(n);

                let counter = AtomicUsize::new(0);
                let cpus_for_pin = cpus.clone();
                builder.on_thread_start(move || {
                    let idx = counter.fetch_add(1, Ordering::Relaxed) % cpus_for_pin.len();
                    let core_id = cpus_for_pin[idx];
                    pin_current_thread(core_id);
                });
            }
            _ => {
                info!(
                    "thread-per-core feature enabled but VOLANT_CPU_LIST unset/empty; \
                     running unpinned"
                );
            }
        }

        builder
            .build()
            .context("failed to build Tokio runtime (thread-per-core)")
    }

    fn parse_cpu_list(raw: Option<std::ffi::OsString>) -> Option<Vec<usize>> {
        let raw = raw?;
        let s = match raw.to_str() {
            Some(s) => s.trim(),
            None => {
                warn!("VOLANT_CPU_LIST is not valid UTF-8; ignoring");
                return None;
            }
        };
        if s.is_empty() {
            return None;
        }

        let mut out = Vec::new();
        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            match part.parse::<usize>() {
                Ok(id) => out.push(id),
                Err(_) => warn!(token = %part, "invalid CPU id in VOLANT_CPU_LIST; skipping"),
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    fn pin_current_thread(core_id: usize) {
        let core = core_affinity::CoreId { id: core_id };
        if core_affinity::set_for_current(core) {
            info!(core_id, "pinned worker thread to CPU");
        } else {
            warn!(
                core_id,
                "failed to pin worker thread to CPU (unsupported platform or permission); continuing"
            );
        }
    }
}
