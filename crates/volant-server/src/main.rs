//! Volant broker server entrypoint.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tracing::info;
use volant_broker::{run_metrics_server, run_server, Broker, ClusterConfig};
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
            || args.no_tls_inter_broker
        {
            bail!(
                "--tls-cert/--tls-key/--tls-ca/--no-tls-inter-broker require building with \
                 `--features tls` (default build is plaintext-only for broad platform support)"
            );
        }
    }

    #[cfg(feature = "tls")]
    let tls_acceptor = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(tls::build_acceptor(cert, key)?),
        (None, None) => None,
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

    // Inter-broker TLS: on when server TLS is active unless opted out.
    #[cfg(feature = "tls")]
    {
        let server_tls = tls_acceptor.is_some();
        if server_tls && !args.no_tls_inter_broker {
            if !args.tls_peer_insecure && args.tls_ca.is_none() {
                info!(
                    "inter-broker TLS peer verification enabled without --tls-ca; \
                     using webpki roots only"
                );
            }
            broker.set_inter_broker_tls(Some(volant_broker::InterBrokerTls {
                insecure: args.tls_peer_insecure,
                ca_path: args.tls_ca.clone(),
            }));
            info!(
                peer_insecure = args.tls_peer_insecure,
                "inter-broker TLS enabled"
            );
        } else if server_tls && args.no_tls_inter_broker {
            info!("inter-broker TLS disabled (--no-tls-inter-broker); peers use plaintext");
        }
    }

    // Silence unused when tls feature is off (fields exist for clap).
    #[cfg(not(feature = "tls"))]
    {
        let _ = (
            &args.no_tls_inter_broker,
            &args.tls_peer_insecure,
            &args.tls_ca,
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

    if let Some(metrics_addr) = &args.metrics_addr {
        let maddr: SocketAddr = metrics_addr
            .parse()
            .with_context(|| format!("invalid --metrics-addr: {metrics_addr}"))?;
        let b = Arc::clone(&broker);
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(maddr, b).await {
                tracing::error!(error = %e, "metrics server exited");
            }
        });
        info!(%maddr, "metrics endpoint enabled");
    }

    info!(
        data_dir = %args.data_dir.display(),
        listen = %addr,
        node_id = broker.node_id(),
        "starting volant broker"
    );

    #[cfg(feature = "tls")]
    if let Some(acceptor) = tls_acceptor {
        return tls::run_tls_server(addr, broker, acceptor).await;
    }

    run_server(addr, broker).await.map_err(Into::into)
}

/// Optional TLS accept path (feature `tls`).
#[cfg(feature = "tls")]
mod tls {
    use std::fs::File;
    use std::io::BufReader;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::sync::Arc;

    use anyhow::{bail, Context, Result};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;
    use tracing::{debug, error, info};
    use volant_broker::{start_background_tasks, Broker};
    use volant_core::Error;
    use volant_protocol::{Frame, Response};

    pub fn build_acceptor(cert_path: &Path, key_path: &Path) -> Result<TlsAcceptor> {
        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .context("build rustls ServerConfig")?;
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

    /// TLS-only listen when certs are provided (no dual plain/TLS).
    pub async fn run_tls_server(
        addr: SocketAddr,
        broker: Arc<Broker>,
        acceptor: TlsAcceptor,
    ) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        broker.set_advertised(local.ip().to_string(), local.port());
        info!(%local, "volant broker listening (TLS)");
        start_background_tasks(Arc::clone(&broker));

        // Reuse framed dispatch by accepting TLS streams and handling via a
        // thin adapter that mirrors plaintext handle_connection logic.
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    broker.metrics().record_connection();
                    debug!(%peer, "accepted TLS connection");
                    let b = Arc::clone(&broker);
                    let acc = acceptor.clone();
                    tokio::spawn(async move {
                        match acc.accept(stream).await {
                            Ok(tls_stream) => {
                                if let Err(e) = handle_tls_connection(tls_stream, b).await {
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

    async fn handle_tls_connection<S>(
        mut stream: S,
        broker: Arc<Broker>,
    ) -> volant_core::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use bytes::BytesMut;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tracing::info_span;
        use tracing::Instrument;
        use volant_protocol::codec::{decode_frame, encode_frame};
        use volant_protocol::pack_response;

        let mut buf = BytesMut::with_capacity(8 * 1024);
        let auth_required = broker.auth_token().is_some();
        let mut authenticated = !auth_required;

        loop {
            loop {
                match decode_frame(&mut buf)? {
                    Some(frame) => {
                        let corr = frame.header.correlation_id;
                        let opcode = frame.header.opcode;
                        let span = info_span!("rpc", opcode, correlation_id = corr, authenticated);
                        let response = async {
                            dispatch_tls(&broker, frame, &mut authenticated, auth_required).await
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

    // Minimal TLS-path dispatch mirroring net.rs auth gate. Full produce/fetch
    // handling is shared via a private approach: for Phase 7 we accept that TLS
    // path re-implements the gate and forwards to broker methods through the
    // same request decode + broker API. To avoid large duplication, we call into
    // plaintext-equivalent logic by decoding and matching Auth only here, then
    // using a shared internal path.
    //
    // Practical Phase 7: TLS connections use the same Auth rules; other opcodes
    // go through a compact re-export. For maintainability we invoke the broker
    // produce/fetch via cloning net behaviour — keep Auth + Metadata + Produce +
    // Fetch + rest by packing through volant_broker's public Broker methods.

    async fn dispatch_tls(
        broker: &Broker,
        frame: Frame,
        authenticated: &mut bool,
        auth_required: bool,
    ) -> Response {
        use volant_protocol::{decode_request, ErrorCode, Request};

        let req = match decode_request(frame.header.opcode, &frame.payload) {
            Ok(r) => r,
            Err(e) => {
                broker.metrics().record_error(ErrorCode::Protocol as u16);
                return Response::Error {
                    code: ErrorCode::Protocol as u16,
                    message: e.to_string(),
                };
            }
        };

        if let Request::Auth { token } = &req {
            return match broker.auth_token() {
                None => {
                    *authenticated = true;
                    Response::Auth { error_code: 0 }
                }
                Some(expected) if expected == *token => {
                    *authenticated = true;
                    Response::Auth { error_code: 0 }
                }
                Some(_) => {
                    *authenticated = false;
                    broker
                        .metrics()
                        .record_error(ErrorCode::AuthenticationFailed as u16);
                    Response::Auth {
                        error_code: ErrorCode::AuthenticationFailed as u16,
                    }
                }
            };
        }

        if auth_required && !*authenticated {
            broker
                .metrics()
                .record_error(ErrorCode::AuthenticationRequired as u16);
            return Response::Error {
                code: ErrorCode::AuthenticationRequired as u16,
                message: "authentication required; send Auth first".into(),
            };
        }

        // Delegate full request handling by briefly bridging through a one-shot
        // internal TCP is not available. Instead re-use produce/fetch via Broker
        // public API for the common path; remaining opcodes return Unsupported
        // if we cannot share net.rs dispatch privately.
        //
        // To keep TLS fully functional, we spawn a localhost plaintext is too
        // heavy. Prefer extracting dispatch — for Phase 7, call the same match
        // tree by making handle_request pub(crate). See net.rs.

        // Call into shared dispatch helper exposed for TLS.
        volant_broker::net::dispatch_request(broker, req).await
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
