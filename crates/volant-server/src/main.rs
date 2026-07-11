//! Volant broker server entrypoint.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use tracing::info;
use volant_broker::{run_server, Broker, ClusterConfig};
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
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "volant=info".into()),
        )
        .init();

    let args = Args::parse();

    #[cfg(feature = "thread-per-core")]
    let runtime = affinity::build_runtime()?;
    #[cfg(not(feature = "thread-per-core"))]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")?;

    runtime.block_on(async_main(args))
}

async fn async_main(args: Args) -> Result<()> {
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

    info!(
        data_dir = %args.data_dir.display(),
        listen = %addr,
        node_id = broker.node_id(),
        "starting volant broker"
    );

    run_server(addr, broker).await.map_err(Into::into)
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
