//! Volant broker server entrypoint.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use tracing::info;
use volant_broker::Broker;
use volant_storage::StorageConfig;

/// Volant — lightweight, high-performance streaming message broker.
#[derive(Debug, Parser)]
#[command(name = "volant-server", version, about)]
struct Args {
    /// Directory for log segments and metadata.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Listen address (network server lands in Phase 2).
    #[arg(long, default_value = "0.0.0.0:9092")]
    listen: String,

    /// Default number of partitions for auto-created topics.
    #[arg(long, default_value_t = 1)]
    default_partitions: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "volant=info".into()),
        )
        .init();

    let args = Args::parse();
    let storage = StorageConfig {
        data_dir: args.data_dir.clone(),
        ..StorageConfig::default()
    };

    let broker = Broker::new(storage);
    info!(
        data_dir = %args.data_dir.display(),
        listen = %args.listen,
        "volant broker started (in-process; network listener Phase 2)"
    );

    // Smoke-path: ensure broker is usable in-process until TCP lands.
    let _ = broker.list_topics();
    let _ = args.default_partitions;

    // Keep process alive as a placeholder for the future accept loop.
    info!("press Ctrl-C to exit");
    tokio::signal::ctrl_c().await?;
    info!("shutting down");
    Ok(())
}
