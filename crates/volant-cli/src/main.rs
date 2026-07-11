//! Volant command-line interface.

use anyhow::Result;
use clap::{Parser, Subcommand};

/// Volant CLI — manage topics and inspect cluster state.
#[derive(Debug, Parser)]
#[command(name = "volant", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print version and project status.
    Version,
    /// Topic administration (network path TBD).
    Topic {
        #[command(subcommand)]
        action: TopicCmd,
    },
}

#[derive(Debug, Subcommand)]
enum TopicCmd {
    /// List topics on the cluster.
    List {
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
    /// Create a topic.
    Create {
        /// Topic name.
        name: String,
        /// Partition count.
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        /// Broker address.
        #[arg(long, default_value = "127.0.0.1:9092")]
        broker: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Version => {
            println!("volant {}", env!("CARGO_PKG_VERSION"));
            println!("status: scaffold — see ROADMAP.md");
        }
        Commands::Topic { action } => match action {
            TopicCmd::List { broker } => {
                println!("topic list via {broker}: not implemented (Phase 2)");
            }
            TopicCmd::Create {
                name,
                partitions,
                broker,
            } => {
                println!(
                    "create topic '{name}' partitions={partitions} via {broker}: not implemented (Phase 2)"
                );
            }
        },
    }
    Ok(())
}
