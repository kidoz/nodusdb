use clap::{Parser, Subcommand};
use nodus_monitoring::ClusterOverview;
use reqwest::Client;

#[derive(Parser)]
#[command(author, version, about = "CLI for NodusDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

const DEFAULT_ADDR: &str = "http://127.0.0.1:8088";

#[derive(Subcommand)]
enum Commands {
    /// Show client version
    Version,
    /// Check liveness of the server
    Health {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
    /// Cluster administration
    Cluster {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },
    /// Print raw Prometheus metrics from the server
    Metrics {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[derive(Subcommand)]
enum ClusterCmd {
    /// Show a cluster overview (nodes, shards, QPS, alerts)
    Info {
        #[arg(long, default_value = DEFAULT_ADDR)]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::new();

    match &cli.command {
        Commands::Version => {
            println!("nodusctl {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Health { addr } => {
            let resp = client.get(format!("{addr}/healthz")).send().await?;
            if resp.status().is_success() {
                println!("Server at {addr} is healthy.");
            } else {
                anyhow::bail!("Server responded with status: {}", resp.status());
            }
        }
        Commands::Cluster {
            cmd: ClusterCmd::Info { addr },
        } => {
            let resp = client
                .get(format!("{addr}/api/v1/cluster/overview"))
                .send()
                .await?
                .error_for_status()?;
            let o: ClusterOverview = resp.json().await?;
            println!("Cluster status : {}", o.cluster_status);
            println!("Nodes          : {}/{} live", o.nodes_live, o.nodes_total);
            println!(
                "Shards         : {} total, {} unavailable",
                o.shards_total, o.shards_unavailable
            );
            println!("QPS            : {:.1}", o.qps);
            println!("Active alerts  : {}", o.active_alerts);
        }
        Commands::Metrics { addr } => {
            let body = client
                .get(format!("{addr}/metrics"))
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;
            print!("{body}");
        }
    }

    Ok(())
}
