use clap::{Parser, Subcommand};
use reqwest::Client;

#[derive(Parser)]
#[command(author, version, about = "CLI for NodusDB", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show version
    Version,
    /// Check health of the server
    Health {
        #[arg(long, default_value = "http://127.0.0.1:8088")]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Version => {
            println!("nodusctl {}", env!("CARGO_PKG_VERSION"));
        }
        Commands::Health { addr } => {
            let client = Client::new();
            let url = format!("{}/healthz", addr);
            match client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        println!("Server at {} is healthy.", addr);
                    } else {
                        anyhow::bail!("Server responded with status: {}", response.status());
                    }
                }
                Err(e) => {
                    anyhow::bail!("Failed to connect to server: {}", e);
                }
            }
        }
    }

    Ok(())
}
