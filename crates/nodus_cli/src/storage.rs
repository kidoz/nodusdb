use anyhow::{Context, Result, ensure};
use clap::Subcommand;
use std::path::PathBuf;

#[derive(Subcommand)]
pub enum StorageCommand {
    /// Publish a manifest for a verified historical store; stop the node and back up the directory first
    MigrateLegacy {
        #[arg(long)]
        data_dir: PathBuf,
        /// Historical active WAL id (max SSTable id + 1, or 1 without SSTables)
        #[arg(long)]
        active_wal: u64,
        /// Every final SSTable id; repeat this option for multiple files
        #[arg(long = "sstable")]
        sstables: Vec<u64>,
        /// Protected file containing the original 64-hex-character WAL key
        #[arg(long)]
        wal_key_file: Option<PathBuf>,
    },
}

pub fn run(cmd: &StorageCommand) -> Result<()> {
    match cmd {
        StorageCommand::MigrateLegacy {
            data_dir,
            active_wal,
            sstables,
            wal_key_file,
        } => {
            let key = wal_key_file
                .as_ref()
                .map(|path| {
                    let encoded = std::fs::read_to_string(path).context("reading WAL key file")?;
                    let encoded = encoded.trim();
                    ensure!(
                        encoded.len() == 64,
                        "WAL key file must contain exactly 64 hex characters"
                    );
                    let bytes = hex::decode(encoded).map_err(|_| {
                        anyhow::anyhow!("WAL key file must contain exactly 64 hex characters")
                    })?;
                    <[u8; 32]>::try_from(bytes)
                        .map_err(|_| anyhow::anyhow!("invalid WAL key length"))
                })
                .transpose()?;
            nodus_storage_lsm::migrate_legacy_manifest(data_dir, sstables, *active_wal, key)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "status": "migrated", "manifest": data_dir.join("MANIFEST"),
                    "active_wal": active_wal, "sstables": sstables,
                }))?
            );
        }
    }
    Ok(())
}
