use anyhow::Result;
use nodus_raftstore::ShardCommand;
use nodus_upgrade::{UpgradeCoordinator, UpgradeState};
use std::sync::Arc;

use crate::raft_router::RaftRouter;

/// `UpgradeCoordinator` that replicates rolling-upgrade transitions through Raft
/// (the `shard-meta` group) before reading state back from the local coordinator.
///
/// Write methods route through the async [`RaftRouter`], whose `submit` waits via
/// `blocking_recv` — so they MUST be invoked from a blocking context (e.g. inside
/// `tokio::task::spawn_blocking`), never directly on a runtime worker thread.
pub struct RaftUpgradeCoordinator {
    pub local: Arc<dyn UpgradeCoordinator>,
    pub router: RaftRouter,
    pub shard_id: String,
}

impl RaftUpgradeCoordinator {
    /// Replicate an upgrade command and surface a labelled error on failure.
    fn replicate(&self, op: &str, cmd: ShardCommand) -> Result<()> {
        self.router.submit(&self.shard_id, cmd).map_err(|e| {
            tracing::error!("{op} client_write failed: {e}");
            anyhow::anyhow!("{op} raft error: {e}")
        })
    }
}

impl UpgradeCoordinator for RaftUpgradeCoordinator {
    fn get_state(&self) -> Result<UpgradeState> {
        self.local.get_state()
    }

    fn start_upgrade(&self, target_version: String) -> Result<()> {
        self.replicate(
            "start_upgrade",
            ShardCommand::UpgradeStart { target_version },
        )
    }

    fn report_node_upgraded(&self, node_id: &str) -> Result<()> {
        self.replicate(
            "report_node_upgraded",
            ShardCommand::UpgradeNodeUpgraded {
                node_id: node_id.to_string(),
            },
        )
    }

    fn finalize_upgrade(&self) -> Result<()> {
        self.replicate("finalize_upgrade", ShardCommand::UpgradeFinalize)
    }

    fn rollback(&self) -> Result<()> {
        self.replicate("rollback", ShardCommand::UpgradeRollback)
    }

    fn is_gate_enabled(&self, feature: &str) -> bool {
        self.local.is_gate_enabled(feature)
    }

    fn current_cluster_version(&self) -> u64 {
        self.local.current_cluster_version()
    }
}
