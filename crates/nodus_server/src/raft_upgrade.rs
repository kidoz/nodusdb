use anyhow::Result;
use nodus_raftstore::{NodusTypeConfig, ShardCommand};
use nodus_upgrade::{UpgradeCoordinator, UpgradeState};
use openraft::Raft;
use std::sync::Arc;

pub struct RaftUpgradeCoordinator {
    pub local: Arc<dyn UpgradeCoordinator>,
    pub raft_state: nodus_raftstore::server::RaftState,
}

impl RaftUpgradeCoordinator {
    async fn get_raft(&self) -> Result<Raft<NodusTypeConfig>> {
        let rafts = self.raft_state.rafts.read().await;
        rafts
            .get("shard-meta")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Meta shard raft not found"))
    }
}

impl UpgradeCoordinator for RaftUpgradeCoordinator {
    fn get_state(&self) -> Result<UpgradeState> {
        self.local.get_state()
    }

    fn start_upgrade(&self, target_version: String) -> Result<()> {
        let cmd = ShardCommand::UpgradeStart { target_version };
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("start_upgrade client_write failed: {}", e);
            anyhow::bail!("start_upgrade raft error: {}", e);
        }
        Ok(())
    }

    fn report_node_upgraded(&self, node_id: &str) -> Result<()> {
        let cmd = ShardCommand::UpgradeNodeUpgraded {
            node_id: node_id.to_string(),
        };
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("report_node_upgraded client_write failed: {}", e);
            anyhow::bail!("report_node_upgraded raft error: {}", e);
        }
        Ok(())
    }

    fn finalize_upgrade(&self) -> Result<()> {
        let cmd = ShardCommand::UpgradeFinalize;
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("finalize_upgrade client_write failed: {}", e);
            anyhow::bail!("finalize_upgrade raft error: {}", e);
        }
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        let cmd = ShardCommand::UpgradeRollback;
        let res = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let raft = self.get_raft().await?;
                raft.client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))
            })
        });
        if let Err(e) = res {
            tracing::error!("rollback client_write failed: {}", e);
            anyhow::bail!("rollback raft error: {}", e);
        }
        Ok(())
    }

    fn is_gate_enabled(&self, feature: &str) -> bool {
        self.local.is_gate_enabled(feature)
    }

    fn current_cluster_version(&self) -> u64 {
        self.local.current_cluster_version()
    }
}
