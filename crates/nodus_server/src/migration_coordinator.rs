//! Dormant integration for the recoverable pre-copy coordinator. Production
//! activation remains closed until member capability negotiation is verified.
//! The scheduler must call `resume` again after a timeout or leadership change.
use crate::{
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_router::RaftRouter,
};
use anyhow::{Result, anyhow, ensure};
use nodus_catalog::TableId;
use nodus_raftstore::{
    ShardCommand,
    migration::coordinator::{self, PhaseV2, PlanV2, RecordV2, driver::Backend},
};
use std::{sync::Arc, time::Duration};

pub struct MigrationCoordinator {
    manager: Arc<MultiRaftManager>,
    router: RaftRouter,
}

impl MigrationCoordinator {
    pub fn new(manager: Arc<MultiRaftManager>, router: RaftRouter) -> Self {
        Self { manager, router }
    }

    /// Resume a known table from its durable journal. No public start endpoint
    /// or production activation flag exists in this slice.
    pub async fn resume(&self, table: TableId) -> Result<PhaseV2> {
        ensure!(
            self.router.migration_enabled(),
            "migration protocol activation requires verified cluster-wide compatibility"
        );
        coordinator::driver::resume(self, table, Duration::from_secs(5)).await
    }
}

impl Backend for MigrationCoordinator {
    async fn load(&self, table: TableId) -> Result<RecordV2> {
        let raft = self
            .manager
            .get(META_SHARD)
            .await
            .ok_or_else(|| anyhow!("meta group unavailable"))?;
        // Only the current meta leader coordinates. A lost election after this
        // check remains safe: every transition checks operation/phase at apply.
        raft.ensure_linearizable().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let sm = machine.read().await;
        let kv = sm
            .kv
            .as_ref()
            .ok_or_else(|| anyhow!("meta storage unavailable"))?;
        coordinator::read_record_v2(kv.as_ref(), table)?
            .ok_or_else(|| anyhow!("migration journal absent"))
    }

    async fn submit(&self, group: &str, command: ShardCommand) -> Result<()> {
        ensure!(
            self.router.migration_enabled(),
            "migration protocol activation requires verified cluster-wide compatibility"
        );
        let raft = self
            .manager
            .get(group)
            .await
            .ok_or_else(|| anyhow!("participant {group} unavailable"))?;
        // Forwarded V2 ingress stays disabled; a remote leader produces an
        // explicit retryable failure until authenticated activation is built.
        let response = raft.client_write(command).await?.data;
        ensure!(
            response.success,
            "{}",
            response
                .error
                .unwrap_or_else(|| "participant rejected migration".into())
        );
        Ok(())
    }

    async fn routing_matches(&self, plan: &PlanV2) -> Result<bool> {
        let machine = self.manager.machine(META_SHARD).await?;
        let sm = machine.read().await;
        let meta = sm
            .meta_store
            .as_ref()
            .ok_or_else(|| anyhow!("metadata unavailable"))?;
        coordinator::routing_matches(plan, meta.as_ref())
    }
}
