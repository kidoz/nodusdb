//! Leader-only upgrade service; status uses ReadIndex and every mutation reads
//! durable authority. Client-supplied names never count as capability evidence.
use crate::multi_raft::{META_SHARD, MultiRaftManager};
use anyhow::{Result, ensure};
use nodus_raftstore::{
    ShardCommand,
    network::RaftTransport,
    upgrade::{self, CommandV1, OperationV1, Phase},
};
use nodus_storage_api::KvEngine;
use std::sync::Arc;

pub struct RaftUpgradeCoordinator {
    pub kv: Arc<dyn KvEngine>,
    pub manager: Arc<MultiRaftManager>,
    pub transport: RaftTransport,
    /// Shared with meta membership admission, across the probe/commit interval.
    pub membership_lock: Arc<tokio::sync::Mutex<()>>,
}
impl RaftUpgradeCoordinator {
    async fn leader(&self) -> Result<nodus_raftstore::server::NodusRaft> {
        let raft = self
            .manager
            .get(META_SHARD)
            .await
            .ok_or_else(|| anyhow::anyhow!("meta group unavailable"))?;
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            raft.ensure_linearizable(),
        )
        .await??;
        Ok(raft)
    }
    pub async fn get_state(&self) -> Result<serde_json::Value> {
        self.leader().await?;
        let record = upgrade::read(self.kv.as_ref())?;
        let mut value = serde_json::to_value(&record)?;
        value["target_version"] = if record.phase == Phase::Idle {
            serde_json::Value::Null
        } else {
            upgrade::TARGET.into()
        };
        value["feature_gates"] = serde_json::json!({"mvcc_snapshots": record.cluster_version >= 2});
        Ok(value)
    }
    pub fn check_membership_change(&self) -> Result<()> {
        ensure!(
            upgrade::read(self.kv.as_ref())?.phase == Phase::Idle,
            "meta membership is frozen during and after snapshot-v2 finalization"
        );
        Ok(())
    }
    pub async fn start_upgrade(&self, target: String) -> Result<()> {
        ensure!(
            target == upgrade::TARGET,
            "supported target is snapshot-v2; binary versions are reported by nodes"
        );
        self.execute(OperationV1::Start, None).await
    }
    pub async fn report_node_upgraded(&self, node: &str) -> Result<()> {
        let node = node
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("node must be a numeric Raft member ID"))?;
        self.execute(OperationV1::Refresh, Some(node)).await
    }
    pub async fn finalize_upgrade(&self) -> Result<()> {
        self.execute(OperationV1::Finalize, None).await
    }
    pub async fn rollback(&self) -> Result<()> {
        self.execute(OperationV1::Rollback, None).await
    }

    async fn execute(&self, operation: OperationV1, requested: Option<u64>) -> Result<()> {
        // Bound lock acquisition, probes and consensus waits as one operation.
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            self.execute_inner(operation, requested),
        )
        .await?
    }
    async fn execute_inner(&self, operation: OperationV1, requested: Option<u64>) -> Result<()> {
        let _guard = self.membership_lock.lock().await;
        let raft = self.leader().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let membership = machine.read().await.last_membership.clone();
        let roster = upgrade::members(&membership)?;
        ensure!(
            requested.is_none_or(|id| roster.contains_key(&id)),
            "unknown upgrade node"
        );
        let old = upgrade::read(self.kv.as_ref())?;
        let session = uuid::Uuid::new_v4();
        let mut reports = Vec::new();
        if !matches!(operation, OperationV1::Rollback) {
            for (id, addr) in &roster {
                let report = if *id == self.manager.node_id() {
                    let mut report = upgrade::CapabilityV1::local(*id, session);
                    let kv = self.kv.clone();
                    report.ready_for_finalize =
                        tokio::task::spawn_blocking(move || upgrade::preflight(kv.as_ref()))
                            .await??;
                    report
                } else {
                    self.transport.probe(*id, addr, session, true).await?
                };
                reports.push(report);
            }
        }
        let cmd = CommandV1 {
            expected_revision: old.revision,
            membership,
            session,
            operation,
            reports,
        };
        let result = raft
            .client_write(ShardCommand::UpgradeControlV1(cmd))
            .await?
            .data;
        ensure!(
            result.success,
            "{}",
            result.error.unwrap_or_else(|| "upgrade rejected".into())
        );
        Ok(())
    }
}
