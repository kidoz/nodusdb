use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpgradePhase {
    Idle,
    Preflight,
    RollingNodes,
    MixedVersion,
    ReadyToFinalize,
    Finalizing,
    Finalized,
    RollbackAvailable,
    RollbackClosed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterVersionInfo {
    pub binary_version: String,
    pub cluster_version: u64,
    pub storage_format_version: u64,
    pub index_format_version: u64,
    pub network_protocol_version: u64,
    pub backup_format_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeState {
    pub phase: UpgradePhase,
    pub target_version: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub feature_gates: HashMap<String, bool>,
}

impl Default for UpgradeState {
    fn default() -> Self {
        Self {
            phase: UpgradePhase::Idle,
            target_version: None,
            started_at: None,
            feature_gates: HashMap::new(),
        }
    }
}

pub trait UpgradeCoordinator: Send + Sync {
    fn get_state(&self) -> anyhow::Result<UpgradeState>;
    fn start_upgrade(&self, target_version: String) -> anyhow::Result<()>;
    fn finalize_upgrade(&self) -> anyhow::Result<()>;
}
