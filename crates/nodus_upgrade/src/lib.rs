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

use std::collections::HashSet;
use std::sync::RwLock;

/// Default rolling-upgrade coordinator.
///
/// Drives the cluster through `Idle -> Preflight -> RollingNodes ->
/// MixedVersion -> ReadyToFinalize -> Finalizing -> Finalized`. Irreversible
/// format changes are modeled as named feature gates that stay **disabled**
/// until finalization, so a rollback at any point before finalize is safe.
pub struct DefaultUpgradeCoordinator {
    state: RwLock<UpgradeState>,
    nodes_total: usize,
    nodes_upgraded: RwLock<HashSet<String>>,
    /// Features (e.g. new on-disk formats) enabled only at finalization.
    gated_features: Vec<String>,
    cluster_version: RwLock<u64>,
}

impl DefaultUpgradeCoordinator {
    pub fn new(nodes_total: usize, gated_features: Vec<String>, cluster_version: u64) -> Self {
        let mut gates = HashMap::new();
        for f in &gated_features {
            gates.insert(f.clone(), false);
        }
        Self {
            state: RwLock::new(UpgradeState {
                feature_gates: gates,
                ..UpgradeState::default()
            }),
            nodes_total,
            nodes_upgraded: RwLock::new(HashSet::new()),
            gated_features,
            cluster_version: RwLock::new(cluster_version),
        }
    }

    /// Preflight: a placeholder for compatibility/health checks. Returns Ok when
    /// the cluster is safe to upgrade.
    fn preflight(&self) -> anyhow::Result<()> {
        if self.nodes_total == 0 {
            anyhow::bail!("preflight failed: cluster has no nodes");
        }
        Ok(())
    }

    /// Records that a node has been rolled to the target binary, advancing the
    /// phase once every node reports in.
    pub fn report_node_upgraded(&self, node_id: &str) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
        if state.phase != UpgradePhase::RollingNodes && state.phase != UpgradePhase::MixedVersion {
            anyhow::bail!("not rolling nodes (phase {:?})", state.phase);
        }
        let mut upgraded = self.nodes_upgraded.write().unwrap();
        upgraded.insert(node_id.to_string());
        state.phase = if upgraded.len() >= self.nodes_total {
            UpgradePhase::ReadyToFinalize
        } else {
            UpgradePhase::MixedVersion
        };
        Ok(())
    }

    /// Aborts an in-progress upgrade. Permitted any time before `Finalizing`,
    /// since no irreversible feature gates have been enabled yet.
    pub fn rollback(&self) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
        match state.phase {
            UpgradePhase::Finalizing | UpgradePhase::Finalized | UpgradePhase::RollbackClosed => {
                anyhow::bail!("cannot roll back after finalization has begun");
            }
            _ => {}
        }
        self.nodes_upgraded.write().unwrap().clear();
        *state = UpgradeState {
            feature_gates: state.feature_gates.clone(),
            ..UpgradeState::default()
        };
        for v in state.feature_gates.values_mut() {
            *v = false;
        }
        state.phase = UpgradePhase::Idle;
        Ok(())
    }

    pub fn is_gate_enabled(&self, feature: &str) -> bool {
        self.state
            .read()
            .unwrap()
            .feature_gates
            .get(feature)
            .copied()
            .unwrap_or(false)
    }

    pub fn current_cluster_version(&self) -> u64 {
        *self.cluster_version.read().unwrap()
    }
}

impl UpgradeCoordinator for DefaultUpgradeCoordinator {
    fn get_state(&self) -> anyhow::Result<UpgradeState> {
        Ok(self.state.read().unwrap().clone())
    }

    fn start_upgrade(&self, target_version: String) -> anyhow::Result<()> {
        {
            let state = self.state.read().unwrap();
            if !matches!(state.phase, UpgradePhase::Idle | UpgradePhase::Failed) {
                anyhow::bail!(
                    "an upgrade is already in progress (phase {:?})",
                    state.phase
                );
            }
        }
        self.preflight()?;
        self.nodes_upgraded.write().unwrap().clear();
        let mut state = self.state.write().unwrap();
        state.phase = UpgradePhase::RollingNodes;
        state.target_version = Some(target_version);
        state.started_at = Some(Utc::now());
        for v in state.feature_gates.values_mut() {
            *v = false;
        }
        Ok(())
    }

    fn finalize_upgrade(&self) -> anyhow::Result<()> {
        let mut state = self.state.write().unwrap();
        if state.phase != UpgradePhase::ReadyToFinalize {
            anyhow::bail!("not ready to finalize (phase {:?})", state.phase);
        }
        state.phase = UpgradePhase::Finalizing;
        // Enable irreversible feature gates only now that every node is upgraded.
        for f in &self.gated_features {
            state.feature_gates.insert(f.clone(), true);
        }
        *self.cluster_version.write().unwrap() += 1;
        state.phase = UpgradePhase::Finalized;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord() -> DefaultUpgradeCoordinator {
        DefaultUpgradeCoordinator::new(3, vec!["new_wal_format".into()], 1)
    }

    #[test]
    fn happy_path_enables_gates_only_at_finalize() {
        let c = coord();
        c.start_upgrade("0.2.0".into()).unwrap();
        assert_eq!(c.get_state().unwrap().phase, UpgradePhase::RollingNodes);

        c.report_node_upgraded("n1").unwrap();
        assert_eq!(c.get_state().unwrap().phase, UpgradePhase::MixedVersion);
        // Gate stays disabled during mixed-version operation.
        assert!(!c.is_gate_enabled("new_wal_format"));

        c.report_node_upgraded("n2").unwrap();
        c.report_node_upgraded("n3").unwrap();
        assert_eq!(c.get_state().unwrap().phase, UpgradePhase::ReadyToFinalize);
        assert!(!c.is_gate_enabled("new_wal_format"));

        c.finalize_upgrade().unwrap();
        assert_eq!(c.get_state().unwrap().phase, UpgradePhase::Finalized);
        assert!(c.is_gate_enabled("new_wal_format"));
        assert_eq!(c.current_cluster_version(), 2);
    }

    #[test]
    fn rollback_before_finalize_then_blocked_after() {
        let c = coord();
        c.start_upgrade("0.2.0".into()).unwrap();
        c.report_node_upgraded("n1").unwrap();
        c.rollback().unwrap();
        assert_eq!(c.get_state().unwrap().phase, UpgradePhase::Idle);
        assert!(!c.is_gate_enabled("new_wal_format"));

        // Drive to finalize, then rollback must fail.
        c.start_upgrade("0.2.0".into()).unwrap();
        for n in ["n1", "n2", "n3"] {
            c.report_node_upgraded(n).unwrap();
        }
        c.finalize_upgrade().unwrap();
        assert!(c.rollback().is_err());
    }

    #[test]
    fn cannot_finalize_before_ready() {
        let c = coord();
        c.start_upgrade("0.2.0".into()).unwrap();
        assert!(c.finalize_upgrade().is_err());
    }
}
