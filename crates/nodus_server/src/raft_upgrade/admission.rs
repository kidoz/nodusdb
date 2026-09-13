//! Retryable, additive member admission. Caller holds the membership mutex.
use super::*;
use nodus_raftstore::upgrade::admission::{self, CommandV1, OperationV1, StageV1};
use std::collections::BTreeMap;
use uuid::Uuid;

impl RaftUpgradeCoordinator {
    async fn admission_reports(
        &self,
        roster: &BTreeMap<u64, String>,
        challenge: Uuid,
    ) -> Result<Vec<upgrade::CapabilityV1>> {
        let mut reports = Vec::with_capacity(roster.len());
        for (id, address) in roster {
            let report = if *id == self.manager.node_id() {
                upgrade::CapabilityV1::local(*id, challenge)
            } else {
                self.transport.probe(*id, address, challenge, false).await?
            };
            admission::require_reader(&report)?;
            reports.push(report);
        }
        Ok(reports)
    }

    /// Resume a recorded admission, or approve a new candidate. The admin join
    /// handler holds the shared mutex; each phase also checks replicated state.
    pub async fn admit_member_locked(&self, node_id: u64, address: &str) -> Result<()> {
        admission::validate_address(address)?;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            // Approval, learner, promotion and completion; each iteration reads
            // applied state behind ReadIndex. An interrupted caller can retry.
            loop {
                if self.admission_step(node_id, address).await? {
                    return Ok(());
                }
            }
        })
        .await?
    }

    pub(crate) async fn admission_step(&self, node_id: u64, address: &str) -> Result<bool> {
        let raft = self.leader().await?;
        let machine = self.manager.machine(META_SHARD).await?;
        let membership = machine.read().await.last_membership.clone();
        let authority = upgrade::read(self.kv.as_ref())?;
        ensure!(
            authority.phase == Phase::Finalized,
            "admission requires snapshot-v2 finalization"
        );
        let ledger = admission::read(self.kv.as_ref())?;
        let mut roster = admission::approved(&authority, ledger.as_ref())?;
        let challenge = Uuid::new_v4();
        let revision = ledger.as_ref().map_or(0, |r| r.revision);
        let (operation_id, action, reports) =
            if let Some(plan) = ledger.as_ref().and_then(|r| r.pending.as_ref()) {
                ensure!(
                    plan.node_id == node_id && roster.get(&node_id).is_some_and(|a| a == address),
                    "another admission is pending; retry its node ID and address"
                );
                admission::validate_progress(
                    ledger
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("missing admission"))?,
                    &membership,
                )?;
                match plan.stage {
                    StageV1::Approved => {
                        // Repeating add_learner waits for catch-up even if the learner
                        // already exists. Approval survives a timeout or leader loss.
                        self.admission_reports(&roster, challenge).await?;
                        raft.add_learner(node_id, openraft::BasicNode::new(address), true)
                            .await?;
                        let current = machine.read().await.last_membership.clone();
                        let fresh = Uuid::new_v4();
                        let reports = self.admission_reports(&roster, fresh).await?;
                        let result = raft
                            .client_write(ShardCommand::UpgradeAdmissionV1(CommandV1 {
                                expected_revision: revision,
                                operation_id: plan.operation,
                                membership: current,
                                challenge: fresh,
                                reports,
                                action: OperationV1::Promote,
                            }))
                            .await?
                            .data;
                        ensure!(
                            result.success,
                            "{}",
                            result.error.unwrap_or_else(|| "promotion rejected".into())
                        );
                        return Ok(false);
                    }
                    StageV1::Promoting => {
                        self.admission_reports(&roster, challenge).await?;
                        if !plan.complete(&membership) {
                            raft.change_membership(plan.target_voters(), true).await?;
                            return Ok(false);
                        }
                        (plan.operation, OperationV1::Complete, Vec::new())
                    }
                }
            } else {
                if let Some(approved) = roster.get(&node_id) {
                    ensure!(
                        approved == address
                            && upgrade::members(&membership)? == roster
                            && membership.membership().get_joint_config().len() == 1
                            && membership.membership().voter_ids().any(|id| id == node_id),
                        "node ID/address conflict or unexpected membership"
                    );
                    admission::validate_stable(&authority, ledger.as_ref(), &membership)?;
                    self.admission_reports(&roster, challenge).await?;
                    return Ok(true);
                }
                admission::validate_stable(&authority, ledger.as_ref(), &membership)?;
                ensure!(roster.len() < upgrade::MAX_MEMBERS, "member limit reached");
                ensure!(
                    !roster.values().any(|a| a == address),
                    "address already belongs to another node"
                );
                admission::validate_address(address)?;
                roster.insert(node_id, address.to_owned());
                let reports = self.admission_reports(&roster, challenge).await?;
                (
                    Uuid::new_v4(),
                    OperationV1::Approve {
                        node_id,
                        address: address.to_owned(),
                    },
                    reports,
                )
            };
        let complete = matches!(action, OperationV1::Complete);
        let result = raft
            .client_write(ShardCommand::UpgradeAdmissionV1(CommandV1 {
                expected_revision: revision,
                operation_id,
                membership,
                challenge,
                reports,
                action,
            }))
            .await?
            .data;
        ensure!(
            result.success,
            "{}",
            result.error.unwrap_or_else(|| "admission rejected".into())
        );
        Ok(complete)
    }
}
