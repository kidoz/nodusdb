//! Tracked coordinators, selected only after transaction-authority-v5 finalizes.
//! All authority reads use a ReadIndex barrier before local access.

mod finalize;
mod replay;
#[cfg(test)]
mod tests;

use super::*;
use nodus_raftstore::{txn_authority as authority, txn_participant as participant};

impl RaftKvEngine {
    fn authority_read(&self, txn: TxnId) -> Result<Option<authority::RecordV1>> {
        self.router.read_barrier(META_SHARD)?;
        authority::read(self.local.as_ref(), txn.0)
    }

    fn authority_apply(
        &self,
        txn: TxnId,
        expected: Option<authority::TokenV1>,
        operation: authority::OperationV1,
    ) -> Result<()> {
        self.router.submit(
            META_SHARD,
            ShardCommand::TxnAuthorityV1(authority::CommandV1 {
                txn_id: txn.0,
                expected,
                operation,
            }),
        )
    }

    fn ensure_owner(&self, record: &authority::RecordV1) -> Result<()> {
        anyhow::ensure!(
            record.token.owner == self.router.authority_owner && record.token.generation == 1,
            "transaction ownership was fenced; retry transaction"
        );
        Ok(())
    }

    /// Registration precedes every first participant write; even a delayed
    /// write whose response is lost has a durable participant to fence.
    pub(super) fn tracked_enroll(&self, txn: TxnId, group: &str, epoch: u64) -> Result<()> {
        for _ in 0..16 {
            let Some(record) = self.authority_read(txn)? else {
                let result = self.authority_apply(
                    txn,
                    None,
                    authority::OperationV1::Begin {
                        owner: self.router.authority_owner.clone(),
                    },
                );
                // A lost acknowledgement or concurrent Begin is reconciled by
                // a fresh linearizable read, never by assuming absence.
                if let Err(error) = result {
                    if self.authority_read(txn)?.is_none() {
                        return Err(error);
                    }
                }
                continue;
            };
            self.ensure_owner(&record)?;
            anyhow::ensure!(
                record.phase == authority::PhaseV1::Active,
                "transaction no longer accepts participants or writes"
            );
            if let Some(enrolled) = record.participants.get(group) {
                anyhow::ensure!(*enrolled == epoch, "transaction participant epoch changed");
                return Ok(());
            }
            if self
                .authority_apply(
                    txn,
                    Some(record.token),
                    authority::OperationV1::Enroll {
                        group: group.into(),
                        epoch,
                    },
                )
                .is_ok()
            {
                continue;
            }
            // A concurrent enrollment can change the revision. Bound retries;
            // partitions and read errors propagate rather than admitting writes.
        }
        anyhow::bail!("transaction enrollment did not converge; retry transaction")
    }

    pub(super) fn tracked_command(
        &self,
        txn: TxnId,
        epoch: u64,
        operation: participant::OperationV1,
    ) -> ShardCommand {
        ShardCommand::TxnParticipantV1(participant::CommandV1 {
            txn_id: txn.0,
            owner: self.router.authority_owner.clone(),
            generation: 1,
            epoch,
            operation,
        })
    }

    pub(super) fn tracked_write(
        &self,
        txn: TxnId,
        group: &str,
        epoch: u64,
        operations: Vec<nodus_raftstore::migration::EpochIntentOperationV1>,
    ) -> Result<()> {
        self.tracked_enroll(txn, group, epoch)?;
        self.router.submit(
            group,
            self.tracked_command(txn, epoch, participant::OperationV1::Write { operations }),
        )
    }

    /// Called only with a record just read through the meta ReadIndex barrier.
    /// No external caller may submit an unverified decision as a certificate.
    fn resolve_authority(&self, record: &authority::RecordV1) -> Result<()> {
        anyhow::ensure!(
            matches!(
                record.phase,
                authority::PhaseV1::Committed { .. } | authority::PhaseV1::Aborted
            ),
            "cannot resolve an undecided transaction"
        );
        let _span = tracing::info_span!("txn.resolve_tracked", txn = %record.txn_id,
            generation = record.token.generation, participants = record.participants.len())
        .entered();
        let progress =
            nodus_raftstore::txn_resolution::progress(self.local.as_ref(), record.txn_id)?;
        if progress.as_ref().is_some_and(|p| p.complete) {
            return Ok(());
        }
        for (group, epoch) in &record.participants {
            if progress
                .as_ref()
                .is_some_and(|p| p.acknowledged.contains(group))
            {
                continue;
            }
            self.resolve_participant(record, group, *epoch, progress.is_some())?;
        }
        Ok(())
    }

    fn resolve_participant(
        &self,
        record: &authority::RecordV1,
        group: &str,
        epoch: u64,
        journaled: bool,
    ) -> Result<()> {
        self.router.submit(
            group,
            ShardCommand::TxnParticipantV1(participant::CommandV1 {
                txn_id: record.txn_id,
                owner: record.token.owner.clone(),
                generation: record.token.generation,
                epoch,
                operation: participant::OperationV1::Resolve {
                    decision: Box::new(record.clone()),
                },
            }),
        )?;
        if journaled {
            self.router.submit(
                META_SHARD,
                ShardCommand::TxnResolutionV1(
                    nodus_raftstore::txn_resolution::CommandV1::Acknowledge {
                        txn_id: record.txn_id,
                        decision: record.token.clone(),
                        group: group.into(),
                    },
                ),
            )?;
        }
        Ok(())
    }

    /// Explicit dormant recovery; never called by the janitor or a background
    /// timeout. A pending tracked transaction is conditionally fence-aborted;
    /// an existing commit is completed. Unknown legacy IDs are never aborted.
    pub fn recover_tracked_transaction(&self, txn: TxnId) -> Result<()> {
        anyhow::ensure!(
            self.router.txn_authority_enabled(),
            "transaction authority activation is unavailable"
        );
        let recovery = authority::OwnerV1 {
            node_id: self.manager.node_id(),
            incarnation: Uuid::new_v4(),
        };
        for _ in 0..16 {
            let record = self.authority_read(txn)?.ok_or_else(|| {
                anyhow::anyhow!("transaction has no durable authority; intents remain protected")
            })?;
            if matches!(
                record.phase,
                authority::PhaseV1::Committed { .. } | authority::PhaseV1::Aborted
            ) {
                self.resolve_authority(&record)?;
                self.metrics.txn_recoveries_total.inc();
                return Ok(());
            }
            let result = self.authority_apply(
                txn,
                Some(record.token),
                authority::OperationV1::FenceAbort {
                    recovery: recovery.clone(),
                },
            );
            if result.is_ok() {
                continue;
            }
            // Re-read before retrying: either the commit or another resolver
            // may have won. Losing the CAS never authorizes a local abort.
        }
        anyhow::bail!("transaction recovery did not converge; retry resolution")
    }
}
