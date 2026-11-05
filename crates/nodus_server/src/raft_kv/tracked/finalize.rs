use super::*;

impl RaftKvEngine {
    pub(in crate::raft_kv) fn tracked_commit(
        &self,
        txn: TxnId,
        targets: &BTreeMap<String, TxnParticipant>,
        commit_ts: u64,
    ) -> Result<()> {
        let Some(mut record) = self.authority_read(txn)? else {
            anyhow::ensure!(
                targets.is_empty(),
                "transaction has intents without durable ownership"
            );
            return Ok(());
        };
        if let authority::PhaseV1::Committed { commit_ts: decided } = record.phase {
            anyhow::ensure!(
                decided == commit_ts,
                "transaction already committed at another timestamp"
            );
            return self.resolve_authority(&record);
        }
        self.ensure_owner(&record)?;
        let epochs: BTreeMap<_, _> = targets.iter().map(|(g, t)| (g.clone(), t.epoch)).collect();
        anyhow::ensure!(
            record.participants == epochs && !epochs.is_empty(),
            "durable participant set differs from coordinator write set"
        );
        if record.phase == authority::PhaseV1::Active {
            let result =
                self.authority_apply(txn, Some(record.token), authority::OperationV1::Seal);
            record = self
                .authority_read(txn)?
                .ok_or_else(|| anyhow::anyhow!("transaction authority disappeared"))?;
            if let Err(error) = result {
                if record.phase != authority::PhaseV1::Preparing {
                    return Err(error);
                }
            }
        }
        self.ensure_owner(&record)?;
        anyhow::ensure!(
            record.phase == authority::PhaseV1::Preparing && record.participants == epochs,
            "transaction did not seal the expected participants"
        );
        for (group, target) in targets {
            let keys: Vec<_> = target.intents.iter().cloned().collect();
            let result = self.router.submit(
                group,
                self.tracked_command(
                    txn,
                    target.epoch,
                    participant::OperationV1::Prepare {
                        keys_hash: participant::digest_keys(&keys),
                    },
                ),
            );
            if let Err(error) = result {
                // The abort decision must itself win in meta Raft before any
                // participant is touched. A decided commit remains a commit.
                let _ = self.tracked_abort(txn);
                return Err(error.context("tracked participant could not prepare"));
            }
        }
        let result = self.authority_apply(
            txn,
            Some(record.token),
            authority::OperationV1::Commit { commit_ts },
        );
        let decided = self
            .authority_read(txn)?
            .ok_or_else(|| anyhow::anyhow!("transaction authority disappeared"))?;
        if decided.phase != (authority::PhaseV1::Committed { commit_ts }) {
            result?;
            anyhow::bail!("transaction did not acquire the requested commit decision");
        }
        self.resolve_authority(&decided)?;
        if targets.len() > 1 {
            self.metrics.cross_shard_commits_total.inc();
        }
        Ok(())
    }

    pub(in crate::raft_kv) fn tracked_abort(&self, txn: TxnId) -> Result<()> {
        for _ in 0..16 {
            let Some(record) = self.authority_read(txn)? else {
                return Ok(());
            };
            match record.phase {
                authority::PhaseV1::Committed { .. } => {
                    self.resolve_authority(&record)?;
                    anyhow::bail!("transaction already committed; abort refused");
                }
                authority::PhaseV1::Aborted => return self.resolve_authority(&record),
                _ => {
                    if record.token.owner != self.router.authority_owner {
                        return self.recover_tracked_transaction(txn);
                    }
                    self.ensure_owner(&record)?;
                    if self
                        .authority_apply(txn, Some(record.token), authority::OperationV1::Abort)
                        .is_ok()
                        && record.participants.len() > 1
                    {
                        self.metrics.cross_shard_aborts_total.inc();
                    }
                }
            }
        }
        anyhow::bail!("transaction abort did not converge; retry resolution")
    }
}
