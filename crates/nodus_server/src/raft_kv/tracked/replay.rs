//! Bounded, fair replay of terminal decisions only. Never calls fence-abort.
use super::*;
use nodus_raftstore::txn_resolution as journal;

const MAX_TRANSACTIONS: usize = 32;
const MAX_PARTICIPANT_STEPS: usize = 16;
const PASS_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

impl RaftKvEngine {
    pub(in crate::raft_kv) fn replay_terminal_decisions(&self) -> Result<usize> {
        if !self.router.txn_authority_enabled() {
            return Ok(0);
        }
        self.router.read_barrier(META_SHARD)?;
        let old = journal::cursor(self.local.as_ref())?;
        let through = if old.through == 0 {
            journal::high_water(self.local.as_ref())?
        } else {
            old.through
        };
        if through == 0 {
            return Ok(0);
        }
        let page =
            journal::pending_page(self.local.as_ref(), old.after, through, MAX_TRANSACTIONS)?;
        let started = std::time::Instant::now();
        let mut steps = 0;
        let mut completed = 0;
        let mut failures = 0;
        let mut after = old.after;
        let mut scanned = 0;
        for (index, txn) in &page {
            if steps >= MAX_PARTICIPANT_STEPS || started.elapsed() >= PASS_BUDGET {
                break;
            }
            after = *index;
            scanned += 1;
            let record = self
                .authority_read(TxnId(*txn))?
                .ok_or_else(|| anyhow::anyhow!("terminal queue authority missing"))?;
            anyhow::ensure!(
                matches!(
                    record.phase,
                    authority::PhaseV1::Committed { .. } | authority::PhaseV1::Aborted
                ),
                "terminal queue contains an undecided transaction; intents protected"
            );
            let progress = journal::progress(self.local.as_ref(), *txn)?
                .ok_or_else(|| anyhow::anyhow!("terminal queue progress missing"))?;
            if progress.complete {
                continue;
            }
            for (group, epoch) in &record.participants {
                if progress.acknowledged.contains(group) {
                    continue;
                }
                if steps >= MAX_PARTICIPANT_STEPS || started.elapsed() >= PASS_BUDGET {
                    break;
                }
                steps += 1;
                if let Err(error) = self.resolve_participant(&record, group, *epoch, true) {
                    failures += 1;
                    tracing::warn!(txn=%txn, group, %error, "terminal participant replay deferred");
                    break;
                }
            }
            if journal::progress(self.local.as_ref(), *txn)?.is_some_and(|p| p.complete) {
                completed += 1;
            }
        }
        let exhausted = scanned == page.len() && page.len() < MAX_TRANSACTIONS || after >= through;
        let (after, through) = if exhausted { (0, 0) } else { (after, through) };
        if old.after != after || old.through != through {
            // A competing resolver can move this cursor. That loses only scan
            // scheduling; participant acknowledgements remain durable.
            let response = self.router.submit_voting(
                META_SHARD,
                ShardCommand::TxnResolutionV1(journal::CommandV1::Advance {
                    expected_revision: old.revision,
                    after,
                    through,
                }),
            )?;
            if !response.success {
                tracing::debug!("terminal recovery cursor changed concurrently");
            }
        }
        tracing::debug!(
            scanned,
            steps,
            completed,
            failures,
            "terminal decision replay pass"
        );
        Ok(completed)
    }
}
