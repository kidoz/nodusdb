//! Bounded, durable Raft prefix removal. Every batch publishes its tombstones
//! and purge watermark together, using the existing v1 record format.

use super::*;

const PURGE_BATCH_SIZE: usize = 256;

impl RaftMetaStore {
    fn purge_batch(&self, entries: &[LogId<u64>], applied: &AppliedState) -> anyhow::Result<()> {
        let state = encode_raft(serde_json::to_vec(applied)?);
        let txn = TxnId::new();
        let result = (|| {
            for entry in entries {
                self.kv
                    .delete_intent(txn, Bytes::from(log_key(entry.index)))?;
            }
            self.kv.write_intent(
                txn,
                Bytes::from_static(RAFT_APPLIED_KEY),
                Bytes::from(state),
            )?;
            self.kv.commit(txn, self.next_ts())?;
            Ok(())
        })();
        if result.is_err() {
            // A failed commit can have an uncertain durable outcome. Report it
            // to Raft even if abort succeeds; recovery is authoritative.
            let _ = self.kv.abort(txn);
        }
        result
    }
}

impl NodusRaftStore {
    pub(super) async fn purge_log_prefix(
        &self,
        target: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let started = std::time::Instant::now();
        // Same lock order as log-state reads. Apply/snapshot installation must
        // not overwrite the applied record while a purge publishes its copy.
        let mut log = self.log.clone().write_owned().await;
        let mut sm = self.state_machine.clone().write_owned().await;
        if sm.last_purged.is_some_and(|id| id.index >= target.index) {
            return Ok(());
        }
        let meta = self.meta.clone();
        // Own the guards inside the blocking task: cancelling the async caller
        // cannot release them between durable publication and cache updates.
        let result = tokio::task::spawn_blocking(move || -> anyhow::Result<(usize, usize)> {
            let mut deleted = 0;
            let mut batches = 0;
            loop {
                let entries: Vec<_> = log
                    .range(..=target.index)
                    .take(PURGE_BATCH_SIZE + 1)
                    .map(|(_, e)| e.log_id)
                    .collect();
                let complete = entries.len() <= PURGE_BATCH_SIZE;
                let entries = &entries[..entries.len().min(PURGE_BATCH_SIZE)];
                let watermark = if complete {
                    target
                } else {
                    entries[PURGE_BATCH_SIZE - 1]
                };
                let applied = AppliedState {
                    last_applied: sm.last_applied_log,
                    last_membership: sm.last_membership.clone(),
                    last_purged: Some(watermark),
                };
                if let Some(meta) = &meta {
                    meta.purge_batch(entries, &applied)?;
                }
                for entry in entries {
                    log.remove(&entry.index);
                }
                sm.last_purged = Some(watermark);
                deleted += entries.len();
                batches += 1;
                if complete {
                    return Ok((deleted, batches));
                }
            }
        })
        .await;
        let result = result.map_err(anyhow::Error::from).and_then(|r| r);
        match result {
            Ok((entries, batches)) => {
                tracing::info!(group = %self.snapshot_group, target = target.index, entries, batches,
                    elapsed_ms = started.elapsed().as_millis() as u64, "Raft log prefix purged");
                Ok(())
            }
            Err(error) => {
                tracing::error!(group = %self.snapshot_group, target = target.index, %error,
                    elapsed_ms = started.elapsed().as_millis() as u64, "Raft log purge failed");
                Err(StorageIOError::write_logs(AnyError::error(format!(
                    "purge Raft log through {}: {error:#}",
                    target.index
                )))
                .into())
            }
        }
    }
}

#[cfg(test)]
mod tests;
