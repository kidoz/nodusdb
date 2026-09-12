use super::*;
use nodus_raftstore::migration::{EpochMutationV1, read_fence};

impl RaftKvEngine {
    pub(super) fn current_epoch(&self, group: &str) -> Result<u64> {
        let fence = read_fence(self.engine_for(group).as_ref())?;
        anyhow::ensure!(
            !fence.as_ref().is_some_and(|f| f.closed),
            "shard routing changed: participant is fenced; retry transaction"
        );
        Ok(fence.map_or(0, |f| f.epoch))
    }

    pub(super) fn epoch_command(
        &self,
        group: &str,
        epoch: u64,
        mutation: EpochMutationV1,
    ) -> ShardCommand {
        if self.router.migration_enabled() || epoch != 0 {
            return ShardCommand::EpochWriteV1 { epoch, mutation };
        }
        let shard_id = Self::shard_field(group);
        match mutation {
            EpochMutationV1::Put { txn_id, key, value } => ShardCommand::PutIntent {
                txn_id: txn_id.to_string(),
                key,
                value,
                shard_id,
            },
            EpochMutationV1::Delete { txn_id, key } => ShardCommand::DeleteIntent {
                txn_id: txn_id.to_string(),
                key,
                shard_id,
            },
            EpochMutationV1::Prepare { txn_id } => ShardCommand::PrepareTxn {
                txn_id: txn_id.to_string(),
                shard_id,
            },
            EpochMutationV1::Commit { txn_id, commit_ts } => ShardCommand::CommitTxn {
                txn_id: txn_id.to_string(),
                commit_ts,
                shard_id,
            },
        }
    }
}

impl PendingTxn {
    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        let payload = serde_json::to_vec(self)?;
        Ok(if self.epochs.is_empty() {
            payload
        } else {
            nodus_common::versioned::encode(2, &payload)
        })
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        use nodus_common::versioned::Envelope;
        let (payload, versioned) = match nodus_common::versioned::decode(bytes) {
            Envelope::Versioned {
                version: 2,
                payload,
            } => (payload, true),
            Envelope::Legacy(payload) => (payload, false),
            _ => anyhow::bail!("unsupported 2PC record version"),
        };
        let record: Self = serde_json::from_slice(payload)?;
        anyhow::ensure!(
            if versioned {
                record.epochs.len() == record.participants.len()
                    && record
                        .participants
                        .iter()
                        .all(|p| record.epochs.contains_key(p))
            } else {
                record.epochs.is_empty()
            },
            "invalid participant epochs in 2PC record"
        );
        Ok(record)
    }
}
