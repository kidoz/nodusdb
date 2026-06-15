use anyhow::Result;
use bytes::Bytes;
use nodus_raftstore::NodusTypeConfig;
use nodus_raftstore::ShardCommand;
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use openraft::Raft;
use std::sync::Arc;

pub struct RaftKvEngine {
    pub local: Arc<dyn KvEngine>,
    pub raft: Raft<NodusTypeConfig>,
}

impl KvEngine for RaftKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.local.get(key, read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.local.scan(range, read_ts)
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let cmd = ShardCommand::PutIntent {
            txn_id: txn_id.0.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        };
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self
                    .raft
                    .client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))?;
                Ok::<(), anyhow::Error>(())
            })
        })?;
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let cmd = ShardCommand::DeleteIntent {
            txn_id: txn_id.0.to_string(),
            key: key.to_vec(),
        };
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self
                    .raft
                    .client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))?;
                Ok::<(), anyhow::Error>(())
            })
        })?;
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> Result<()> {
        self.local.replace_intent(txn_id, key, replacement)
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let cmd = ShardCommand::CommitTxn {
            txn_id: txn_id.0.to_string(),
            commit_ts,
        };
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self
                    .raft
                    .client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))?;
                Ok::<(), anyhow::Error>(())
            })
        })?;
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let cmd = ShardCommand::AbortTxn {
            txn_id: txn_id.0.to_string(),
        };
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self
                    .raft
                    .client_write(cmd)
                    .await
                    .map_err(|e| anyhow::anyhow!("raft write error: {}", e))?;
                Ok::<(), anyhow::Error>(())
            })
        })?;
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.local.garbage_collect(watermark)
    }
}
