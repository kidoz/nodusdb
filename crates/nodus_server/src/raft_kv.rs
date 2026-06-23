use anyhow::Result;
use bytes::Bytes;
use nodus_raftstore::ShardCommand;
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use std::sync::Arc;

use crate::raft_router::RaftRouter;

/// `KvEngine` that replicates mutations through Raft before applying them
/// locally. Reads are served from the local engine; writes are submitted to the
/// owning shard's Raft group via [`RaftRouter`] (currently always `shard-meta`).
///
/// Write methods route through the async [`RaftRouter`], whose `submit` waits via
/// `blocking_recv` — so they MUST be invoked from a blocking context (e.g. inside
/// `tokio::task::spawn_blocking`), never directly on a runtime worker thread.
pub struct RaftKvEngine {
    pub local: Arc<dyn KvEngine>,
    pub router: RaftRouter,
    pub shard_id: String,
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
        self.router.submit(&self.shard_id, cmd)
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let cmd = ShardCommand::DeleteIntent {
            txn_id: txn_id.0.to_string(),
            key: key.to_vec(),
        };
        self.router.submit(&self.shard_id, cmd)
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
        self.router.submit(&self.shard_id, cmd)
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let cmd = ShardCommand::AbortTxn {
            txn_id: txn_id.0.to_string(),
        };
        self.router.submit(&self.shard_id, cmd)
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.local.garbage_collect(watermark)
    }
}
