//! Exit after the participant marker's WAL intent, before its atomic commit.
use super::*;

pub(super) struct ExitBeforeMarkerCommit(pub Arc<dyn KvEngine>);

impl KvEngine for ExitBeforeMarkerCommit {
    fn get(&self, key: &[u8], ts: Timestamp) -> Result<Option<Bytes>> {
        self.0.get(key, ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.0.scan(range, ts)
    }
    fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.0.write_intent(txn, key, value)
    }
    fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
        self.0.delete_intent(txn, key)
    }
    fn write_intent_batch(
        &self,
        txn: TxnId,
        operations: Vec<nodus_storage_api::IntentOperation>,
    ) -> KvResult<()> {
        self.0.write_intent_batch(txn, operations)
    }
    fn replace_intent(
        &self,
        txn: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        self.0.replace_intent(txn, key, replacement)
    }
    fn commit(&self, txn: TxnId, ts: Timestamp) -> KvResult<()> {
        let keys = self.0.pending_intent_keys(txn);
        if keys.len() > 1
            && keys.iter().any(|key| {
                key.windows(participant::PREFIX.len())
                    .any(|w| w == participant::PREFIX)
            })
        {
            // Active/prepared control writes have one marker key. Only the
            // final user transaction holds both its data and terminal marker.
            std::process::exit(90);
        }
        self.0.commit(txn, ts)
    }
    fn abort(&self, txn: TxnId) -> KvResult<()> {
        self.0.abort(txn)
    }
    fn pending_intent_keys(&self, txn: TxnId) -> Vec<Bytes> {
        self.0.pending_intent_keys(txn)
    }
    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        self.0.has_pending_intents(prefix)
    }
    fn pending_intents(&self, prefix: &[u8]) -> Result<Vec<nodus_storage_api::PendingIntents>> {
        self.0.pending_intents(prefix)
    }
    fn snapshot_rows(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
    ) -> Result<Vec<nodus_storage_api::SnapshotRow>> {
        self.0.snapshot_rows(scope)
    }
    fn replace_snapshot(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
        rows: Vec<nodus_storage_api::SnapshotRow>,
        pointers: Vec<nodus_storage_api::SnapshotRow>,
    ) -> Result<()> {
        self.0.replace_snapshot(scope, rows, pointers)
    }
}
