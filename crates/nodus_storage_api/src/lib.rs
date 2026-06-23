use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{IndexId, TableId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub type Timestamp = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId(pub Uuid);

impl TxnId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TxnId {
    fn default() -> Self {
        Self::new()
    }
}

pub struct KeyRange {
    pub start: Bytes,
    pub end: Bytes,
}

pub struct KvPair {
    pub key: Bytes,
    pub value: Bytes,
    pub version: Timestamp,
}

pub enum IntentReplacement {
    Put(Bytes),
    Delete,
    Clear,
}

pub trait KvEngine: Send + Sync {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>>;
    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>>;
    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()>;
    /// Writes a deletion (tombstone) intent for `key`. After commit the key
    /// reads as absent at timestamps at or after the commit.
    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()>;
    /// Replaces or clears this transaction's current intent for one key.
    /// Used by SQL savepoints to restore the uncommitted state that existed
    /// when the savepoint was created.
    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> Result<()>;
    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()>;
    fn abort(&self, txn_id: TxnId) -> Result<()>;

    /// Reclaims MVCC versions that no active reader can observe: for each key,
    /// committed versions strictly older than the newest version at or below
    /// `watermark` are removed. `watermark` must be ≤ the oldest active read
    /// timestamp. Returns the number of versions reclaimed. Default: no-op.
    fn garbage_collect(&self, _watermark: Timestamp) -> Result<usize> {
        Ok(0)
    }

    /// Flushes any in-memory data to persistent storage and rotates the write-ahead log.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// A `KvEngine` view that confines all keys to a per-shard namespace by
/// prefixing them, so multiple Raft groups can share one underlying store
/// without their key spaces overlapping. Keys are transparently prefixed on
/// write and stripped on read; scans are translated into the namespace's
/// physical key range. As a result, a group's snapshot (a full scan of its
/// engine) only ever observes its own namespace's keys.
///
/// Transaction-scoped operations (`commit`/`abort`) and `garbage_collect`
/// delegate to the inner engine unchanged: they act per transaction id or by
/// watermark, not per key, and a given transaction writes within one namespace.
pub struct NamespacedKvEngine {
    inner: Arc<dyn KvEngine>,
    prefix: Bytes,
}

impl NamespacedKvEngine {
    /// Builds a namespace from `namespace` plus a `0x00` separator. The
    /// separator guarantees one namespace's prefix is never a prefix of
    /// another's (e.g. `shard-1` vs `shard-10`).
    pub fn new(inner: Arc<dyn KvEngine>, namespace: &str) -> Self {
        let mut prefix = Vec::with_capacity(namespace.len() + 1);
        prefix.extend_from_slice(namespace.as_bytes());
        prefix.push(0u8);
        Self {
            inner,
            prefix: Bytes::from(prefix),
        }
    }

    fn physical_key(&self, key: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(self.prefix.len() + key.len());
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(key);
        Bytes::from(out)
    }

    /// Derives a per-namespace transaction id. `commit`/`abort` on the shared
    /// inner engine are keyed by transaction id and would otherwise finalize a
    /// transaction's intents across *every* namespace at once; mapping the id
    /// deterministically per namespace keeps each shard's commit independent
    /// (essential for cross-shard 2PC, where one shard may commit before
    /// another). The mapping is deterministic so writes and their later
    /// commit/abort agree.
    fn namespaced_txn(&self, txn_id: TxnId) -> TxnId {
        let ns = Uuid::new_v5(&Uuid::NAMESPACE_OID, &self.prefix);
        TxnId(Uuid::new_v5(&ns, txn_id.0.as_bytes()))
    }
}

impl KvEngine for NamespacedKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(&self.physical_key(key), read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let physical = KeyRange {
            start: self.physical_key(range.start.as_ref()),
            end: self.physical_key(range.end.as_ref()),
        };
        let prefix = self.prefix.clone();
        let iter = self.inner.scan(physical, read_ts)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|pair| {
                let key = match pair.key.strip_prefix(prefix.as_ref()) {
                    Some(suffix) => Bytes::copy_from_slice(suffix),
                    None => pair.key,
                };
                KvPair {
                    key,
                    value: pair.value,
                    version: pair.version,
                }
            })
        })))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        self.inner
            .write_intent(self.namespaced_txn(txn_id), self.physical_key(&key), value)
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        self.inner
            .delete_intent(self.namespaced_txn(txn_id), self.physical_key(&key))
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> Result<()> {
        self.inner
            .replace_intent(self.namespaced_txn(txn_id), self.physical_key(&key), replacement)
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        self.inner.commit(self.namespaced_txn(txn_id), commit_ts)
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        self.inner.abort(self.namespaced_txn(txn_id))
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.inner.garbage_collect(watermark)
    }

    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

pub type RowKey = Bytes;
pub type Datum = Bytes; // simplified

pub trait IndexKvCodec {
    fn encode_primary_key(
        &self,
        table_id: TableId,
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes>;
    fn encode_secondary_key(
        &self,
        index_id: IndexId,
        values: &[Datum],
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes>;
}
