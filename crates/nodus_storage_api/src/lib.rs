use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{IndexId, TableId};
use serde::{Deserialize, Serialize};
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
