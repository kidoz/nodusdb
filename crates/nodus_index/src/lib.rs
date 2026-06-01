use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use nodus_catalog::{IndexDescriptor, IndexId, TableId};
use nodus_storage_api::{Datum, IndexKvCodec, KvEngine, RowKey, Timestamp, TxnId};
use std::sync::Arc;

pub mod backfill;
pub use backfill::IndexBackfiller;

pub struct DefaultIndexCodec;

impl DefaultIndexCodec {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultIndexCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexKvCodec for DefaultIndexCodec {
    fn encode_primary_key(
        &self,
        table_id: TableId,
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes> {
        let mut buf = BytesMut::with_capacity(16 + primary_key.len() + 8);
        buf.put_u8(b'P'); // Primary Key Prefix
        buf.put_slice(table_id.0.as_bytes());
        buf.put_slice(primary_key);
        buf.put_u64(version_ts); // Version suffix for MVCC separation within the tree if needed, though usually handled by MVCC values.
        Ok(buf.freeze())
    }

    fn encode_secondary_key(
        &self,
        index_id: IndexId,
        values: &[Datum],
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S'); // Secondary Index Prefix
        buf.put_slice(index_id.0.as_bytes());

        for val in values {
            // Simplistic encoding: length-prefixed bytes
            buf.put_u32(val.len() as u32);
            buf.put_slice(val);
        }

        // Always append primary key to make secondary keys unique
        buf.put_slice(primary_key);
        buf.put_u64(version_ts);

        Ok(buf.freeze())
    }
}

pub struct IndexManager {
    kv: Arc<dyn KvEngine>,
    codec: Arc<dyn IndexKvCodec>,
}

impl IndexManager {
    pub fn new(kv: Arc<dyn KvEngine>, codec: Arc<dyn IndexKvCodec>) -> Self {
        Self { kv, codec }
    }

    pub fn insert_primary(
        &self,
        txn_id: TxnId,
        table_id: TableId,
        primary_key: &RowKey,
        row_data: Bytes,
    ) -> Result<()> {
        let key = self.codec.encode_primary_key(table_id, primary_key, 0)?; // MVCC logic lives in KvEngine values
        self.kv.write_intent(txn_id, key, row_data)
    }

    pub fn get_primary(
        &self,
        table_id: TableId,
        primary_key: &RowKey,
        read_ts: Timestamp,
    ) -> Result<Option<Bytes>> {
        let key = self.codec.encode_primary_key(table_id, primary_key, 0)?;
        self.kv.get(key.as_ref(), read_ts)
    }

    pub fn insert_secondary(
        &self,
        txn_id: TxnId,
        index_desc: &IndexDescriptor,
        values: &[Datum],
        primary_key: &RowKey,
    ) -> Result<()> {
        let key = self
            .codec
            .encode_secondary_key(index_desc.id, values, primary_key, 0)?;
        // For secondary indexes, the value is typically empty (or contains included columns).
        // For MVP, we just store an empty byte array.
        self.kv.write_intent(txn_id, key, Bytes::new())
    }

    // We would add secondary index point lookups and range scans here.
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_storage_api::{KvEngine, TxnId};
    use nodus_storage_mem::MemKvEngine;

    #[test]
    fn test_index_encoding_and_write() {
        let kv: Arc<dyn KvEngine> = Arc::new(MemKvEngine::new());
        let codec: Arc<dyn IndexKvCodec> = Arc::new(DefaultIndexCodec::new());
        let index_manager = IndexManager::new(kv.clone(), codec);

        let txn_id = TxnId::new();
        let table_id = TableId::new();
        let pk = Bytes::from("user_123");
        let row_data = Bytes::from("alice");

        index_manager
            .insert_primary(txn_id, table_id, &pk, row_data.clone())
            .unwrap();

        // Needs to be committed to be readable
        kv.commit(txn_id, 10).unwrap();

        let read_back = index_manager.get_primary(table_id, &pk, 10).unwrap();
        assert_eq!(read_back.unwrap(), row_data);
    }
}
