use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{ShardDescriptor, TableId};
use nodus_storage_api::{KvEngine, TxnId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMap {
    pub table_id: TableId,
    pub shards: Vec<ShardDescriptor>,
}

/// On-disk format version of persisted meta-store records (shard maps,
/// placements).
const META_RECORD_VERSION: u16 = 1;

/// Wraps a JSON payload in a versioned envelope for durable storage.
fn encode_meta(payload: Vec<u8>) -> Bytes {
    Bytes::from(nodus_common::versioned::encode(
        META_RECORD_VERSION,
        &payload,
    ))
}

/// Returns the JSON payload of a persisted meta record, dispatching on its
/// envelope version and accepting legacy (pre-envelope) JSON. An unknown future
/// version is a hard error rather than a misparse.
fn decode_meta(bytes: &[u8]) -> Result<&[u8]> {
    use nodus_common::versioned::{Envelope, decode};
    match decode(bytes) {
        Envelope::Versioned { version, payload } if version == META_RECORD_VERSION => Ok(payload),
        Envelope::Versioned { version, .. } => anyhow::bail!(
            "unsupported meta record version {version}; this binary supports {META_RECORD_VERSION}"
        ),
        Envelope::Legacy(legacy) => Ok(legacy),
    }
}

pub trait MetaStore: Send + Sync {
    fn get_shard_map(&self, table_id: TableId) -> Result<ShardMap>;
    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()>;
    fn get_shard_placements(&self) -> Result<HashMap<nodus_catalog::ShardId, String>>;
    fn update_shard_placements(
        &self,
        placements: &HashMap<nodus_catalog::ShardId, String>,
    ) -> Result<()>;
}

pub struct MemMetaStore {
    maps: RwLock<HashMap<TableId, ShardMap>>,
    placements: RwLock<HashMap<nodus_catalog::ShardId, String>>,
}

impl Default for MemMetaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemMetaStore {
    pub fn new() -> Self {
        Self {
            maps: RwLock::new(HashMap::new()),
            placements: RwLock::new(HashMap::new()),
        }
    }
}

impl MetaStore for MemMetaStore {
    fn get_shard_map(&self, table_id: TableId) -> Result<ShardMap> {
        let guard = self.maps.read().unwrap();
        guard
            .get(&table_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Shard map not found for table {}", table_id))
    }

    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()> {
        let mut guard = self.maps.write().unwrap();
        guard.insert(shard_map.table_id, shard_map);
        Ok(())
    }

    fn get_shard_placements(&self) -> Result<HashMap<nodus_catalog::ShardId, String>> {
        Ok(self.placements.read().unwrap().clone())
    }

    fn update_shard_placements(
        &self,
        placements: &HashMap<nodus_catalog::ShardId, String>,
    ) -> Result<()> {
        *self.placements.write().unwrap() = placements.clone();
        Ok(())
    }
}

pub struct PersistentMetaStore {
    kv: Arc<dyn KvEngine>,
}

impl PersistentMetaStore {
    pub fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self { kv }
    }

    fn key_for(table_id: TableId) -> Bytes {
        Bytes::from(format!("meta:shard_map:{}", table_id))
    }

    fn placements_key() -> Bytes {
        Bytes::from("meta:shard_placements")
    }
}

impl MetaStore for PersistentMetaStore {
    fn get_shard_map(&self, table_id: TableId) -> Result<ShardMap> {
        let key = Self::key_for(table_id);
        let val = self.kv.get(&key, u64::MAX)?;
        if let Some(bytes) = val {
            let map: ShardMap = serde_json::from_slice(decode_meta(&bytes)?)?;
            Ok(map)
        } else {
            anyhow::bail!("Shard map not found for table {}", table_id)
        }
    }

    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()> {
        let key = Self::key_for(shard_map.table_id);
        let val = encode_meta(serde_json::to_vec(&shard_map)?);

        let txn_id = TxnId::new();
        self.kv.write_intent(txn_id, key, val)?;

        // Use current timestamp as commit_ts
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        self.kv.commit(txn_id, ts)?;
        Ok(())
    }

    fn get_shard_placements(&self) -> Result<HashMap<nodus_catalog::ShardId, String>> {
        let key = Self::placements_key();
        let val = self.kv.get(&key, u64::MAX)?;
        if let Some(bytes) = val {
            let map: HashMap<nodus_catalog::ShardId, String> =
                serde_json::from_slice(decode_meta(&bytes)?)?;
            Ok(map)
        } else {
            Ok(HashMap::new())
        }
    }

    fn update_shard_placements(
        &self,
        placements: &HashMap<nodus_catalog::ShardId, String>,
    ) -> Result<()> {
        let key = Self::placements_key();
        let val = encode_meta(serde_json::to_vec(placements)?);

        let txn_id = TxnId::new();
        self.kv.write_intent(txn_id, key, val)?;

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        self.kv.commit(txn_id, ts)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_records_round_trip_through_the_envelope() {
        let encoded = encode_meta(serde_json::to_vec(&"payload").unwrap());
        // The stored bytes carry the versioned envelope...
        assert!(matches!(
            nodus_common::versioned::decode(&encoded),
            nodus_common::versioned::Envelope::Versioned {
                version: META_RECORD_VERSION,
                ..
            }
        ));
        // ...and decode recovers the original JSON payload.
        let value: String = serde_json::from_slice(decode_meta(&encoded).unwrap()).unwrap();
        assert_eq!(value, "payload");
    }

    #[test]
    fn legacy_unversioned_meta_records_still_decode() {
        let legacy = serde_json::to_vec(&"payload").unwrap();
        let value: String = serde_json::from_slice(decode_meta(&legacy).unwrap()).unwrap();
        assert_eq!(value, "payload");
    }

    #[test]
    fn unknown_meta_record_version_fails_loudly() {
        let future = nodus_common::versioned::encode(META_RECORD_VERSION + 1, b"{}");
        assert!(decode_meta(&future).is_err());
    }
}
