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
            let map: ShardMap = serde_json::from_slice(&bytes)?;
            Ok(map)
        } else {
            anyhow::bail!("Shard map not found for table {}", table_id)
        }
    }

    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()> {
        let key = Self::key_for(shard_map.table_id);
        let val = Bytes::from(serde_json::to_vec(&shard_map)?);

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
            let map: HashMap<nodus_catalog::ShardId, String> = serde_json::from_slice(&bytes)?;
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
        let val = Bytes::from(serde_json::to_vec(placements)?);

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
