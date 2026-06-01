use anyhow::Result;
use nodus_catalog::{ShardDescriptor, TableId};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ShardMap {
    pub table_id: TableId,
    pub shards: Vec<ShardDescriptor>,
}

pub trait MetaStore: Send + Sync {
    fn get_shard_map(&self, table_id: TableId) -> Result<ShardMap>;
    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()>;
}

pub struct MemMetaStore {
    maps: RwLock<HashMap<TableId, ShardMap>>,
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
}
