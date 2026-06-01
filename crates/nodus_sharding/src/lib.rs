use anyhow::Result;
use nodus_catalog::{ShardId, TableId};
use nodus_storage_api::KeyRange;

pub trait ShardRouter: Send + Sync {
    fn locate_key(&self, table_id: TableId, key: &[u8]) -> Result<ShardId>;
    fn locate_range(&self, table_id: TableId, range: KeyRange) -> Result<Vec<ShardId>>;
}

// In-Memory MVP Implementation
use std::collections::HashMap;
use std::sync::RwLock;

pub struct MemShardRouter {
    // simplified: table -> shard mapping
    routes: RwLock<HashMap<TableId, ShardId>>,
}

impl Default for MemShardRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl MemShardRouter {
    pub fn new() -> Self {
        Self {
            routes: RwLock::new(HashMap::new()),
        }
    }

    pub fn assign_shard(&self, table_id: TableId, shard_id: ShardId) {
        let mut guard = self.routes.write().unwrap();
        guard.insert(table_id, shard_id);
    }
}

impl ShardRouter for MemShardRouter {
    fn locate_key(&self, table_id: TableId, _key: &[u8]) -> Result<ShardId> {
        let guard = self.routes.read().unwrap();
        if let Some(shard_id) = guard.get(&table_id) {
            Ok(*shard_id)
        } else {
            anyhow::bail!("No shard assigned for table {}", table_id);
        }
    }

    fn locate_range(&self, table_id: TableId, _range: KeyRange) -> Result<Vec<ShardId>> {
        let guard = self.routes.read().unwrap();
        if let Some(shard_id) = guard.get(&table_id) {
            Ok(vec![*shard_id])
        } else {
            Ok(vec![])
        }
    }
}
