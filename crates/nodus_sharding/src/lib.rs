use anyhow::Result;
use nodus_catalog::{ShardId, TableId};
use nodus_meta::MetaStore;
use nodus_storage_api::KeyRange;
use std::sync::Arc;

pub trait ShardRouter: Send + Sync {
    fn locate_key(&self, table_id: TableId, key: &[u8]) -> Result<ShardId>;
    fn locate_range(&self, table_id: TableId, range: KeyRange) -> Result<Vec<ShardId>>;
}

pub struct CatalogShardRouter {
    meta_store: Arc<dyn MetaStore>,
}

impl CatalogShardRouter {
    pub fn new(meta_store: Arc<dyn MetaStore>) -> Self {
        Self { meta_store }
    }
}

impl ShardRouter for CatalogShardRouter {
    fn locate_key(&self, table_id: TableId, key: &[u8]) -> Result<ShardId> {
        let map = self.meta_store.get_shard_map(table_id)?;
        for shard in &map.shards {
            // Empty start_key means -infinity, empty end_key means +infinity for this MVP
            let start_ok = shard.start_key.is_empty() || key >= shard.start_key.as_slice();
            let end_ok = shard.end_key.is_empty() || key < shard.end_key.as_slice();
            if start_ok && end_ok {
                return Ok(shard.id);
            }
        }
        anyhow::bail!("No shard found for key in table {}", table_id);
    }

    fn locate_range(&self, table_id: TableId, range: KeyRange) -> Result<Vec<ShardId>> {
        let map = self.meta_store.get_shard_map(table_id)?;
        let mut result = Vec::new();
        for shard in &map.shards {
            let start_ok =
                shard.end_key.is_empty() || range.start.as_ref() < shard.end_key.as_slice();
            let end_ok =
                shard.start_key.is_empty() || range.end.as_ref() > shard.start_key.as_slice();
            if start_ok && end_ok {
                result.push(shard.id);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use nodus_catalog::{DescriptorState, ShardDescriptor};
    use nodus_meta::MemMetaStore;

    #[test]
    fn test_shard_routing() {
        let meta = Arc::new(MemMetaStore::new());
        let table_id = TableId::new();

        let shard1 = ShardDescriptor {
            id: ShardId::new(),
            name: "shard1".into(),
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            table_id,
            start_key: vec![], // -inf
            end_key: vec![10],
        };

        let shard2 = ShardDescriptor {
            id: ShardId::new(),
            name: "shard2".into(),
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: DescriptorState::Public,
            table_id,
            start_key: vec![10],
            end_key: vec![], // +inf
        };

        let map = nodus_meta::ShardMap {
            table_id,
            shards: vec![shard1.clone(), shard2.clone()],
        };

        meta.update_shard_map(map).unwrap();

        let router = CatalogShardRouter::new(meta);

        let s_id1 = router.locate_key(table_id, &[5]).unwrap();
        assert_eq!(s_id1, shard1.id);

        let s_id2 = router.locate_key(table_id, &[15]).unwrap();
        assert_eq!(s_id2, shard2.id);

        let s_ids = router
            .locate_range(
                table_id,
                KeyRange {
                    start: Bytes::from(vec![5]),
                    end: Bytes::from(vec![15]),
                },
            )
            .unwrap();
        assert_eq!(s_ids.len(), 2);
    }
}
