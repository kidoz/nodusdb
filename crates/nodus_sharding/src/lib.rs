use anyhow::Result;
use nodus_catalog::{DescriptorState, ShardDescriptor, ShardId, TableId};
use nodus_meta::MetaStore;
use nodus_storage_api::KeyRange;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

// Shard administration: split, merge, move, and rebalance. These operations
// preserve the invariant that, for a table, shard ranges form a contiguous,
// non-overlapping cover of the key space — no key is ever lost or duplicated.
fn new_shard(table_id: TableId, name: String, start_key: Vec<u8>, end_key: Vec<u8>) -> ShardDescriptor {
    let now = chrono::Utc::now();
    ShardDescriptor {
        id: ShardId::new(),
        name,
        version: 1,
        created_at: now,
        updated_at: now,
        state: DescriptorState::Public,
        table_id,
        start_key,
        end_key,
    }
}

pub struct ShardOrchestrator {
    meta: Arc<dyn MetaStore>,
    /// ShardId -> node id. Placement lives here since the descriptor has none.
    placements: RwLock<HashMap<ShardId, String>>,
}

impl ShardOrchestrator {
    pub fn new(meta: Arc<dyn MetaStore>) -> Self {
        Self {
            meta,
            placements: RwLock::new(HashMap::new()),
        }
    }

    /// Splits `shard_id` at `split_key`, producing `[start, split_key)` and
    /// `[split_key, end)`. The split key must lie strictly inside the shard.
    pub fn split(
        &self,
        table_id: TableId,
        shard_id: ShardId,
        split_key: Vec<u8>,
    ) -> Result<(ShardId, ShardId)> {
        let mut map = self.meta.get_shard_map(table_id)?;
        let idx = map
            .shards
            .iter()
            .position(|s| s.id == shard_id)
            .ok_or_else(|| anyhow::anyhow!("shard {shard_id} not found"))?;
        let shard = &map.shards[idx];

        let after_start = shard.start_key.is_empty() || split_key.as_slice() > shard.start_key.as_slice();
        let before_end = shard.end_key.is_empty() || split_key.as_slice() < shard.end_key.as_slice();
        if split_key.is_empty() || !after_start || !before_end {
            anyhow::bail!("split key is not strictly within shard {shard_id}");
        }

        let left = new_shard(
            table_id,
            format!("{}-l", shard.name),
            shard.start_key.clone(),
            split_key.clone(),
        );
        let right = new_shard(
            table_id,
            format!("{}-r", shard.name),
            split_key,
            shard.end_key.clone(),
        );
        let ids = (left.id, right.id);

        // New shards inherit the original's placement. Read the inherited node
        // into a local first so the read guard is dropped before we write.
        let inherited = self.placements.read().unwrap().get(&shard_id).cloned();
        if let Some(node) = inherited {
            let mut p = self.placements.write().unwrap();
            p.insert(left.id, node.clone());
            p.insert(right.id, node);
        }

        map.shards.remove(idx);
        map.shards.push(left);
        map.shards.push(right);
        self.meta.update_shard_map(map)?;
        self.placements.write().unwrap().remove(&shard_id);
        Ok(ids)
    }

    /// Merges two adjacent shards (`left.end_key == right.start_key`) into one
    /// spanning `[left.start, right.end)`.
    pub fn merge(&self, table_id: TableId, left_id: ShardId, right_id: ShardId) -> Result<ShardId> {
        let mut map = self.meta.get_shard_map(table_id)?;
        let left = map
            .shards
            .iter()
            .find(|s| s.id == left_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("shard {left_id} not found"))?;
        let right = map
            .shards
            .iter()
            .find(|s| s.id == right_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("shard {right_id} not found"))?;
        if left.end_key.is_empty() || left.end_key != right.start_key {
            anyhow::bail!("shards {left_id} and {right_id} are not adjacent");
        }

        let merged = new_shard(
            table_id,
            format!("{}+{}", left.name, right.name),
            left.start_key.clone(),
            right.end_key.clone(),
        );
        let merged_id = merged.id;
        map.shards.retain(|s| s.id != left_id && s.id != right_id);
        map.shards.push(merged);
        self.meta.update_shard_map(map)?;
        let mut p = self.placements.write().unwrap();
        p.remove(&left_id);
        p.remove(&right_id);
        Ok(merged_id)
    }

    /// Assigns a shard's replica/leader placement to `node`.
    pub fn move_shard(&self, shard_id: ShardId, node: &str) -> Result<()> {
        self.placements
            .write()
            .unwrap()
            .insert(shard_id, node.to_string());
        Ok(())
    }

    pub fn placement(&self, shard_id: ShardId) -> Option<String> {
        self.placements.read().unwrap().get(&shard_id).cloned()
    }

    /// Evenly distributes a table's shards across `nodes` (round-robin by start
    /// key) and records the resulting placements.
    pub fn rebalance(&self, table_id: TableId, nodes: &[String]) -> Result<()> {
        if nodes.is_empty() {
            anyhow::bail!("cannot rebalance onto zero nodes");
        }
        let map = self.meta.get_shard_map(table_id)?;
        let mut shards = map.shards.clone();
        shards.sort_by(|a, b| sort_key(&a.start_key).cmp(&sort_key(&b.start_key)));
        let mut p = self.placements.write().unwrap();
        for (i, shard) in shards.iter().enumerate() {
            p.insert(shard.id, nodes[i % nodes.len()].clone());
        }
        Ok(())
    }
}

/// Orders start keys with empty (-infinity) sorting first.
fn sort_key(start_key: &[u8]) -> (u8, Vec<u8>) {
    if start_key.is_empty() {
        (0, Vec::new())
    } else {
        (1, start_key.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use nodus_meta::{MemMetaStore, ShardMap};

    /// Asserts the shards form a contiguous, non-overlapping cover from
    /// -infinity to +infinity — i.e. no key lost or duplicated.
    fn assert_contiguous_cover(map: &ShardMap) {
        let mut shards = map.shards.clone();
        shards.sort_by(|a, b| sort_key(&a.start_key).cmp(&sort_key(&b.start_key)));
        assert!(shards.first().unwrap().start_key.is_empty(), "missing -inf start");
        assert!(shards.last().unwrap().end_key.is_empty(), "missing +inf end");
        for w in shards.windows(2) {
            assert_eq!(w[0].end_key, w[1].start_key, "gap or overlap between shards");
        }
    }

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

    fn single_shard_table() -> (Arc<MemMetaStore>, TableId, ShardId) {
        let meta = Arc::new(MemMetaStore::new());
        let table_id = TableId::new();
        let shard = new_shard(table_id, "s0".into(), vec![], vec![]);
        let shard_id = shard.id;
        meta.update_shard_map(ShardMap {
            table_id,
            shards: vec![shard],
        })
        .unwrap();
        (meta, table_id, shard_id)
    }

    #[test]
    fn split_then_merge_preserves_cover() {
        let (meta, table_id, shard_id) = single_shard_table();
        let orch = ShardOrchestrator::new(meta.clone());

        let (left, right) = orch.split(table_id, shard_id, vec![50]).unwrap();
        let map = meta.get_shard_map(table_id).unwrap();
        assert_eq!(map.shards.len(), 2);
        assert_contiguous_cover(&map);

        // A key routes to exactly one of the new shards.
        let router = CatalogShardRouter::new(meta.clone());
        assert_eq!(router.locate_key(table_id, &[10]).unwrap(), left);
        assert_eq!(router.locate_key(table_id, &[90]).unwrap(), right);

        // Splitting on an out-of-range key is rejected.
        assert!(orch.split(table_id, left, vec![200]).is_err());

        let merged = orch.merge(table_id, left, right).unwrap();
        let map = meta.get_shard_map(table_id).unwrap();
        assert_eq!(map.shards.len(), 1);
        assert_eq!(map.shards[0].id, merged);
        assert_contiguous_cover(&map);
    }

    #[test]
    fn rebalance_and_move_assign_placements() {
        let (meta, table_id, shard_id) = single_shard_table();
        let orch = ShardOrchestrator::new(meta.clone());
        orch.move_shard(shard_id, "node-a").unwrap();
        assert_eq!(orch.placement(shard_id).as_deref(), Some("node-a"));

        let (left, right) = orch.split(table_id, shard_id, vec![50]).unwrap();
        // Split children inherit placement.
        assert_eq!(orch.placement(left).as_deref(), Some("node-a"));

        let nodes = vec!["n1".to_string(), "n2".to_string()];
        orch.rebalance(table_id, &nodes).unwrap();
        let pl = orch.placement(left);
        let pr = orch.placement(right);
        assert!(pl.is_some() && pr.is_some());
        assert_ne!(pl, pr, "two shards should spread across two nodes");
    }
}
