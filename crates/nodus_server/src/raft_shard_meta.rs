use anyhow::Result;
use nodus_catalog::{ShardId, TableId};
use nodus_meta::{MetaStore, ShardMap};
use nodus_raftstore::ShardCommand;
use std::collections::HashMap;
use std::sync::Arc;

use crate::multi_raft::META_SHARD;
use crate::raft_router::RaftRouter;

/// `MetaStore` that replicates shard-map and placement **writes** through the
/// meta (`shard-meta`) Raft group, so every node's local store converges on the
/// same routing state. Reads go straight to the local store, which the meta
/// group's apply path keeps current.
///
/// Writes route through the async [`RaftRouter`], whose `submit` waits via
/// `blocking_recv` — so they MUST be invoked from a blocking context (e.g.
/// inside `tokio::task::spawn_blocking`), never directly on a runtime worker.
pub struct RaftShardMetaStore {
    pub local: Arc<dyn MetaStore>,
    pub router: RaftRouter,
}

impl MetaStore for RaftShardMetaStore {
    fn get_shard_map(&self, table_id: TableId) -> Result<ShardMap> {
        self.local.get_shard_map(table_id)
    }

    fn update_shard_map(&self, shard_map: ShardMap) -> Result<()> {
        self.router
            .submit(META_SHARD, ShardCommand::UpdateShardMap(shard_map))
            .map_err(|e| anyhow::anyhow!("update_shard_map raft error: {e}"))
    }

    fn get_shard_placements(&self) -> Result<HashMap<ShardId, String>> {
        self.local.get_shard_placements()
    }

    fn update_shard_placements(&self, placements: &HashMap<ShardId, String>) -> Result<()> {
        self.router
            .submit(
                META_SHARD,
                ShardCommand::UpdateShardPlacements(placements.clone()),
            )
            .map_err(|e| anyhow::anyhow!("update_shard_placements raft error: {e}"))
    }
}
