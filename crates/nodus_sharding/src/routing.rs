use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{DescriptorState, ShardId, TableId};
use nodus_meta::{MetaStore, ShardMap, ShardMapNotFound};
use nodus_storage_api::KeyRange;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Range {
    id: ShardId,
    version: u64,
    start: Vec<u8>,
    end: Vec<u8>,
}

/// A validated, immutable table routing view. Equality compares descriptor
/// versions, IDs, and bounds, independent of their serialized order. This is a
/// local topology check, not the durable migration epoch/fence required for
/// online topology changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingSnapshot {
    ranges: Vec<Range>,
}

impl RoutingSnapshot {
    fn from_map(table: TableId, mut map: ShardMap) -> Result<Self> {
        anyhow::ensure!(
            map.table_id == table,
            "invalid shard map: table identity mismatch"
        );
        anyhow::ensure!(
            !map.shards.is_empty(),
            "invalid shard map: empty coverage for {table}"
        );
        map.shards.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        let mut ids = HashSet::new();
        let mut ranges: Vec<Range> = Vec::with_capacity(map.shards.len());
        for shard in map.shards {
            anyhow::ensure!(
                shard.table_id == table && shard.state == DescriptorState::Public,
                "invalid shard map: non-public or mismatched shard {}",
                shard.id
            );
            anyhow::ensure!(
                ids.insert(shard.id),
                "invalid shard map: duplicate shard {}",
                shard.id
            );
            anyhow::ensure!(
                shard.end_key.is_empty() || shard.start_key < shard.end_key,
                "invalid shard map: reversed or empty range"
            );
            if let Some(previous) = ranges.last() {
                anyhow::ensure!(
                    !previous.end.is_empty() && previous.end == shard.start_key,
                    "invalid shard map: gap or overlap"
                );
            } else {
                anyhow::ensure!(
                    shard.start_key.is_empty(),
                    "invalid shard map: missing lower bound"
                );
            }
            ranges.push(Range {
                id: shard.id,
                version: shard.version,
                start: shard.start_key,
                end: shard.end_key,
            });
        }
        anyhow::ensure!(
            ranges.last().is_some_and(|r| r.end.is_empty()),
            "invalid shard map: missing upper bound"
        );
        Ok(Self { ranges })
    }

    /// Finds the unique owner in this complete, disjoint key-space cover.
    pub fn locate_key(&self, key: &[u8]) -> Result<ShardId> {
        self.ranges
            .iter()
            .find(|r| {
                (r.start.is_empty() || key >= r.start.as_slice())
                    && (r.end.is_empty() || key < r.end.as_slice())
            })
            .map(|r| r.id)
            .ok_or_else(|| anyhow::anyhow!("invalid shard map: key has no owner"))
    }

    /// Intersects a bounded half-open scan with each owner, in key order. An
    /// empty or reversed request has no intersections, never an unbounded scan.
    pub fn intersect(&self, range: &KeyRange) -> Vec<(ShardId, KeyRange)> {
        if range.start >= range.end {
            return Vec::new();
        }
        self.ranges
            .iter()
            .filter_map(|r| {
                let start = range.start.as_ref().max(r.start.as_slice());
                let end = if r.end.is_empty() {
                    range.end.as_ref()
                } else {
                    range.end.as_ref().min(r.end.as_slice())
                };
                (start < end).then(|| {
                    (
                        r.id,
                        KeyRange {
                            start: Bytes::copy_from_slice(start),
                            end: Bytes::copy_from_slice(end),
                        },
                    )
                })
            })
            .collect()
    }
}

pub trait ShardRouter: Send + Sync {
    /// `None` means metadata positively reports no map. Metadata corruption,
    /// invalid coverage, and I/O errors must propagate as errors.
    fn snapshot(&self, table_id: TableId) -> Result<Option<RoutingSnapshot>>;

    fn locate_key(&self, table_id: TableId, key: &[u8]) -> Result<ShardId> {
        self.snapshot(table_id)?
            .ok_or(ShardMapNotFound(table_id))?
            .locate_key(key)
    }

    fn locate_range(&self, table_id: TableId, range: KeyRange) -> Result<Vec<ShardId>> {
        Ok(self
            .snapshot(table_id)?
            .ok_or(ShardMapNotFound(table_id))?
            .intersect(&range)
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }
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
    fn snapshot(&self, table_id: TableId) -> Result<Option<RoutingSnapshot>> {
        match self.meta_store.get_shard_map(table_id) {
            Ok(map) => Ok(Some(RoutingSnapshot::from_map(table_id, map)?)),
            Err(e) if e.downcast_ref::<ShardMapNotFound>().is_some() => Ok(None),
            Err(e) => Err(e),
        }
    }
}
