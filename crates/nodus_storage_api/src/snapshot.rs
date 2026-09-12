//! Bounded, atomic replacement of one logical Raft group's state.
use super::*;

/// Checkpoint construction currently materializes state; reject oversized work
/// before publication instead of allowing unbounded memory growth.
pub const SNAPSHOT_MEMORY_LIMIT: usize = 128 * 1024 * 1024;

#[derive(Clone, Default)]
pub struct SnapshotScope {
    pub prefix: Vec<u8>,
    pub exclude_raft: bool,
    /// The meta group owns unprefixed keys. Physical data-group namespaces use
    /// the server's `shard-*\0` convention and must never enter its snapshot.
    pub exclude_data_groups: bool,
}

impl SnapshotScope {
    pub fn owns(&self, key: &[u8]) -> bool {
        let Some(key) = key.strip_prefix(self.prefix.as_slice()) else {
            return false;
        };
        !(self.exclude_raft && (key.starts_with(b"\0raft\0") || key.starts_with(b"\0hlc\0"))
            || self.exclude_data_groups && key.starts_with(b"shard-") && key.contains(&0))
    }

    pub fn namespaced(&self, prefix: &[u8]) -> Self {
        let mut scope = self.clone();
        scope.prefix = [prefix, self.prefix.as_slice()].concat();
        scope
    }
}

#[derive(Clone)]
pub struct SnapshotValue {
    pub value: Option<Vec<u8>>,
    pub version: Timestamp,
    pub txn_id: Option<TxnId>,
    pub is_intent: bool,
}

#[derive(Clone)]
pub struct SnapshotRow {
    pub key: Bytes,
    pub versions: Vec<SnapshotValue>,
}

impl SnapshotRow {
    pub fn committed(key: Bytes, value: Bytes, version: Timestamp) -> Self {
        Self {
            key,
            versions: vec![SnapshotValue {
                value: Some(value.to_vec()),
                version,
                txn_id: None,
                is_intent: false,
            }],
        }
    }

    pub fn memory_size(&self) -> usize {
        self.key.len().saturating_add(
            self.versions
                .iter()
                .map(|v| v.value.as_ref().map_or(0, Vec::len).saturating_add(64))
                .sum::<usize>(),
        )
    }
}

pub fn check_snapshot_size(rows: &[SnapshotRow]) -> Result<()> {
    let mut size = 0usize;
    for row in rows {
        size = size
            .checked_add(row.memory_size())
            .ok_or_else(|| anyhow::anyhow!("snapshot size overflow"))?;
        anyhow::ensure!(
            size <= SNAPSHOT_MEMORY_LIMIT,
            "snapshot exceeds 128 MiB checkpoint memory limit"
        );
    }
    Ok(())
}
