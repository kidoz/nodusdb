use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccError {
    WriteConflict { existing_txn: TxnId },
    IntentNotFound { txn_id: TxnId },
}

impl fmt::Display for MvccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MvccError::WriteConflict { existing_txn } => {
                write!(
                    f,
                    "Write-write conflict with transaction: {}",
                    existing_txn.0
                )
            }
            MvccError::IntentNotFound { txn_id } => {
                write!(f, "Intent not found for transaction: {}", txn_id.0)
            }
        }
    }
}

impl std::error::Error for MvccError {}

impl From<MvccError> for nodus_storage_api::KvError {
    fn from(err: MvccError) -> Self {
        match err {
            MvccError::WriteConflict { existing_txn } => {
                nodus_storage_api::KvError::WriteConflict(existing_txn)
            }
            MvccError::IntentNotFound { txn_id } => {
                nodus_storage_api::KvError::IntentNotFound(txn_id)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MvccValue {
    pub value: Option<Vec<u8>>, // None = Tombstone
    pub version: Timestamp,
    pub txn_id: Option<TxnId>,
    pub is_intent: bool,
}

impl MvccValue {
    pub fn is_visible(&self, read_ts: Timestamp) -> bool {
        !self.is_intent && self.version <= read_ts
    }
}

/// The outcome of reading a version chain at a timestamp, distinguishing
/// "no version is visible" from "the newest visible version is a tombstone".
/// A store that falls through to an older source on `Absent` must stop on
/// `Tombstone`: a newer committed delete hides every value in older sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibleVersion<'a> {
    /// No committed version is visible at the read timestamp.
    Absent,
    /// The newest visible version is a tombstone: the key is deleted.
    Tombstone,
    /// The newest visible version's value.
    Value(&'a [u8]),
}

/// Whether a garbage-collected chain is layered over older sources that may
/// still hold the key, or is the key's complete remaining history. This
/// decides what may happen to a newest-visible tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcMode {
    /// The chain sits above older sources (a memtable over SSTables). The
    /// newest visible tombstone must be RETAINED: erasing it would empty the
    /// chain, and a reader falling through to an older source would resurrect
    /// the deleted value.
    Layered,
    /// The chain is the key's complete history (compaction output merging all
    /// SSTables, or a single-source store). A newest-visible tombstone at or
    /// below the watermark is fully reclaimable — no older source remains
    /// that could resurrect the key — which bounds tombstone accumulation.
    Complete,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VersionChain {
    pub versions: Vec<MvccValue>,
}

impl VersionChain {
    pub fn new() -> Self {
        Self {
            versions: Vec::new(),
        }
    }

    /// Read the newest version visible at `read_ts`, telling a visible
    /// tombstone apart from the absence of any visible version.
    pub fn read_visible(&self, read_ts: Timestamp) -> VisibleVersion<'_> {
        match self
            .versions
            .iter()
            .filter(|v| v.is_visible(read_ts))
            .max_by_key(|v| v.version)
        {
            Some(version) => match &version.value {
                Some(value) => VisibleVersion::Value(value),
                None => VisibleVersion::Tombstone,
            },
            None => VisibleVersion::Absent,
        }
    }

    /// Read the most recent visible value for the given timestamp.
    pub fn read(&self, read_ts: Timestamp) -> Option<&[u8]> {
        match self.read_visible(read_ts) {
            VisibleVersion::Value(value) => Some(value),
            VisibleVersion::Absent | VisibleVersion::Tombstone => None,
        }
    }

    /// Finds an existing active intent. In standard MVCC, there is usually only
    /// one active intent per key at a time.
    pub fn active_intent(&self) -> Option<&MvccValue> {
        self.versions.iter().rev().find(|v| v.is_intent)
    }

    /// Write an intent (either put or delete) for a given transaction.
    fn write_intent_impl(
        &mut self,
        txn_id: TxnId,
        value: Option<Vec<u8>>,
    ) -> Result<(), MvccError> {
        if let Some(intent) = self.active_intent() {
            if intent.txn_id != Some(txn_id) {
                return Err(MvccError::WriteConflict {
                    existing_txn: intent.txn_id.expect("intent must have txn_id"),
                });
            } else {
                // Same transaction is overwriting its own intent.
                // We'll clean up the old one first.
                self.versions
                    .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
            }
        }

        self.versions.push(MvccValue {
            value,
            version: u64::MAX, // intents represent uncommitted state
            txn_id: Some(txn_id),
            is_intent: true,
        });

        Ok(())
    }

    pub fn write_intent(&mut self, txn_id: TxnId, value: Vec<u8>) -> Result<(), MvccError> {
        self.write_intent_impl(txn_id, Some(value))
    }

    pub fn delete_intent(&mut self, txn_id: TxnId) -> Result<(), MvccError> {
        self.write_intent_impl(txn_id, None)
    }

    pub fn commit(&mut self, txn_id: TxnId, commit_ts: Timestamp) -> Result<(), MvccError> {
        // Intents are appended, so the search starts from the end.
        let intent_idx = self
            .versions
            .iter()
            .rposition(|v| v.is_intent && v.txn_id == Some(txn_id))
            .ok_or(MvccError::IntentNotFound { txn_id })?;

        // Committed in place: readers take the newest visible version by
        // timestamp, so the chain needs no order, and a long chain (a key
        // rewritten on every Raft apply) costs no sort per commit.
        let intent = &mut self.versions[intent_idx];
        intent.is_intent = false;
        intent.version = commit_ts;

        Ok(())
    }

    pub fn abort(&mut self, txn_id: TxnId) -> Result<(), MvccError> {
        let initial_len = self.versions.len();
        self.versions
            .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));

        if self.versions.len() == initial_len {
            return Err(MvccError::IntentNotFound { txn_id });
        }

        Ok(())
    }

    /// Reclaims MVCC versions strictly older than the `watermark`.
    /// Leaves at least one version strictly older than watermark so that
    /// reads at watermark still see the correct value. `mode` decides whether
    /// a newest-visible tombstone may itself be reclaimed (see [`GcMode`]).
    pub fn garbage_collect(&mut self, watermark: Timestamp, mode: GcMode) -> usize {
        // We only care about committed versions. Intents are untouched.
        let mut committed: Vec<_> = self.versions.iter().filter(|v| !v.is_intent).collect();
        committed.sort_by_key(|v| std::cmp::Reverse(v.version)); // Newest first

        let mut keep_idx = None;
        for (i, v) in committed.iter().enumerate() {
            if v.version <= watermark {
                // This is the newest version visible at watermark.
                keep_idx = Some(i);
                break;
            }
        }

        let mut reclaimed = 0;
        if let Some(keep_idx) = keep_idx {
            // We keep the one at keep_idx, but any older committed versions can be dropped.
            if keep_idx + 1 < committed.len() {
                // Everything committed before the kept version goes.
                let keep_version = committed[keep_idx].version;
                reclaimed = committed.len() - keep_idx - 1;
                self.versions
                    .retain(|v| v.is_intent || v.version >= keep_version);

                // If the visible element AT watermark is a tombstone, and there are NO versions newer
                // than watermark, the key is logically fully deleted for all active and future readers.
                // Reclaiming it is only safe when this chain is the key's complete history
                // (`GcMode::Complete`): a layered chain must keep the tombstone, or a reader
                // falling through to an older source would resurrect the deleted value.
                if mode == GcMode::Complete {
                    let newest = self.versions.iter().map(|v| v.version).max().unwrap_or(0);
                    if newest <= watermark {
                        // Check if the newest is a tombstone.
                        if let Some(newest_v) = self.versions.iter().find(|v| v.version == newest)
                            && newest_v.value.is_none()
                            && !newest_v.is_intent
                        {
                            // Fully reclaimable tombstone
                            self.versions.retain(|v| v.version != newest);
                            reclaimed += 1;
                        }
                    }
                }
            }
        }

        reclaimed
    }
}

impl From<MvccValue> for nodus_storage_api::SnapshotValue {
    fn from(v: MvccValue) -> Self {
        Self {
            value: v.value,
            version: v.version,
            txn_id: v.txn_id,
            is_intent: v.is_intent,
        }
    }
}
impl From<nodus_storage_api::SnapshotValue> for MvccValue {
    fn from(v: nodus_storage_api::SnapshotValue) -> Self {
        Self {
            value: v.value,
            version: v.version,
            txn_id: v.txn_id,
            is_intent: v.is_intent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_visibility() {
        let mut chain = VersionChain::new();
        let txn1 = TxnId::new();

        // Write intent
        chain.write_intent(txn1, b"val1".to_vec()).unwrap();
        // Intents are not visible
        assert_eq!(chain.read(10), None);

        // Commit
        chain.commit(txn1, 10).unwrap();
        // Visible at or after commit_ts
        assert_eq!(chain.read(9), None);
        assert_eq!(chain.read(10), Some(b"val1".as_slice()));
        assert_eq!(chain.read(20), Some(b"val1".as_slice()));
    }

    #[test]
    fn long_chains_commit_in_place_and_collect_to_the_watermark() {
        let mut chain = VersionChain::new();
        for ts in 1..=1000u64 {
            let txn = TxnId::new();
            chain.write_intent(txn, ts.to_be_bytes().to_vec()).unwrap();
            chain.commit(txn, ts * 10).unwrap();
        }
        assert_eq!(chain.versions.len(), 1000);
        assert_eq!(chain.read(5005), Some(500u64.to_be_bytes().as_slice()));
        assert_eq!(chain.read(u64::MAX), Some(1000u64.to_be_bytes().as_slice()));
        // A pending intent survives collection; everything older than the
        // version visible at the watermark goes.
        let pending = TxnId::new();
        chain.write_intent(pending, b"pending".to_vec()).unwrap();
        assert_eq!(chain.garbage_collect(5005, GcMode::Complete), 499);
        assert_eq!(chain.read(5005), Some(500u64.to_be_bytes().as_slice()));
        assert_eq!(chain.read(4000), None);
        assert!(chain.active_intent().is_some());
        chain.commit(pending, 20_000).unwrap();
        assert_eq!(chain.read(u64::MAX), Some(b"pending".as_slice()));
    }

    #[test]
    fn read_visible_distinguishes_tombstone_from_absent() {
        let mut chain = VersionChain::new();
        let txn1 = TxnId::new();
        chain.write_intent(txn1, b"val1".to_vec()).unwrap();
        chain.commit(txn1, 10).unwrap();

        let txn2 = TxnId::new();
        chain.delete_intent(txn2).unwrap();
        chain.commit(txn2, 20).unwrap();

        // Before the delete: value visible. After it: a visible tombstone,
        // not mere absence. With no version at all: absent.
        assert_eq!(chain.read_visible(15), VisibleVersion::Value(b"val1"));
        assert_eq!(chain.read_visible(20), VisibleVersion::Tombstone);
        assert_eq!(chain.read_visible(u64::MAX), VisibleVersion::Tombstone);
        assert_eq!(chain.read_visible(5), VisibleVersion::Absent);
        assert_eq!(VersionChain::new().read_visible(100), VisibleVersion::Absent);
        // An uncommitted intent is never visible.
        let txn3 = TxnId::new();
        chain.write_intent(txn3, b"pending".to_vec()).unwrap();
        assert_eq!(chain.read_visible(u64::MAX), VisibleVersion::Tombstone);
    }

    #[test]
    fn test_write_conflict() {
        let mut chain = VersionChain::new();
        let txn1 = TxnId::new();
        let txn2 = TxnId::new();

        chain.write_intent(txn1, b"val1".to_vec()).unwrap();

        // Another txn tries to write
        let err = chain.write_intent(txn2, b"val2".to_vec()).unwrap_err();
        assert!(matches!(err, MvccError::WriteConflict { .. }));

        // Same txn overwrites its own intent
        chain.write_intent(txn1, b"val1_v2".to_vec()).unwrap();
        chain.commit(txn1, 10).unwrap();

        assert_eq!(chain.read(10), Some(b"val1_v2".as_slice()));
    }

    #[test]
    fn test_garbage_collect() {
        let mut chain = VersionChain::new();
        let txn1 = TxnId::new();
        let txn2 = TxnId::new();
        let txn3 = TxnId::new();

        chain.write_intent(txn1, b"val1".to_vec()).unwrap();
        chain.commit(txn1, 10).unwrap();

        chain.write_intent(txn2, b"val2".to_vec()).unwrap();
        chain.commit(txn2, 20).unwrap();

        chain.write_intent(txn3, b"val3".to_vec()).unwrap();
        chain.commit(txn3, 30).unwrap();

        assert_eq!(chain.versions.len(), 3);

        // GC at 15: keeps version 10 (visible at 15), 20, 30
        assert_eq!(chain.garbage_collect(15, GcMode::Complete), 0);
        assert_eq!(chain.versions.len(), 3);

        // GC at 25: keeps version 20 (visible at 25), drops 10. Keeps 30.
        assert_eq!(chain.garbage_collect(25, GcMode::Complete), 1);
        assert_eq!(chain.versions.len(), 2);
        assert_eq!(chain.read(25), Some(b"val2".as_slice()));

        // Write a tombstone
        let txn4 = TxnId::new();
        chain.delete_intent(txn4).unwrap();
        chain.commit(txn4, 40).unwrap();

        // GC at 45. The newest version visible at 45 is a tombstone (40).
        // It's the absolute newest version, so the key is dead to everyone.
        // `GcMode::Complete` (this chain is the key's whole history) drops
        // 20, 30, and the tombstone itself.
        let reclaimed = chain.garbage_collect(45, GcMode::Complete);
        assert_eq!(reclaimed, 3);
        assert!(chain.versions.is_empty());
    }

    #[test]
    fn gc_retains_the_tombstone_when_layered_over_older_sources() {
        // A memtable chain sits above SSTables that may still hold the key:
        // erasing the newest-visible tombstone would empty the chain and let
        // a reader falling through to the older source resurrect the deleted
        // value. `GcMode::Layered` keeps it; `GcMode::Complete` reclaims it.
        let mut chain = VersionChain::new();
        let put = TxnId::new();
        chain.write_intent(put, b"v".to_vec()).unwrap();
        chain.commit(put, 10).unwrap();
        let delete = TxnId::new();
        chain.delete_intent(delete).unwrap();
        chain.commit(delete, 20).unwrap();

        let mut layered = chain.clone();
        assert_eq!(layered.garbage_collect(20, GcMode::Layered), 1);
        assert_eq!(layered.read_visible(30), VisibleVersion::Tombstone);
        assert_eq!(layered.read_visible(5), VisibleVersion::Absent);

        let mut complete = chain;
        assert_eq!(complete.garbage_collect(20, GcMode::Complete), 2);
        assert!(complete.versions.is_empty());
    }
}
