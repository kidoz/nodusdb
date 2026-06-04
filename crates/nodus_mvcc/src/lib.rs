use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccError {
    WriteConflict {
        existing_txn: TxnId,
    },
    IntentNotFound {
        txn_id: TxnId,
    },
}

impl fmt::Display for MvccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MvccError::WriteConflict { existing_txn } => {
                write!(f, "Write-write conflict with transaction: {}", existing_txn.0)
            }
            MvccError::IntentNotFound { txn_id } => {
                write!(f, "Intent not found for transaction: {}", txn_id.0)
            }
        }
    }
}

impl std::error::Error for MvccError {}

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

    /// Read the most recent visible value for the given timestamp.
    pub fn read(&self, read_ts: Timestamp) -> Option<&[u8]> {
        self.versions
            .iter()
            .filter(|v| v.is_visible(read_ts))
            .max_by_key(|v| v.version)
            .and_then(|v| v.value.as_deref())
    }

    /// Finds an existing active intent. In standard MVCC, there is usually only
    /// one active intent per key at a time.
    pub fn active_intent(&self) -> Option<&MvccValue> {
        self.versions.iter().find(|v| v.is_intent)
    }

    /// Write an intent (either put or delete) for a given transaction.
    fn write_intent_impl(&mut self, txn_id: TxnId, value: Option<Vec<u8>>) -> Result<(), MvccError> {
        if let Some(intent) = self.active_intent() {
            if intent.txn_id != Some(txn_id) {
                return Err(MvccError::WriteConflict {
                    existing_txn: intent.txn_id.expect("intent must have txn_id"),
                });
            } else {
                // Same transaction is overwriting its own intent.
                // We'll clean up the old one first.
                self.versions.retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
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
        let intent_idx = self
            .versions
            .iter()
            .position(|v| v.is_intent && v.txn_id == Some(txn_id))
            .ok_or(MvccError::IntentNotFound { txn_id })?;

        let mut intent = self.versions.remove(intent_idx);
        intent.is_intent = false;
        intent.version = commit_ts;
        
        self.versions.push(intent);
        // Sort descending by version so the newest is first if needed, though we use max_by_key in read().
        // Sorting helps keep the chain ordered for efficient gc or scans.
        self.versions.sort_by_key(|b| std::cmp::Reverse(b.version));
        
        Ok(())
    }

    pub fn abort(&mut self, txn_id: TxnId) -> Result<(), MvccError> {
        let initial_len = self.versions.len();
        self.versions.retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
        
        if self.versions.len() == initial_len {
            return Err(MvccError::IntentNotFound { txn_id });
        }
        
        Ok(())
    }

    /// Reclaims MVCC versions strictly older than the `watermark`.
    /// Leaves at least one version strictly older than watermark so that
    /// reads at watermark still see the correct value.
    pub fn garbage_collect(&mut self, watermark: Timestamp) -> usize {
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
                let to_drop: Vec<_> = committed[keep_idx + 1..].iter().map(|v| v.version).collect();
                reclaimed = to_drop.len();
                self.versions.retain(|v| v.is_intent || !to_drop.contains(&v.version));
                
                // If the retained element is a tombstone and strictly older than watermark,
                // and there are no newer versions, we might be able to drop it entirely,
                // but for simplicity standard GC keeps the tombstone or drops it if no one can see it.
                // If the visible element AT watermark is a tombstone, and there are NO versions newer
                // than watermark, the key is logically fully deleted for all active and future readers.
                let newest = self.versions.iter().map(|v| v.version).max().unwrap_or(0);
                if newest <= watermark {
                    // Check if the newest is a tombstone.
                    if let Some(newest_v) = self.versions.iter().find(|v| v.version == newest)
                        && newest_v.value.is_none() && !newest_v.is_intent {
                            // Fully reclaimable tombstone
                            self.versions.retain(|v| v.version != newest);
                            reclaimed += 1;
                        }
                }
            }
        }
        
        reclaimed
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
        assert_eq!(chain.garbage_collect(15), 0);
        assert_eq!(chain.versions.len(), 3);
        
        // GC at 25: keeps version 20 (visible at 25), drops 10. Keeps 30.
        assert_eq!(chain.garbage_collect(25), 1);
        assert_eq!(chain.versions.len(), 2);
        assert_eq!(chain.read(25), Some(b"val2".as_slice()));
        
        // Write a tombstone
        let txn4 = TxnId::new();
        chain.delete_intent(txn4).unwrap();
        chain.commit(txn4, 40).unwrap();
        
        // GC at 45. The newest version visible at 45 is a tombstone (40).
        // It's the absolute newest version, so the key is dead to everyone.
        // It drops 20, 30, and the tombstone itself!
        let reclaimed = chain.garbage_collect(45);
        assert_eq!(reclaimed, 3);
        assert!(chain.versions.is_empty());
    }
}
