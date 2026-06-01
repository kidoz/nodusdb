use anyhow::Result;
use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnState {
    Pending,
    Writing,
    Prepared,
    Committed,
    Aborted,
    Resolving,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnRecord {
    pub txn_id: TxnId,
    pub state: TxnState,
    pub read_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
}

#[derive(Debug, Clone, Copy)]
pub struct HybridLogicalClock {
    pub logical_time: u64,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self { logical_time: 0 }
    }

    pub fn now(&self) -> Timestamp {
        // Simplified for MVP, returning logical time
        self.logical_time
    }

    pub fn tick(&mut self) -> Timestamp {
        self.logical_time += 1;
        self.logical_time
    }
}

pub trait TxnManager: Send + Sync {
    fn begin_txn(&self) -> Result<TxnRecord>;
    fn commit_txn(&self, txn_id: TxnId) -> Result<Timestamp>;
    fn abort_txn(&self, txn_id: TxnId) -> Result<()>;

    /// The highest timestamp at or below which MVCC versions can be safely
    /// reclaimed: the oldest in-flight read timestamp, or the current clock when
    /// no transactions are active. Default is `0` (reclaim nothing).
    fn gc_watermark(&self) -> Timestamp {
        0
    }
}

// In-Memory MVP Implementation
use std::collections::HashMap;
use std::sync::RwLock;

pub struct MemTxnManager {
    records: RwLock<HashMap<TxnId, TxnRecord>>,
    hlc: RwLock<HybridLogicalClock>,
}

impl MemTxnManager {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            hlc: RwLock::new(HybridLogicalClock::new()),
        }
    }
}

impl Default for MemTxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnManager for MemTxnManager {
    fn begin_txn(&self) -> Result<TxnRecord> {
        let read_ts = {
            let mut hlc = self.hlc.write().unwrap();
            hlc.tick()
        };

        let txn_id = TxnId::new();
        let record = TxnRecord {
            txn_id,
            state: TxnState::Pending,
            read_ts,
            commit_ts: None,
        };

        let mut guard = self.records.write().unwrap();
        guard.insert(txn_id, record.clone());
        Ok(record)
    }

    fn commit_txn(&self, txn_id: TxnId) -> Result<Timestamp> {
        let commit_ts = {
            let mut hlc = self.hlc.write().unwrap();
            hlc.tick()
        };

        let mut guard = self.records.write().unwrap();
        if let Some(record) = guard.get_mut(&txn_id) {
            if record.state != TxnState::Pending && record.state != TxnState::Writing {
                anyhow::bail!("Cannot commit transaction in state {:?}", record.state);
            }
            record.state = TxnState::Committed;
            record.commit_ts = Some(commit_ts);
            Ok(commit_ts)
        } else {
            anyhow::bail!("Transaction {} not found", txn_id.0);
        }
    }

    fn abort_txn(&self, txn_id: TxnId) -> Result<()> {
        let mut guard = self.records.write().unwrap();
        if let Some(record) = guard.get_mut(&txn_id) {
            if record.state == TxnState::Committed {
                anyhow::bail!("Cannot abort already committed transaction");
            }
            record.state = TxnState::Aborted;
            Ok(())
        } else {
            anyhow::bail!("Transaction {} not found", txn_id.0);
        }
    }

    fn gc_watermark(&self) -> Timestamp {
        let records = self.records.read().unwrap();
        let oldest_active = records
            .values()
            .filter(|r| {
                matches!(
                    r.state,
                    TxnState::Pending | TxnState::Writing | TxnState::Prepared
                )
            })
            .map(|r| r.read_ts)
            .min();
        // Keep everything an active reader could still see; with no active txns,
        // the current clock lets GC reclaim all superseded versions.
        oldest_active.unwrap_or_else(|| self.hlc.read().unwrap().now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_lifecycle() {
        let manager = MemTxnManager::new();
        let txn = manager.begin_txn().unwrap();
        assert_eq!(txn.state, TxnState::Pending);

        let commit_ts = manager.commit_txn(txn.txn_id).unwrap();
        assert!(commit_ts > txn.read_ts);

        // Aborting already committed should fail
        assert!(manager.abort_txn(txn.txn_id).is_err());
    }
}
