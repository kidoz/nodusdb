use anyhow::Result;
use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};

/// Concurrency primitives for the transaction manager.
///
/// Under `--cfg loom` these resolve to loom's model-checked shims, letting the
/// concurrency-control locking in [`MemTxnManager`] be exhaustively verified by
/// the loom tests at the bottom of this file. In every normal build they are the
/// `std` primitives used in production, so the loom proof covers the real code
/// path rather than a separate model.
pub(crate) mod sync {
    #[cfg(loom)]
    pub(crate) use loom::sync::{Arc, RwLock};
    #[cfg(not(loom))]
    #[allow(unused_imports)] // `Arc` is only used by the loom tests.
    pub(crate) use std::sync::{Arc, RwLock};
}

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
    /// Keys written by this transaction.
    pub write_set: Vec<Vec<u8>>,
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
    /// Tracks a write intent to enable OCC (Optimistic Concurrency Control) conflict detection.
    fn track_write(&self, txn_id: TxnId, key: Vec<u8>) -> Result<()>;
    /// Commits the transaction if no concurrent writes conflict.
    /// NodusDB uses Snapshot Isolation (OCC). Concurrent writes to the same key
    /// by a transaction that committed after our read_ts will abort this transaction.
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
use crate::sync::RwLock;
use std::collections::HashMap;

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
            write_set: Vec::new(),
        };

        let mut guard = self.records.write().unwrap();
        guard.insert(txn_id, record.clone());
        Ok(record)
    }

    fn track_write(&self, txn_id: TxnId, key: Vec<u8>) -> Result<()> {
        let mut guard = self.records.write().unwrap();
        if let Some(record) = guard.get_mut(&txn_id) {
            record.write_set.push(key);
            Ok(())
        } else {
            anyhow::bail!("Transaction {} not found", txn_id.0);
        }
    }

    fn commit_txn(&self, txn_id: TxnId) -> Result<Timestamp> {
        let mut guard = self.records.write().unwrap();

        let record = guard
            .get(&txn_id)
            .ok_or_else(|| anyhow::anyhow!("Transaction {} not found", txn_id.0))?;
        if record.state != TxnState::Pending && record.state != TxnState::Writing {
            anyhow::bail!("Cannot commit transaction in state {:?}", record.state);
        }
        let read_ts = record.read_ts;
        let write_set = record.write_set.clone();

        // Snapshot Isolation (OCC) Conflict Check:
        // Has any transaction committed since `read_ts` written to keys in our `write_set`?
        for other in guard.values() {
            if other.txn_id == txn_id || other.state != TxnState::Committed {
                continue;
            }
            if let Some(other_commit_ts) = other.commit_ts
                && other_commit_ts > read_ts
            {
                // Check intersection
                for key in &write_set {
                    if other.write_set.contains(key) {
                        // Conflict detected
                        guard.get_mut(&txn_id).unwrap().state = TxnState::Aborted;
                        anyhow::bail!("Write-write conflict detected on key. Transaction aborted.");
                    }
                }
            }
        }

        let commit_ts = {
            let mut hlc = self.hlc.write().unwrap();
            hlc.tick()
        };

        let record_mut = guard.get_mut(&txn_id).unwrap();
        record_mut.state = TxnState::Committed;
        record_mut.commit_ts = Some(commit_ts);
        Ok(commit_ts)
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

// Regular unit tests use std locks and must not run under loom, where lock
// operations are only valid inside a `loom::model` closure.
#[cfg(all(test, not(loom)))]
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

// Loom model-checked concurrency tests.
//
// These run under `--cfg loom` (see `just test-loom`) and exhaustively explore
// thread interleavings of `MemTxnManager`'s commit path to prove its
// snapshot-isolation / OCC invariants hold for the real production locking, not
// a hand-written model. The transactions are created sequentially on the main
// thread; only the conflicting work — `commit_txn` — runs concurrently, keeping
// the state space tractable.
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::sync::Arc;
    use loom::thread;

    /// Two transactions whose read snapshots overlap and that write the same
    /// key must not both commit: at most one may win, or a lost update would
    /// silently occur. Because the first committer's timestamp is always issued
    /// after both read timestamps, the loser is guaranteed to observe the
    /// conflict, so in fact *exactly* one wins under every interleaving.
    #[test]
    fn loom_conflicting_commits_exactly_one_wins() {
        loom::model(|| {
            let mgr = Arc::new(MemTxnManager::new());

            let t1 = mgr.begin_txn().unwrap();
            let t2 = mgr.begin_txn().unwrap();
            mgr.track_write(t1.txn_id, b"k".to_vec()).unwrap();
            mgr.track_write(t2.txn_id, b"k".to_vec()).unwrap();

            let (id1, id2) = (t1.txn_id, t2.txn_id);
            let m1 = Arc::clone(&mgr);
            let h = thread::spawn(move || m1.commit_txn(id1).is_ok());
            let ok2 = mgr.commit_txn(id2).is_ok();
            let ok1 = h.join().unwrap();

            assert!(
                ok1 ^ ok2,
                "write-write conflict invariant violated: ok1={ok1}, ok2={ok2}"
            );
        });
    }

    /// Transactions that touch disjoint keys never conflict, so both must commit
    /// regardless of how their commit critical sections interleave.
    #[test]
    fn loom_disjoint_commits_both_win() {
        loom::model(|| {
            let mgr = Arc::new(MemTxnManager::new());

            let t1 = mgr.begin_txn().unwrap();
            let t2 = mgr.begin_txn().unwrap();
            mgr.track_write(t1.txn_id, b"k1".to_vec()).unwrap();
            mgr.track_write(t2.txn_id, b"k2".to_vec()).unwrap();

            let (id1, id2) = (t1.txn_id, t2.txn_id);
            let m1 = Arc::clone(&mgr);
            let h = thread::spawn(move || m1.commit_txn(id1).is_ok());
            let ok2 = mgr.commit_txn(id2).is_ok();
            let ok1 = h.join().unwrap();

            assert!(ok1 && ok2, "disjoint commits must both succeed");
        });
    }

    /// The hybrid logical clock is ticked while holding the records write lock,
    /// so two commits that race must still be assigned distinct, monotonic
    /// commit timestamps — no two committed transactions may share a version.
    #[test]
    fn loom_concurrent_commits_get_distinct_timestamps() {
        loom::model(|| {
            let mgr = Arc::new(MemTxnManager::new());

            let t1 = mgr.begin_txn().unwrap();
            let t2 = mgr.begin_txn().unwrap();
            mgr.track_write(t1.txn_id, b"k1".to_vec()).unwrap();
            mgr.track_write(t2.txn_id, b"k2".to_vec()).unwrap();

            let (id1, id2) = (t1.txn_id, t2.txn_id);
            let m1 = Arc::clone(&mgr);
            let h = thread::spawn(move || m1.commit_txn(id1).unwrap());
            let ts2 = mgr.commit_txn(id2).unwrap();
            let ts1 = h.join().unwrap();

            assert_ne!(ts1, ts2, "concurrent commits shared a commit timestamp");
        });
    }
}
