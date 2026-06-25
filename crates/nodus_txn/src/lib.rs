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

/// A hybrid logical clock: wall-clock-driven but strictly monotonic, and
/// mergeable with timestamps observed from other nodes so that timestamps stay
/// comparable cluster-wide (bounded by clock skew).
///
/// Timestamps are microseconds since the Unix epoch, with the HLC's logical
/// component expressed as extra microseconds: each event advances the clock by
/// at least one µs, so two events in the same wall-clock microsecond still get
/// distinct, increasing timestamps. Keeping the µs scale preserves the meaning
/// of `commit_ts` for PITR/WAL replay while guaranteeing monotonicity.
#[derive(Debug, Clone, Copy)]
pub struct HybridLogicalClock {
    /// The most recently issued timestamp.
    last: Timestamp,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self { last: 0 }
    }

    fn wall_now() -> Timestamp {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Reads the current clock without advancing it: the larger of wall-clock
    /// now and the last issued timestamp. Used as the GC watermark when no
    /// transactions are in flight.
    pub fn now(&self) -> Timestamp {
        Self::wall_now().max(self.last)
    }

    /// Issues the next timestamp for a local event. Strictly greater than every
    /// timestamp this clock has issued, and ≥ wall-clock now.
    pub fn tick(&mut self) -> Timestamp {
        let next = Self::wall_now().max(self.last + 1);
        self.last = next;
        next
    }

    /// Merges a timestamp observed from another node (an HLC "receive" event):
    /// issues a fresh timestamp strictly greater than both the local last and
    /// the remote timestamp. Afterwards, locally issued timestamps order after
    /// the observed remote event.
    pub fn update(&mut self, remote: Timestamp) -> Timestamp {
        let next = Self::wall_now().max(self.last + 1).max(remote + 1);
        self.last = next;
        next
    }
}

/// Durably persists the clock's high-water mark so issued commit timestamps
/// never regress across a restart. Without this, the in-memory clock restarts
/// from `0`/wall-clock, and a clock that is behind the newest persisted
/// `commit_ts` (skew, fast restart, or a peer-advanced timestamp) would issue
/// new commits *below* existing committed MVCC versions — silently shadowing
/// them. Implemented by the server over its durable local engine.
pub trait TimestampStore: Send + Sync {
    /// Loads the last persisted reservation high-water mark, if any.
    fn load(&self) -> Result<Option<Timestamp>>;
    /// Durably stores a new reservation high-water mark.
    fn store(&self, watermark: Timestamp) -> Result<()>;
}

/// How far ahead of the latest issued timestamp each durable reservation
/// reaches (microseconds). This bounds persistence to roughly one durable write
/// per second of clock advancement (or per `RESERVATION_WINDOW` timestamps),
/// instead of an fsync per commit; on restart the clock resumes at least this
/// far past the last *issued* timestamp, never below it.
const RESERVATION_WINDOW: Timestamp = 1_000_000;

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

    /// Merges a timestamp observed from another node into the local clock so
    /// future timestamps order strictly after it (HLC receive). This is what
    /// keeps `read_ts`/`commit_ts` comparable across nodes once they exchange
    /// timestamps; cross-shard transactions (2PC) rely on it. Default: no-op.
    fn observe_timestamp(&self, _ts: Timestamp) {}

    /// Like [`Self::observe_timestamp`], but also extends the durable reservation
    /// so the observed timestamp survives a restart. Applied when a node ingests
    /// a replicated commit (e.g. a follower applying `CommitTxn`): once a node
    /// has durably stored a version at `ts`, it must never — even after a restart
    /// and leadership change — issue a commit timestamp at or below `ts`.
    /// Default: in-memory only (no persistence).
    fn observe_durable(&self, ts: Timestamp) -> Result<()> {
        self.observe_timestamp(ts);
        Ok(())
    }
}

// In-Memory MVP Implementation
use crate::sync::RwLock;
use std::collections::HashMap;

/// The clock plus the highest timestamp it has durably reserved. Issued
/// timestamps never exceed `reserved` without a preceding [`TimestampStore::store`].
struct ClockState {
    hlc: HybridLogicalClock,
    reserved: Timestamp,
}

pub struct MemTxnManager {
    records: RwLock<HashMap<TxnId, TxnRecord>>,
    clock: RwLock<ClockState>,
    persistence: Option<std::sync::Arc<dyn TimestampStore>>,
}

impl MemTxnManager {
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            clock: RwLock::new(ClockState {
                hlc: HybridLogicalClock::new(),
                reserved: 0,
            }),
            persistence: None,
        }
    }

    /// Builds a manager whose clock is seeded from `store` and that durably
    /// reserves timestamp space ahead of issuance. The clock resumes at the
    /// larger of the persisted reservation and wall-clock now, so commit
    /// timestamps issued after a restart strictly exceed every commit timestamp
    /// issued before it.
    pub fn with_timestamp_store(store: std::sync::Arc<dyn TimestampStore>) -> Result<Self> {
        let persisted = store.load()?.unwrap_or(0);
        let last = persisted.max(HybridLogicalClock::wall_now());
        Ok(Self {
            records: RwLock::new(HashMap::new()),
            clock: RwLock::new(ClockState {
                hlc: HybridLogicalClock { last },
                reserved: persisted,
            }),
            persistence: Some(store),
        })
    }

    /// Guarantees `ts` is covered by a durable reservation before it is handed
    /// out as a commit timestamp. Persists a new window only when crossing the
    /// current reservation, so the durable write is amortized.
    fn reserve_through(&self, clock: &mut ClockState, ts: Timestamp) -> Result<()> {
        if ts > clock.reserved {
            let new_reserved = ts.saturating_add(RESERVATION_WINDOW);
            if let Some(store) = &self.persistence {
                store.store(new_reserved)?;
            }
            clock.reserved = new_reserved;
        }
        Ok(())
    }
}

impl Default for MemTxnManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnManager for MemTxnManager {
    fn begin_txn(&self) -> Result<TxnRecord> {
        // A read timestamp is a snapshot, never written as a durable version, so
        // it needs no reservation — only commit timestamps must survive restart.
        let read_ts = {
            let mut clock = self.clock.write().unwrap();
            clock.hlc.tick()
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

        // Issue the commit timestamp and ensure it is durably reserved before it
        // is returned (and thus before any version is written at it). If the
        // reservation cannot be persisted, the commit fails rather than risk a
        // timestamp that could regress after a crash.
        let commit_ts = {
            let mut clock = self.clock.write().unwrap();
            let ts = clock.hlc.tick();
            self.reserve_through(&mut clock, ts)?;
            ts
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
        oldest_active.unwrap_or_else(|| self.clock.read().unwrap().hlc.now())
    }

    fn observe_timestamp(&self, ts: Timestamp) {
        // Advancing the clock here needs no reservation: nothing durable is
        // written at this timestamp, and the next commit that issues a value
        // past it will reserve before returning.
        self.clock.write().unwrap().hlc.update(ts);
    }

    fn observe_durable(&self, ts: Timestamp) -> Result<()> {
        // A version was durably applied at `ts`, so the clock must resume past it
        // after a restart: advance and extend the reservation to cover `ts`.
        let mut clock = self.clock.write().unwrap();
        clock.hlc.update(ts);
        self.reserve_through(&mut clock, ts)
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

    #[test]
    fn clock_ticks_are_strictly_monotonic_within_a_microsecond() {
        // A tight burst issues many timestamps within the same wall-clock µs;
        // each must still be strictly greater than the last.
        let mut hlc = HybridLogicalClock::new();
        let mut prev = hlc.tick();
        for _ in 0..100_000 {
            let next = hlc.tick();
            assert!(
                next > prev,
                "clock went backwards or stalled: {next} <= {prev}"
            );
            prev = next;
        }
    }

    #[test]
    fn update_advances_strictly_past_a_remote_timestamp() {
        let mut hlc = HybridLogicalClock::new();
        let local = hlc.tick();
        let remote = local + 5_000_000_000; // far ahead of local wall clock
        let merged = hlc.update(remote);
        assert!(merged > remote, "merge must order after the remote event");
        assert!(
            hlc.tick() > merged,
            "subsequent ticks stay ahead of the merge"
        );
    }

    /// An in-memory [`TimestampStore`] standing in for the durable engine.
    #[derive(Clone, Default)]
    struct MockStore(std::sync::Arc<std::sync::Mutex<Option<Timestamp>>>);

    impl TimestampStore for MockStore {
        fn load(&self) -> Result<Option<Timestamp>> {
            Ok(*self.0.lock().unwrap())
        }
        fn store(&self, watermark: Timestamp) -> Result<()> {
            *self.0.lock().unwrap() = Some(watermark);
            Ok(())
        }
    }

    #[test]
    fn commit_timestamps_do_not_regress_across_restart() {
        let store = MockStore::default();

        // First "process": advance the clock far past wall time (as a peer HLC
        // would), commit, and capture the issued timestamp.
        let big_commit_ts = {
            let mgr =
                MemTxnManager::with_timestamp_store(std::sync::Arc::new(store.clone())).unwrap();
            let t = mgr.begin_txn().unwrap();
            let remote = t.read_ts + 10_000_000_000; // ~2.7 hours ahead of wall
            mgr.observe_timestamp(remote);
            let t2 = mgr.begin_txn().unwrap();
            let ts = mgr.commit_txn(t2.txn_id).unwrap();
            assert!(
                ts > remote,
                "commit must order after the observed remote ts"
            );
            // The issued commit ts must be durably covered by a reservation.
            assert!(store.0.lock().unwrap().unwrap() >= ts);
            ts
        };

        // "Restart": a fresh manager seeded from the same durable store. Without
        // persistence it would restart near wall-clock now — far below the
        // peer-advanced timestamp — and issue a regressing commit ts.
        let mgr = MemTxnManager::with_timestamp_store(std::sync::Arc::new(store.clone())).unwrap();
        let t = mgr.begin_txn().unwrap();
        let resumed = mgr.commit_txn(t.txn_id).unwrap();
        assert!(
            resumed > big_commit_ts,
            "commit timestamp regressed after restart: {resumed} <= {big_commit_ts}"
        );
    }

    #[test]
    fn durably_observed_timestamps_survive_restart() {
        let store = MockStore::default();

        // A follower applies a replicated commit far ahead of its wall clock.
        let applied_ts = {
            let mgr =
                MemTxnManager::with_timestamp_store(std::sync::Arc::new(store.clone())).unwrap();
            let base = mgr.begin_txn().unwrap().read_ts;
            let applied = base + 10_000_000_000; // ~2.7 hours ahead
            mgr.observe_durable(applied).unwrap();
            applied
        };
        // The reservation must cover the durably-applied timestamp.
        assert!(store.0.lock().unwrap().unwrap() >= applied_ts);

        // After "restart" and promotion to coordinator, the first issued commit
        // must order strictly after the timestamp it already applied.
        let mgr = MemTxnManager::with_timestamp_store(std::sync::Arc::new(store.clone())).unwrap();
        let t = mgr.begin_txn().unwrap();
        let commit = mgr.commit_txn(t.txn_id).unwrap();
        assert!(
            commit > applied_ts,
            "issued {commit} <= durably applied {applied_ts}"
        );
    }

    #[test]
    fn observed_timestamps_order_future_transactions_after_them() {
        let manager = MemTxnManager::new();
        let first = manager.begin_txn().unwrap();
        let remote = first.read_ts + 5_000_000_000;
        manager.observe_timestamp(remote);

        let next = manager.begin_txn().unwrap();
        assert!(
            next.read_ts > remote,
            "a read_ts issued after observing a remote ts must order after it"
        );
        let commit_ts = manager.commit_txn(next.txn_id).unwrap();
        assert!(commit_ts > next.read_ts);
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
