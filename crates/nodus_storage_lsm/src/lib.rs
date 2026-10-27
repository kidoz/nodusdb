use anyhow::Result;
use bytes::Bytes;
#[cfg(test)]
mod powercut_tests;
mod snapshot;
pub mod sstable;

use nodus_mvcc::{GcMode, VersionChain, VisibleVersion};
use nodus_storage_api::{
    IntentOperation, IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, KvVersion,
    PendingIntents, Timestamp, TxnId, wallclock_micros,
};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use serde::{Deserialize, Serialize};
use sstable::Sstable;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

/// Observable storage events, emitted without a storage-to-monitoring dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsmTelemetryEvent {
    /// Approximate live memtable bytes after a write, flush, or observer attachment.
    MemtableBytes { bytes: usize },
    /// Number of SSTables in the engine's current in-memory published set.
    Sstables { count: usize },
    /// A non-empty flush completed, including any compaction it triggered.
    Flush { elapsed: Duration, success: bool },
    /// A compaction with at least two input SSTables completed.
    Compaction { elapsed: Duration, success: bool },
}

type LsmTelemetryObserver = Arc<dyn Fn(LsmTelemetryEvent) + Send + Sync>;

/// Flush the memtable once its (approximate) live size reaches this many bytes.
const DEFAULT_FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;
/// Compact once this many SSTables accumulate, to bound read amplification.
const COMPACTION_TRIGGER: usize = 4;
const MANIFEST_FILE: &str = "MANIFEST";

/// The durable record of which files make up the store: the live SSTables and
/// the active WAL segment. Recovery reads this instead of guessing from file
/// names, which decouples SSTable ids from WAL ids and lets compaction rewrite
/// the SSTable set atomically (swap the manifest, then delete the old files).
#[derive(Debug, Default, Serialize, Deserialize)]
struct Manifest {
    active_wal: u64,
    sstables: Vec<u64>,
    /// Oldest WAL segment id that crash recovery must replay. Recovery walks the
    /// `SegmentHeader.predecessor` lineage back from `active_wal` down to (and
    /// including) this floor. The floor only advances past a flush that retained
    /// no uncommitted intents and saw no intent WAL append in flight —
    /// otherwise an intent whose records were rotated into an older segment
    /// (with its commit landing in a newer one) would be dropped, losing a
    /// committed transaction. `#[serde(default)]` keeps legacy manifests
    /// (floor 0 ⇒ replay the full available lineage) readable.
    #[serde(default)]
    min_replay_wal: u64,
}

pub struct LsmKvEngine {
    snapshot_gate: RwLock<()>,
    snapshot_failed: std::sync::atomic::AtomicBool,
    data_dir: Option<PathBuf>,
    memtable: RwLock<BTreeMap<Bytes, VersionChain>>,
    /// Transaction id → its outstanding intent keys, each stamped with its
    /// write time (HLC-scale wall-clock µs, recorded in the WAL) so an
    /// intent's age survives crash replay for the abandoned-intent janitor.
    intents: RwLock<HashMap<TxnId, Vec<(Bytes, Timestamp)>>>,
    sstables: RwLock<Vec<Sstable>>,
    wal: RwLock<Option<Arc<dyn WalEngine>>>,
    /// Id of the active WAL segment (`{id}.log`), recorded in the manifest.
    active_wal_id: AtomicU64,
    /// Oldest WAL segment recovery must replay (see [`Manifest::min_replay_wal`]).
    min_replay_wal: AtomicU64,
    next_file_id: AtomicU64,
    /// Approximate live size of the memtable, driving size-triggered flushes.
    memtable_bytes: AtomicUsize,
    flush_threshold: AtomicUsize,
    /// Most recent GC watermark, applied to SSTables during compaction.
    gc_watermark: AtomicU64,
    /// Serializes flush and compaction (both rewrite the SSTable set + manifest).
    flush_compact_lock: Mutex<()>,
    /// Serializes intent validation and publication without holding memtable or
    /// intent locks across WAL appends or syncs.
    intent_mutation_lock: Mutex<()>,
    /// Intent WAL appends (write/delete) that have started but not yet
    /// registered their intent in the memtable. Flush must not advance the
    /// replay floor while any are in flight: such an append may already sit in
    /// the segment being rotated away from while its registration (and later
    /// `CommitTxn`) is still pending — advancing the floor would exclude the
    /// `WriteIntent` from crash replay and its commit would be dropped.
    inflight_appends: AtomicUsize,
    /// Test-only pause point between an intent's WAL append and its memtable
    /// registration, for deterministic coverage of the flush race window.
    #[cfg(test)]
    append_pause: AppendPause,
    /// Test-only pause point inside the WAL rotation, after the segment swap
    /// and before the manifest publication, for deterministic coverage of the
    /// swap→publish window.
    #[cfg(test)]
    flush_pause: AppendPause,
    _wal_key: Option<[u8; 32]>,
    telemetry_observer: OnceLock<LsmTelemetryObserver>,
}

/// Decrements [`LsmKvEngine::inflight_appends`] on drop, so every exit from an
/// intent write — success or error — balances the increment.
struct InflightAppendGuard<'a> {
    counter: &'a AtomicUsize,
}

impl Drop for InflightAppendGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Test-only gate that parks a writer thread after its intent's WAL append,
/// before memtable registration, until the test releases it.
#[cfg(test)]
#[derive(Default)]
struct AppendPause {
    state: Mutex<AppendPauseState>,
    parked: std::sync::Condvar,
    release: std::sync::Condvar,
}

#[cfg(test)]
#[derive(Default)]
struct AppendPauseState {
    armed: bool,
    parked: usize,
    generation: usize,
}

/// Approximate in-memory size of a key's version chain, for the flush threshold.
fn chain_bytes(key: &[u8], chain: &VersionChain) -> usize {
    key.len()
        + chain
            .versions
            .iter()
            .map(|v| v.value.as_ref().map_or(0, |x| x.len()) + 24)
            .sum::<usize>()
}

/// One key-ordered source (an SSTable's ranged iterator or the memtable
/// snapshot) feeding the lazy merge, with one item buffered for peeking.
struct ScanSource {
    iter: Box<dyn Iterator<Item = Result<(Bytes, VersionChain)>> + Send>,
    front: Option<(Bytes, VersionChain)>,
    done: bool,
}

impl ScanSource {
    fn new(iter: Box<dyn Iterator<Item = Result<(Bytes, VersionChain)>> + Send>) -> Self {
        Self {
            iter,
            front: None,
            done: false,
        }
    }

    /// Ensures `front` holds the next item, pulling one if needed. A read error
    /// is surfaced to the caller and leaves the source exhausted.
    fn fill(&mut self) -> Result<()> {
        if self.front.is_none() && !self.done {
            match self.iter.next() {
                Some(Ok(item)) => self.front = Some(item),
                Some(Err(e)) => {
                    self.done = true;
                    return Err(e);
                }
                None => self.done = true,
            }
        }
        Ok(())
    }
}

/// A lazy k-way merge over key-ordered [`ScanSource`]s. It yields each key once,
/// in ascending order, resolving the MVCC version visible at `read_ts` from the
/// highest-precedence source holding that key (sources are ordered ascending in
/// precedence, so a later source wins). Peak memory is the number of sources
/// plus one buffered entry each — independent of the range's size — instead of
/// materializing the whole range up front.
struct LazyMergeScan {
    sources: Vec<ScanSource>,
    read_ts: Timestamp,
    errored: bool,
}

impl Iterator for LazyMergeScan {
    type Item = Result<KvPair>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }
        loop {
            for source in &mut self.sources {
                if let Err(e) = source.fill() {
                    self.errored = true;
                    return Some(Err(e));
                }
            }

            let min_key = self
                .sources
                .iter()
                .filter_map(|s| s.front.as_ref().map(|(k, _)| k.clone()))
                .min();
            let min_key = min_key?;

            // Merge the version chains of EVERY source holding this key. A key's
            // history can be split across the memtable and several SSTables (each
            // flush captures only the versions live at that moment), so the
            // version visible at `read_ts` may sit in a lower-precedence source.
            // Reading only the highest-precedence chain (as before) would skip
            // such a key entirely — silently omitting a row that `get` returns,
            // because `get` falls through sources. `read`/visibility are
            // order-independent (max visible version wins, intents excluded), so
            // concatenating versions is correct, mirroring `scan_versions`.
            let mut merged = VersionChain::default();
            for source in &mut self.sources {
                if source.front.as_ref().is_some_and(|(k, _)| *k == min_key) {
                    let (_, chain) = source.front.take().unwrap();
                    merged.versions.extend(chain.versions);
                }
            }

            if let Some(val) = merged.read(self.read_ts) {
                let version = merged
                    .versions
                    .iter()
                    .filter(|v| v.is_visible(self.read_ts))
                    .map(|v| v.version)
                    .max()
                    .unwrap_or(0);
                return Some(Ok(KvPair {
                    key: min_key,
                    value: Bytes::from(val.to_vec()),
                    version,
                }));
            }
            // No version visible at read_ts (tombstone or newer write): skip this
            // key and continue with the next.
        }
    }
}

impl LsmKvEngine {
    pub fn new() -> Self {
        Self {
            snapshot_gate: RwLock::new(()),
            snapshot_failed: std::sync::atomic::AtomicBool::new(false),
            data_dir: None,
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(Vec::new()),
            wal: RwLock::new(None),
            active_wal_id: AtomicU64::new(0),
            min_replay_wal: AtomicU64::new(0),
            next_file_id: AtomicU64::new(1),
            memtable_bytes: AtomicUsize::new(0),
            flush_threshold: AtomicUsize::new(DEFAULT_FLUSH_THRESHOLD),
            gc_watermark: AtomicU64::new(0),
            flush_compact_lock: Mutex::new(()),
            intent_mutation_lock: Mutex::new(()),
            inflight_appends: AtomicUsize::new(0),
            #[cfg(test)]
            append_pause: AppendPause::default(),
            #[cfg(test)]
            flush_pause: AppendPause::default(),
            _wal_key: None,
            telemetry_observer: OnceLock::new(),
        }
    }

    /// Overrides the memtable flush threshold (bytes). Mainly for tests.
    pub fn set_flush_threshold(&self, bytes: usize) {
        self.flush_threshold.store(bytes, Ordering::Relaxed);
    }

    /// Installs the single telemetry observer and emits the current resource snapshot.
    pub fn set_telemetry_observer(
        &self,
        observer: impl Fn(LsmTelemetryEvent) + Send + Sync + 'static,
    ) -> Result<()> {
        let observer: LsmTelemetryObserver = Arc::new(observer);
        self.telemetry_observer
            .set(observer.clone())
            .map_err(|_| anyhow::anyhow!("LSM telemetry observer is already installed"))?;
        observer(LsmTelemetryEvent::MemtableBytes {
            bytes: self.memtable_bytes.load(Ordering::Relaxed),
        });
        observer(LsmTelemetryEvent::Sstables {
            count: self.sstables.read().unwrap().len(),
        });
        Ok(())
    }

    fn emit_telemetry(&self, event: LsmTelemetryEvent) {
        if let Some(observer) = self.telemetry_observer.get() {
            observer(event);
        }
    }

    fn set_memtable_bytes(&self, bytes: usize) {
        self.memtable_bytes.store(bytes, Ordering::Relaxed);
        self.emit_telemetry(LsmTelemetryEvent::MemtableBytes { bytes });
    }

    fn add_memtable_bytes(&self, added: usize) {
        let previous = self.memtable_bytes.fetch_add(added, Ordering::Relaxed);
        self.set_memtable_bytes(previous.wrapping_add(added));
    }

    fn emit_sstable_count(&self, count: usize) {
        self.emit_telemetry(LsmTelemetryEvent::Sstables { count });
    }

    pub fn with_wal<P: AsRef<Path>>(data_dir: P, key: Option<[u8; 32]>) -> Result<Self> {
        let path = data_dir.as_ref();
        std::fs::create_dir_all(path)?;

        // Prefer the manifest (authoritative file set). Fall back to a directory
        // scan when it is absent (pre-manifest / fresh dirs) OR unreadable/corrupt
        // — a torn manifest must not make the engine unstartable.
        let manifest: Option<Manifest> = std::fs::read(path.join(MANIFEST_FILE))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        let (mut sst_ids, active_wal_id, min_replay_wal): (Vec<u64>, u64, u64) = match manifest {
            Some(m) => (m.sstables, m.active_wal, m.min_replay_wal),
            None => {
                let mut ids = Vec::new();
                let mut max_id = 0;
                for entry in std::fs::read_dir(path)? {
                    let p = entry?.path();
                    // Only complete `*.sst` (atomic rename guarantees this);
                    // partial `*.sst.tmp` and orphans are ignored.
                    if p.extension().and_then(|s| s.to_str()) == Some("sst")
                        && let Some(name) = p.file_stem().and_then(|n| n.to_str())
                        && let Ok(id) = name.parse::<u64>()
                    {
                        max_id = std::cmp::max(max_id, id);
                        ids.push(id);
                    }
                }
                // No manifest ⇒ replay whatever WAL lineage is on disk (floor 0).
                (ids, max_id + 1, 0)
            }
        };

        sst_ids.sort_unstable();
        let sstables: Vec<Sstable> = sst_ids
            .iter()
            .map(|id| Sstable::open(path.join(format!("{id}.sst"))))
            .collect();
        let next = sst_ids
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(active_wal_id)
            + 1;

        let wal_path = path.join(format!("{active_wal_id}.log"));
        let wal = Arc::new(FileWalEngine::with_encryption(&wal_path, key)?);

        let engine = Self {
            snapshot_gate: RwLock::new(()),
            snapshot_failed: std::sync::atomic::AtomicBool::new(false),
            data_dir: Some(path.to_path_buf()),
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(sstables),
            wal: RwLock::new(Some(wal)),
            active_wal_id: AtomicU64::new(active_wal_id),
            min_replay_wal: AtomicU64::new(min_replay_wal),
            next_file_id: AtomicU64::new(next),
            memtable_bytes: AtomicUsize::new(0),
            flush_threshold: AtomicUsize::new(DEFAULT_FLUSH_THRESHOLD),
            gc_watermark: AtomicU64::new(0),
            flush_compact_lock: Mutex::new(()),
            intent_mutation_lock: Mutex::new(()),
            inflight_appends: AtomicUsize::new(0),
            #[cfg(test)]
            append_pause: AppendPause::default(),
            #[cfg(test)]
            flush_pause: AppendPause::default(),
            _wal_key: key,
            telemetry_observer: OnceLock::new(),
        };

        engine.recover()?;
        // Recompute the memtable size from what the WAL replay restored, and make
        // sure a manifest exists (writes the initial one for legacy/fresh dirs).
        let restored = engine
            .memtable
            .read()
            .unwrap()
            .iter()
            .map(|(k, c)| chain_bytes(k, c))
            .sum();
        engine.set_memtable_bytes(restored);
        engine.save_manifest()?;
        Ok(engine)
    }

    /// Path of the manifest file, when persistent.
    fn manifest_path(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|d| d.join(MANIFEST_FILE))
    }

    /// Atomically persists the current file set (live SSTables + active WAL).
    fn save_manifest(&self) -> Result<()> {
        let Some(manifest_path) = self.manifest_path() else {
            return Ok(());
        };
        let sstables: Vec<u64> = self
            .sstables
            .read()
            .unwrap()
            .iter()
            .filter_map(|s| s.path.file_stem()?.to_str()?.parse::<u64>().ok())
            .collect();
        let manifest = Manifest {
            active_wal: self.active_wal_id.load(Ordering::Relaxed),
            sstables,
            min_replay_wal: self.min_replay_wal.load(Ordering::Relaxed),
        };
        let bytes = serde_json::to_vec(&manifest)?;
        let tmp = manifest_path.with_extension("tmp");
        std::fs::write(&tmp, &bytes)?;
        if let Ok(f) = std::fs::File::open(&tmp) {
            let _ = f.sync_all();
        }
        std::fs::rename(&tmp, &manifest_path)?;
        if let Some(dir) = manifest_path.parent()
            && let Ok(f) = std::fs::File::open(dir)
        {
            let _ = f.sync_all();
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        self.flush_inner()
    }

    fn flush_inner(&self) -> Result<()> {
        if self.data_dir.is_none() || self.memtable.read().unwrap().is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let result = self.flush_inner_impl();
        self.emit_telemetry(LsmTelemetryEvent::Flush {
            elapsed: started.elapsed(),
            success: result.is_ok(),
        });
        result
    }

    fn flush_inner_impl(&self) -> Result<()> {
        let dir = match &self.data_dir {
            Some(d) => d.clone(),
            None => return Ok(()), // no-op for in-memory only
        };

        {
            // Serialize against compaction; both rewrite the SSTable set + manifest.
            let _guard = self.flush_compact_lock.lock().unwrap();
            let mut mem_guard = self.memtable.write().unwrap();
            if mem_guard.is_empty() {
                return Ok(());
            }

            // Flush only fully-committed keys. Keys still carrying an uncommitted
            // intent are retained in the memtable, so a later `commit`/`abort`
            // (which mutates the chain in place) can still find them — otherwise a
            // flush mid-transaction would strand the intent in an SSTable forever.
            let active_keys: HashSet<Bytes> = self
                .intents
                .read()
                .unwrap()
                .values()
                .flat_map(|keys| keys.iter().map(|(k, _)| k.clone()))
                .collect();
            let mut to_flush = BTreeMap::new();
            mem_guard.retain(|k, chain| {
                if active_keys.contains(k) {
                    true
                } else {
                    to_flush.insert(k.clone(), chain.clone());
                    false
                }
            });
            let retained: usize = mem_guard.iter().map(|(k, c)| chain_bytes(k, c)).sum();
            self.set_memtable_bytes(retained);

            if !to_flush.is_empty() {
                // Durably + atomically publish committed data as an SSTable.
                let sst_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
                let sst = Sstable::build(dir.join(format!("{sst_id}.sst")), &to_flush)?;
                let count = {
                    let mut sstables = self.sstables.write().unwrap();
                    sstables.push(sst);
                    sstables.len()
                };
                self.emit_sstable_count(count);
            }

            // Rotate to a fresh WAL segment; older segments stay on disk for the
            // backup WAL-archiver / PITR. The manifest swap below makes the new
            // file set durable (recovery reads the manifest, not file names).
            let predecessor = self.active_wal_id.load(Ordering::Relaxed);
            let wal_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
            let new_wal = Arc::new(FileWalEngine::with_encryption(
                dir.join(format!("{wal_id}.log")),
                self._wal_key,
            )?);
            // Record this segment's lineage as its first record so PITR can
            // verify an unbroken archived chain despite sparse segment ids, and
            // so crash recovery can walk back to any segment still holding a
            // live intent. fsync the header before the manifest commit below so
            // the lineage link is durable: recovery and PITR both depend on it.
            new_wal.append(WalRecord::V1(WalRecordV1::SegmentHeader {
                predecessor: Some(predecessor),
            }))?;
            new_wal.sync()?;
            // Hold the WAL write lock from the swap until the manifest naming
            // the new segment is durable. No append may land in the new segment
            // before recovery could ever find it: a commit() fsyncing its
            // `CommitTxn` into a segment the manifest does not yet name would
            // be silently dropped by a crash before the manifest commit — an
            // acknowledged write lost ('no ack before durable publication').
            // The critical section is one manifest write + fsync. Writers
            // increment `inflight_appends` BEFORE acquiring `wal.read`, so this
            // lock is the only synchronization needed: the floor check below is
            // a single load, never a wait, and cannot deadlock against a writer
            // blocked on `wal.read` here. Lock order note: `commit`/`write_intent`
            // release `wal.read` before taking the memtable lock, so holding
            // `mem_guard` + `wal` write here creates no lock cycle.
            let mut wal_guard = self.wal.write().unwrap();
            let previous_wal = (*wal_guard).replace(new_wal);
            self.active_wal_id.store(wal_id, Ordering::Relaxed);
            // Advance the replay floor only when nothing in an older segment
            // can still be needed: this flush retained no uncommitted intents
            // AND no intent append is in flight. If intents were retained
            // (`mem_guard` still holds their keys), an older segment may carry
            // their `WriteIntent`/committed-version records while the matching
            // `CommitTxn` will land in this new segment, so the floor must
            // stay put to keep the whole lineage in the replay window. An
            // in-flight append is the same situation one step earlier: its
            // `WriteIntent` is already in the old segment but its registration
            // is still pending, so a floor advance would drop a transaction
            // whose commit lands after the rotation.
            let previous_floor = self.min_replay_wal.load(Ordering::Relaxed);
            if mem_guard.is_empty() && self.inflight_appends.load(Ordering::SeqCst) == 0 {
                self.min_replay_wal.store(wal_id, Ordering::Relaxed);
            }
            self.pause_before_manifest_save();
            if let Err(error) = self.save_manifest() {
                // Publication failed: the manifest on disk still names the old
                // segment. Roll the in-memory rotation back so future appends
                // keep landing in the published segment recovery will replay,
                // instead of an orphan the manifest never names. (The SSTable
                // push above is not rolled back: after a crash its data is
                // rebuilt from the retained WAL lineage, so it stays correct.)
                *wal_guard = previous_wal;
                self.active_wal_id.store(predecessor, Ordering::Relaxed);
                self.min_replay_wal.store(previous_floor, Ordering::Relaxed);
                return Err(error);
            }
        }

        // Bound read amplification once enough SSTables have accumulated.
        if self.sstables.read().unwrap().len() >= COMPACTION_TRIGGER {
            self.compact_inner()?;
        }
        Ok(())
    }

    /// Merges all live SSTables into one, combining each key's version chains and
    /// dropping versions reclaimable below the current GC watermark. Crash-safe:
    /// the new SSTable is published durably, the manifest is swapped to point at
    /// it alone (the commit point — a crash before this leaves the old set in the
    /// manifest, orphaning the new file), then the old files are deleted.
    pub fn compact(&self) -> Result<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        self.compact_inner()
    }

    fn compact_inner(&self) -> Result<()> {
        if self.data_dir.is_none() || self.sstables.read().unwrap().len() < 2 {
            return Ok(());
        }
        let started = Instant::now();
        let result = self.compact_inner_impl();
        self.emit_telemetry(LsmTelemetryEvent::Compaction {
            elapsed: started.elapsed(),
            success: result.is_ok(),
        });
        result
    }

    fn compact_inner_impl(&self) -> Result<()> {
        let dir = match &self.data_dir {
            Some(d) => d.clone(),
            None => return Ok(()),
        };
        let _guard = self.flush_compact_lock.lock().unwrap();

        // Snapshot the current SSTable paths. `flush_compact_lock` ensures the set
        // does not change underneath us, so replacing it wholesale below is safe.
        let old_paths: Vec<PathBuf> = self
            .sstables
            .read()
            .unwrap()
            .iter()
            .map(|s| s.path.clone())
            .collect();
        if old_paths.len() < 2 {
            return Ok(());
        }

        // Merge oldest → newest so newer versions extend the chain.
        let watermark = self.gc_watermark.load(Ordering::Relaxed);
        let mut merged: BTreeMap<Bytes, VersionChain> = BTreeMap::new();
        let sorted = {
            let mut p = old_paths.clone();
            p.sort_by_key(|path| {
                path.file_stem()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0)
            });
            p
        };
        for path in &sorted {
            for item in Sstable::open(path).iter()? {
                let (key, chain) = item?;
                merged
                    .entry(key)
                    .or_default()
                    .versions
                    .extend(chain.versions);
            }
        }
        // Normalize each merged chain (dedup + drop GC-able versions); drop keys
        // left with no versions. Compaction merges ALL SSTables into one output,
        // so the merged chain is the key's complete history and a newest-visible
        // tombstone is fully reclaimable (`GcMode::Complete`) — this is what
        // bounds tombstone accumulation.
        merged.retain(|_, chain| {
            chain.versions.sort_by_key(|v| v.version);
            chain.versions.dedup();
            chain.garbage_collect(watermark, GcMode::Complete);
            !chain.versions.is_empty()
        });

        let new_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let new_sst = Sstable::build(dir.join(format!("{new_id}.sst")), &merged)?;

        // Atomic swap: install the single compacted SSTable, persist the manifest
        // (commit point), then delete the now-orphaned old files.
        *self.sstables.write().unwrap() = vec![new_sst];
        self.emit_sstable_count(1);
        self.save_manifest()?;
        for path in old_paths {
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }

    /// Flushes if the memtable has grown past its byte threshold. Called after
    /// each write; the guards are already released, so `flush` can re-take them.
    fn maybe_flush_by_size(&self) -> Result<()> {
        if self.data_dir.is_some()
            && self.memtable_bytes.load(Ordering::Relaxed)
                >= self.flush_threshold.load(Ordering::Relaxed)
        {
            self.flush_inner()?;
        }
        Ok(())
    }

    /// Marks the start of an intent WAL append. The returned guard ends the
    /// in-flight window on drop; the caller must hold it from before the WAL
    /// append until the intent is registered in the memtable (or the write
    /// fails), so `flush_inner` can tell a not-yet-registered append apart
    /// from quiescence before it advances the replay floor.
    fn track_append(&self) -> InflightAppendGuard<'_> {
        self.inflight_appends.fetch_add(1, Ordering::SeqCst);
        InflightAppendGuard {
            counter: &self.inflight_appends,
        }
    }

    /// Test-only pause point between an intent's WAL append and its memtable
    /// registration. A no-op unless a test armed the pause.
    #[inline]
    fn pause_after_wal_append(&self) {
        #[cfg(test)]
        {
            let mut state = self.append_pause.state.lock().unwrap();
            if !state.armed {
                return;
            }
            state.parked += 1;
            self.append_pause.parked.notify_all();
            let generation = state.generation;
            while state.generation == generation {
                state = self.append_pause.release.wait(state).unwrap();
            }
            state.parked -= 1;
        }
    }

    /// Arms the post-WAL-append pause for subsequent intent writes (tests).
    #[cfg(test)]
    fn arm_append_pause(&self) {
        self.append_pause.state.lock().unwrap().armed = true;
    }

    /// Test-only pause point inside the WAL rotation, after the segment swap
    /// and before the manifest publication. A no-op unless a test armed the
    /// pause. Runs while the `wal` write lock is held, so a racing commit
    /// blocks on `wal.read` for the whole parked window.
    #[inline]
    fn pause_before_manifest_save(&self) {
        #[cfg(test)]
        {
            let mut state = self.flush_pause.state.lock().unwrap();
            if !state.armed {
                return;
            }
            state.parked += 1;
            self.flush_pause.parked.notify_all();
            let generation = state.generation;
            while state.generation == generation {
                state = self.flush_pause.release.wait(state).unwrap();
            }
            state.parked -= 1;
        }
    }

    /// Arms the pre-manifest-save pause for the next flush (tests).
    #[cfg(test)]
    fn arm_flush_pause(&self) {
        self.flush_pause.state.lock().unwrap().armed = true;
    }

    /// Blocks until `n` flushes are parked between the WAL swap and the
    /// manifest publication (tests).
    #[cfg(test)]
    fn wait_for_parked_flush(&self, n: usize) {
        let mut state = self.flush_pause.state.lock().unwrap();
        while state.parked < n {
            state = self.flush_pause.parked.wait(state).unwrap();
        }
    }

    /// Releases every flush parked before the manifest publication and
    /// disarms the pause (tests).
    #[cfg(test)]
    fn release_parked_flush(&self) {
        let mut state = self.flush_pause.state.lock().unwrap();
        state.armed = false;
        state.generation += 1;
        self.flush_pause.release.notify_all();
    }

    /// Blocks until `n` writers are parked between WAL append and intent
    /// registration (tests).
    #[cfg(test)]
    fn wait_for_parked_appends(&self, n: usize) {
        let mut state = self.append_pause.state.lock().unwrap();
        while state.parked < n {
            state = self.append_pause.parked.wait(state).unwrap();
        }
    }

    /// Releases every writer parked between WAL append and intent
    /// registration and disarms the pause (tests).
    #[cfg(test)]
    fn release_parked_appends(&self) {
        let mut state = self.append_pause.state.lock().unwrap();
        state.armed = false;
        state.generation += 1;
        self.append_pause.release.notify_all();
    }

    fn recover(&self) -> Result<()> {
        let Some(dir) = self.data_dir.clone() else {
            return Ok(());
        };
        let active = self.active_wal_id.load(Ordering::Relaxed);
        let floor = self.min_replay_wal.load(Ordering::Relaxed);

        // Walk the WAL-segment lineage backward from the active segment down to
        // the replay floor, collecting each segment's records. A flush rotates
        // to a fresh segment but retains keys with uncommitted intents in the
        // memtable; those intents' `WriteIntent` records live in the *older*
        // segment while the matching `CommitTxn` lands in a newer one. Replaying
        // only the active segment (the previous behavior) therefore dropped any
        // transaction whose intent predated a flush — a committed-data-loss bug.
        // Replaying the whole lineage oldest→newest reconstructs it correctly;
        // versions are ordered by commit ts, so re-applying data already flushed
        // to an SSTable is idempotent (duplicate versions dedup on compaction).
        let mut segments: Vec<Vec<WalRecord>> = Vec::new();
        let mut cur = Some(active);
        let mut visited: HashSet<u64> = HashSet::new();
        while let Some(id) = cur {
            if id < floor || !visited.insert(id) {
                break;
            }
            let seg_path = dir.join(format!("{id}.log"));
            if !seg_path.exists() {
                break;
            }
            // Reuse the already-open active segment; open older segments read-only.
            let records = if id == active {
                match self.wal.read().unwrap().as_ref() {
                    Some(w) => w.recover()?,
                    None => break,
                }
            } else {
                FileWalEngine::with_encryption(&seg_path, self._wal_key)?.recover()?
            };
            // A segment's predecessor is named in its `SegmentHeader` (first
            // record). The very first segment ever created has none, which ends
            // the walk.
            let predecessor = records.iter().find_map(|r| match r {
                WalRecord::V1(WalRecordV1::SegmentHeader { predecessor }) => *predecessor,
                _ => None,
            });
            segments.push(records);
            cur = predecessor;
        }

        {
            let mut mem_guard = self.memtable.write().unwrap();
            let mut int_guard = self.intents.write().unwrap();

            // Oldest → newest so commits observe their intents and later versions
            // extend earlier ones.
            for records in segments.into_iter().rev() {
                for record in records {
                    let WalRecord::V1(rec) = record;
                    match rec {
                        WalRecordV1::WriteIntent {
                            txn_id,
                            key,
                            value,
                            started_at,
                        } => {
                            let k = Bytes::from(key);
                            let chain = mem_guard.entry(k.clone()).or_default();
                            let _ = chain.write_intent(txn_id, value);
                            // Records from before `started_at` existed fall back
                            // to the replay time: the intent reads as young,
                            // deferring janitor action (the safe direction).
                            int_guard
                                .entry(txn_id)
                                .or_default()
                                .push((k, started_at.unwrap_or_else(wallclock_micros)));
                        }
                        WalRecordV1::DeleteIntent {
                            txn_id,
                            key,
                            started_at,
                        } => {
                            let k = Bytes::from(key);
                            let chain = mem_guard.entry(k.clone()).or_default();
                            let _ = chain.delete_intent(txn_id);
                            int_guard
                                .entry(txn_id)
                                .or_default()
                                .push((k, started_at.unwrap_or_else(wallclock_micros)));
                        }
                        WalRecordV1::CommitTxn { txn_id, commit_ts } => {
                            if let Some(keys) = int_guard.remove(&txn_id) {
                                for (key, _) in keys {
                                    if let Some(chain) = mem_guard.get_mut(&key) {
                                        let _ = chain.commit(txn_id, commit_ts);
                                    }
                                }
                            }
                        }
                        WalRecordV1::CommitTxnBatchV1 { commits } => {
                            for (txn_id, commit_ts) in commits {
                                if let Some(keys) = int_guard.remove(&txn_id) {
                                    for (key, _) in keys {
                                        if let Some(chain) = mem_guard.get_mut(&key) {
                                            let _ = chain.commit(txn_id, commit_ts);
                                        }
                                    }
                                }
                            }
                        }
                        WalRecordV1::AbortTxn { txn_id } => {
                            if let Some(keys) = int_guard.remove(&txn_id) {
                                for (key, _) in keys {
                                    if let Some(chain) = mem_guard.get_mut(&key) {
                                        let _ = chain.abort(txn_id);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }
}

impl Default for LsmKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for LsmKvEngine {
    fn reclaim_archived_wal_segment(&self, segment_id: u64) -> Result<bool> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let _checkpoint = self.flush_compact_lock.lock().unwrap();
        let Some(dir) = &self.data_dir else {
            return Ok(false);
        };
        // Consult the published manifest, not in-memory counters: a failed
        // manifest publication may have already advanced those counters.
        let manifest: Manifest = serde_json::from_slice(&std::fs::read(dir.join(MANIFEST_FILE))?)?;
        if segment_id >= manifest.min_replay_wal || segment_id == manifest.active_wal {
            return Ok(false);
        }
        match std::fs::remove_file(dir.join(format!("{segment_id}.log"))) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn snapshot_rows(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
    ) -> Result<Vec<nodus_storage_api::SnapshotRow>> {
        self.export_snapshot_rows(scope)
    }

    fn replace_snapshot(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
        rows: Vec<nodus_storage_api::SnapshotRow>,
        pointers: Vec<nodus_storage_api::SnapshotRow>,
    ) -> Result<()> {
        self.replace_snapshot_rows(scope, rows, pointers)
    }

    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        Ok(self
            .intents
            .read()
            .unwrap()
            .values()
            .flatten()
            .any(|(key, _)| key.starts_with(prefix)))
    }

    fn flush(&self) -> Result<()> {
        self.flush()
    }

    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        // A visible tombstone in a newer source hides every older source:
        // stop there instead of falling through, or the deleted row would
        // resurrect from an SSTable/older state. `Absent` (no version visible
        // at `read_ts` in this chain) keeps searching older sources.
        let guard = self.memtable.read().unwrap();
        if let Some(chain) = guard.get(key) {
            match chain.read_visible(read_ts) {
                VisibleVersion::Value(val) => return Ok(Some(Bytes::from(val.to_vec()))),
                VisibleVersion::Tombstone => return Ok(None),
                VisibleVersion::Absent => {}
            }
        }

        // Search through sstables from newest to oldest
        let sst_guard = self.sstables.read().unwrap();
        for sst in sst_guard.iter().rev() {
            if let Some(chain) = sst.get(key)? {
                match chain.read_visible(read_ts) {
                    VisibleVersion::Value(val) => return Ok(Some(Bytes::from(val.to_vec()))),
                    VisibleVersion::Tombstone => return Ok(None),
                    VisibleVersion::Absent => {}
                }
            }
        }

        Ok(None)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let start = Bytes::from(range.start.as_ref().to_vec());
        let end = Bytes::from(range.end.as_ref().to_vec());

        // Snapshot the memtable range (bounded by the flush threshold) and open
        // one lazy file iterator per SSTable, then release the locks. Each
        // iterator owns its file descriptor, so a compaction that later unlinks
        // the file doesn't disturb an in-flight scan (the inode stays alive while
        // the fd is open). Lock order is memtable-before-sstables, as in `get`.
        let mem_guard = self.memtable.read().unwrap();
        let sst_guard = self.sstables.read().unwrap();
        let mem_snapshot: Vec<(Bytes, VersionChain)> = mem_guard
            .range(start.clone()..end.clone())
            .map(|(k, chain)| (k.clone(), chain.clone()))
            .collect();
        // Ascending precedence: earlier SSTables first, then the memtable last, so
        // the highest-precedence chain for a key wins the merge (matching `get`).
        let mut sources: Vec<ScanSource> = Vec::with_capacity(sst_guard.len() + 1);
        for sst in sst_guard.iter() {
            // An unavailable source may contain committed rows. Failing to
            // open it must fail the scan, never silently shorten the result.
            let iter = sst.range_iter(&start, end.clone())?;
            let lower = start.clone();
            // `range_iter` may emit a few keys below `start` from the first
            // block; drop them here while preserving iterator errors.
            let bounded = iter.filter(move |item| match item {
                Ok((k, _)) => k.as_ref() >= lower.as_ref(),
                Err(_) => true,
            });
            sources.push(ScanSource::new(Box::new(bounded)));
        }
        drop(sst_guard);
        drop(mem_guard);
        sources.push(ScanSource::new(Box::new(mem_snapshot.into_iter().map(Ok))));

        Ok(Box::new(LazyMergeScan {
            sources,
            read_ts,
            errored: false,
        }))
    }

    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let start = Bytes::from(range.start.as_ref().to_vec());
        let end = Bytes::from(range.end.as_ref().to_vec());
        let mut merged: BTreeMap<Bytes, VersionChain> = BTreeMap::new();

        let mem_guard = self.memtable.read().unwrap();
        let sst_guard = self.sstables.read().unwrap();
        for sst in sst_guard.iter() {
            for item in sst.iter()? {
                let (key, chain) = item?;
                if key >= start && key < end {
                    merged
                        .entry(key)
                        .or_default()
                        .versions
                        .extend(chain.versions);
                }
            }
        }
        for (key, chain) in mem_guard.range(start..end) {
            merged
                .entry(key.clone())
                .or_default()
                .versions
                .extend(chain.versions.clone());
        }
        drop(sst_guard);
        drop(mem_guard);

        let mut results = Vec::new();
        for (key, mut chain) in merged {
            chain.versions.sort_by_key(|v| (v.version, v.value.clone()));
            chain.versions.dedup();
            for version in chain
                .versions
                .iter()
                .filter(|v| !v.is_intent && v.version > since_ts && v.version <= read_ts)
            {
                results.push(Ok(KvVersion {
                    key: key.clone(),
                    value: version
                        .value
                        .as_ref()
                        .map(|value| Bytes::from(value.clone())),
                    version: version.version,
                }));
            }
        }
        results.sort_by(|a, b| match (a, b) {
            (Ok(left), Ok(right)) => left
                .key
                .cmp(&right.key)
                .then_with(|| left.version.cmp(&right.version)),
            _ => std::cmp::Ordering::Equal,
        });

        Ok(Box::new(results.into_iter()))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let added = key.len() + value.len() + 32;
        let started_at = wallclock_micros();
        {
            let _mutation = self.intent_mutation_lock.lock().unwrap();
            // In flight from the WAL append until the intent is registered:
            // flush consults `inflight_appends` before advancing the replay
            // floor, so this record's segment is never rotated out of the
            // replay window while its registration is still pending.
            let _inflight = self.track_append();
            // Append to the WAL and release the WAL lock *before* taking the memtable
            // lock, so this never holds wal+memtable in the opposite order from
            // `flush` (which holds memtable then rotates the WAL) — avoiding deadlock.
            {
                let wal_guard = self.wal.read().unwrap();
                if let Some(wal) = wal_guard.as_ref() {
                    wal.append(WalRecord::V1(WalRecordV1::WriteIntent {
                        txn_id,
                        key: key.to_vec(),
                        value: value.to_vec(),
                        started_at: Some(started_at),
                    }))?;
                }
            }
            self.pause_after_wal_append();

            let mut store_guard = self.memtable.write().unwrap();
            let mut intents_guard = self.intents.write().unwrap();

            let chain = store_guard.entry(key.clone()).or_default();
            chain.write_intent(txn_id, value.to_vec())?;
            intents_guard
                .entry(txn_id)
                .or_default()
                .push((key, started_at));
        }
        self.add_memtable_bytes(added);
        self.maybe_flush_by_size()?;
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let added = key.len() + 32;
        let started_at = wallclock_micros();
        {
            let _mutation = self.intent_mutation_lock.lock().unwrap();
            // In flight exactly as in `write_intent`.
            let _inflight = self.track_append();
            {
                let wal_guard = self.wal.read().unwrap();
                if let Some(wal) = wal_guard.as_ref() {
                    wal.append(WalRecord::V1(WalRecordV1::DeleteIntent {
                        txn_id,
                        key: key.to_vec(),
                        started_at: Some(started_at),
                    }))?;
                }
            }
            self.pause_after_wal_append();

            let mut store_guard = self.memtable.write().unwrap();
            let mut intents_guard = self.intents.write().unwrap();

            let chain = store_guard.entry(key.clone()).or_default();
            chain.delete_intent(txn_id)?;
            intents_guard
                .entry(txn_id)
                .or_default()
                .push((key, started_at));
        }
        self.add_memtable_bytes(added);
        self.maybe_flush_by_size()?;
        Ok(())
    }

    fn write_intent_batch(&self, txn_id: TxnId, ops: Vec<IntentOperation>) -> KvResult<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let added = ops
            .iter()
            .map(|op| match op {
                IntentOperation::Put { key, value } => key.len() + value.len() + 32,
                IntentOperation::Delete { key } => key.len() + 32,
            })
            .sum::<usize>();
        let started_at = wallclock_micros();
        {
            let _mutation = self.intent_mutation_lock.lock().unwrap();
            {
                let store_guard = self.memtable.read().unwrap();
                for op in &ops {
                    if let Some(intent) = store_guard
                        .get(op.key())
                        .and_then(|chain| chain.active_intent())
                        && intent.txn_id != Some(txn_id)
                    {
                        return Err(nodus_storage_api::KvError::WriteConflict(txn_id));
                    }
                }
            }

            let _inflight = self.track_append();
            {
                let wal_guard = self.wal.read().unwrap();
                if let Some(wal) = wal_guard.as_ref() {
                    for op in &ops {
                        let record = match op {
                            IntentOperation::Put { key, value } => WalRecordV1::WriteIntent {
                                txn_id,
                                key: key.to_vec(),
                                value: value.to_vec(),
                                started_at: Some(started_at),
                            },
                            IntentOperation::Delete { key } => WalRecordV1::DeleteIntent {
                                txn_id,
                                key: key.to_vec(),
                                started_at: Some(started_at),
                            },
                        };
                        wal.append(WalRecord::V1(record))?;
                    }
                }
            }
            self.pause_after_wal_append();

            let mut store_guard = self.memtable.write().unwrap();
            let mut intents_guard = self.intents.write().unwrap();
            for op in ops {
                let key = match &op {
                    IntentOperation::Put { key, .. } | IntentOperation::Delete { key } => {
                        key.clone()
                    }
                };
                let chain = store_guard.entry(key.clone()).or_default();
                match op {
                    IntentOperation::Put { value, .. } => {
                        chain.write_intent(txn_id, value.to_vec())?
                    }
                    IntentOperation::Delete { .. } => chain.delete_intent(txn_id)?,
                }
                intents_guard
                    .entry(txn_id)
                    .or_default()
                    .push((key, started_at));
            }
        }
        self.add_memtable_bytes(added);
        self.maybe_flush_by_size()?;
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let _mutation = self.intent_mutation_lock.lock().unwrap();
        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();
        let chain = store_guard.entry(key.clone()).or_default();
        chain
            .versions
            .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
        match replacement {
            IntentReplacement::Put(value) => {
                chain.write_intent(txn_id, value.to_vec())?;
                intents_guard
                    .entry(txn_id)
                    .or_default()
                    .push((key, wallclock_micros()));
            }
            IntentReplacement::Delete => {
                chain.delete_intent(txn_id)?;
                intents_guard
                    .entry(txn_id)
                    .or_default()
                    .push((key, wallclock_micros()));
            }
            IntentReplacement::Clear => {
                if let Some(keys) = intents_guard.get_mut(&txn_id) {
                    keys.retain(|(k, _)| k != &key);
                    if keys.is_empty() {
                        intents_guard.remove(&txn_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn pending_intent_keys(&self, txn_id: TxnId) -> Vec<Bytes> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.intents
            .read()
            .unwrap()
            .get(&txn_id)
            .map(|keys| keys.iter().map(|(k, _)| k.clone()).collect())
            .unwrap_or_default()
    }

    fn pending_intents(&self, prefix: &[u8]) -> Result<Vec<PendingIntents>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let intents = self.intents.read().unwrap();
        Ok(intents
            .iter()
            .filter_map(|(txn_id, keys)| {
                let matched: Vec<&(Bytes, Timestamp)> =
                    keys.iter().filter(|(k, _)| k.starts_with(prefix)).collect();
                if matched.is_empty() {
                    return None;
                }
                let oldest_ts = matched.iter().map(|(_, ts)| *ts).min().unwrap_or(0);
                Some(PendingIntents {
                    txn_id: *txn_id,
                    keys: matched.iter().map(|(k, _)| k.clone()).collect(),
                    oldest_ts,
                })
            })
            .collect())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let _mutation = self.intent_mutation_lock.lock().unwrap();
        {
            let wal_guard = self.wal.read().unwrap();
            if let Some(wal) = wal_guard.as_ref() {
                wal.append(WalRecord::V1(WalRecordV1::CommitTxn { txn_id, commit_ts }))?;
                wal.sync()?;
            }
        }

        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for (key, _) in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.commit(txn_id, commit_ts);
                }
            }
        }
        Ok(())
    }

    fn commit_batch(&self, commits: Vec<(TxnId, Timestamp)>) -> KvResult<()> {
        if commits.is_empty() {
            return Ok(());
        }
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let _mutation = self.intent_mutation_lock.lock().unwrap();
        {
            let wal_guard = self.wal.read().unwrap();
            if let Some(wal) = wal_guard.as_ref() {
                wal.append(WalRecord::V1(WalRecordV1::CommitTxnBatchV1 {
                    commits: commits.clone(),
                }))?;
                wal.sync()?;
            }
        }

        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();
        for (txn_id, commit_ts) in commits {
            if let Some(keys) = intents_guard.remove(&txn_id) {
                for (key, _) in keys {
                    if let Some(chain) = store_guard.get_mut(&key) {
                        let _ = chain.commit(txn_id, commit_ts);
                    }
                }
            }
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        let _mutation = self.intent_mutation_lock.lock().unwrap();
        {
            let wal_guard = self.wal.read().unwrap();
            if let Some(wal) = wal_guard.as_ref() {
                wal.append(WalRecord::V1(WalRecordV1::AbortTxn { txn_id }))?;
                wal.sync()?;
            }
        }

        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for (key, _) in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.abort(txn_id);
                }
            }
        }
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.snapshot_available()?;
        // Remember the watermark so the next compaction can reclaim superseded
        // versions from SSTables too, not just the memtable.
        self.gc_watermark.store(watermark, Ordering::Relaxed);
        let mut store = self.memtable.write().unwrap();
        let mut removed = 0usize;
        let mut dead_keys = Vec::new();

        for (key, chain) in store.iter_mut() {
            // The memtable is layered over SSTables that may still hold the
            // key, so the newest-visible tombstone must be RETAINED: erasing
            // it would empty the chain and `get` would fall through to an
            // SSTable, resurrecting the deleted row. Compaction (which merges
            // every source) is where tombstones are fully reclaimed.
            removed += chain.garbage_collect(watermark, GcMode::Layered);
            if chain.versions.is_empty() {
                dead_keys.push(key.clone());
            }
        }

        for k in dead_keys {
            store.remove(&k);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Commits `key=value` at `ts` in one transaction.
    fn put(engine: &LsmKvEngine, key: &str, value: &str, ts: Timestamp) {
        let txn = TxnId::new();
        engine
            .write_intent(
                txn,
                Bytes::from(key.to_owned()),
                Bytes::from(value.to_owned()),
            )
            .unwrap();
        engine.commit(txn, ts).unwrap();
    }

    fn scan_all(engine: &LsmKvEngine, read_ts: Timestamp) -> Vec<(String, String)> {
        engine
            .scan(
                KeyRange {
                    start: Bytes::from_static(b""),
                    end: Bytes::from_static(b"\xff"),
                },
                read_ts,
            )
            .unwrap()
            .map(|p| {
                let p = p.unwrap();
                (
                    String::from_utf8(p.key.to_vec()).unwrap(),
                    String::from_utf8(p.value.to_vec()).unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn scan_merges_memtable_over_sstable_in_key_order() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        // Three keys flushed to an SSTable...
        put(&engine, "k1", "a", 10);
        put(&engine, "k2", "b", 10);
        put(&engine, "k3", "c", 10);
        engine.flush().unwrap();
        // ...then an update to k2 and a new k4 in the memtable.
        put(&engine, "k2", "B", 20);
        put(&engine, "k4", "d", 20);

        // The merge yields every key once, in order, with the memtable's k2
        // overriding the SSTable's.
        assert_eq!(
            scan_all(&engine, 20),
            vec![
                ("k1".into(), "a".into()),
                ("k2".into(), "B".into()),
                ("k3".into(), "c".into()),
                ("k4".into(), "d".into()),
            ]
        );
    }

    #[test]
    fn scan_skips_keys_deleted_in_the_memtable() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        put(&engine, "k1", "a", 10);
        put(&engine, "k2", "b", 10);
        engine.flush().unwrap();

        // Tombstone k2 in the memtable; the merge must drop it (the memtable
        // tombstone outranks the SSTable value).
        let txn = TxnId::new();
        engine
            .delete_intent(txn, Bytes::from_static(b"k2"))
            .unwrap();
        engine.commit(txn, 20).unwrap();

        assert_eq!(scan_all(&engine, 20), vec![("k1".into(), "a".into())]);
    }

    #[test]
    fn scan_respects_an_older_read_timestamp() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        put(&engine, "k1", "a", 10);
        put(&engine, "k2", "b", 30);

        // At ts=20 only k1 is visible; k2's only version (ts=30) is in the future.
        assert_eq!(scan_all(&engine, 20), vec![("k1".into(), "a".into())]);
    }

    #[test]
    fn scan_finds_an_old_version_split_across_sstables() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        // `k` = v1 @10, flushed to one SSTable; then `k` = v2 @30, flushed to a
        // second. The two versions of the same key now live in different
        // SSTables (the normal state between compactions).
        put(&engine, "k", "v1", 10);
        engine.flush().unwrap();
        put(&engine, "k", "v2", 30);
        engine.flush().unwrap();

        // At read_ts=20 only v1 is visible — and it lives in the *older*,
        // lower-precedence SSTable. `scan` must find it (merging chains across
        // sources), agreeing with `get` rather than picking only the newest
        // SSTable's chain and skipping the key.
        assert_eq!(engine.get(b"k", 20).unwrap(), Some(Bytes::from("v1")));
        assert_eq!(scan_all(&engine, 20), vec![("k".into(), "v1".into())]);

        // At read_ts=30 the newer version wins.
        assert_eq!(scan_all(&engine, 30), vec![("k".into(), "v2".into())]);
    }

    #[test]
    fn sstable_range_iter_stops_at_end_without_reading_the_rest() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("many.sst");
        let mut data = std::collections::BTreeMap::new();
        for i in 0..1000 {
            // Zero-padded keys sort lexicographically in numeric order.
            data.insert(Bytes::from(format!("key{i:04}")), VersionChain::default());
        }
        Sstable::build(&path, &data).unwrap();
        let sst = Sstable::open(&path);

        // Starting before all keys and bounding at "key0005" must yield exactly
        // the first five entries — the iterator stops at the bound instead of
        // walking all 1000.
        let prefix: Vec<String> = sst
            .range_iter(b"", Bytes::from_static(b"key0005"))
            .unwrap()
            .map(|item| String::from_utf8(item.unwrap().0.to_vec()).unwrap())
            .collect();
        assert_eq!(
            prefix,
            vec!["key0000", "key0001", "key0002", "key0003", "key0004"]
        );
    }

    #[test]
    fn test_custom_lsm_mvcc_visibility() {
        let engine = LsmKvEngine::new();

        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");

        let txn = TxnId::new();
        engine.write_intent(txn, k1.clone(), v1.clone()).unwrap();

        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert!(res.is_none());

        engine.commit(txn, 10).unwrap();

        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert_eq!(res.unwrap(), v1);

        let res = engine.get(k1.as_ref(), 9).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_custom_lsm_garbage_collect() {
        let engine = LsmKvEngine::new();
        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        let v2 = Bytes::from("v2");

        let txn1 = TxnId::new();
        let txn2 = TxnId::new();

        engine.write_intent(txn1, k1.clone(), v1.clone()).unwrap();
        engine.commit(txn1, 10).unwrap();
        engine.write_intent(txn2, k1.clone(), v2.clone()).unwrap();
        engine.commit(txn2, 20).unwrap();

        assert_eq!(engine.get(k1.as_ref(), 15).unwrap().unwrap(), v1);
        assert_eq!(engine.get(k1.as_ref(), 25).unwrap().unwrap(), v2);

        let removed = engine.garbage_collect(15).unwrap();
        assert_eq!(removed, 0);

        let removed = engine.garbage_collect(25).unwrap();
        assert_eq!(removed, 1);

        assert!(engine.get(k1.as_ref(), 15).unwrap().is_none());
        assert_eq!(engine.get(k1.as_ref(), 25).unwrap().unwrap(), v2);
    }

    #[test]
    fn test_custom_lsm_scan() {
        let engine = LsmKvEngine::new();
        let txn = TxnId::new();
        engine
            .write_intent(txn, Bytes::from("a1"), Bytes::from("v1"))
            .unwrap();
        engine
            .write_intent(txn, Bytes::from("a2"), Bytes::from("v2"))
            .unwrap();
        engine
            .write_intent(txn, Bytes::from("a3"), Bytes::from("v3"))
            .unwrap();
        engine.commit(txn, 10).unwrap();

        let mut scan = engine
            .scan(
                KeyRange {
                    start: Bytes::from("a1"),
                    end: Bytes::from("a3"),
                },
                15,
            )
            .unwrap();

        let res1 = scan.next().unwrap().unwrap();
        assert_eq!(res1.key, Bytes::from("a1"));
        assert_eq!(res1.value, Bytes::from("v1"));

        let res2 = scan.next().unwrap().unwrap();
        assert_eq!(res2.key, Bytes::from("a2"));
        assert_eq!(res2.value, Bytes::from("v2"));

        assert!(scan.next().is_none()); // a3 is exclusive
    }

    #[test]
    fn unavailable_sstable_fails_reads_instead_of_hiding_committed_rows() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let txn = TxnId::new();
        engine
            .write_intent(txn, Bytes::from("k"), Bytes::from("v"))
            .unwrap();
        engine.commit(txn, 10).unwrap();
        engine.flush().unwrap();
        let path = engine.sstables.read().unwrap()[0].path.clone();
        let mut contents = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let range = || KeyRange {
            start: Bytes::from("a"),
            end: Bytes::from("z"),
        };
        assert!(engine.get(b"k", 20).is_err());
        assert!(engine.scan(range(), 20).is_err());
        assert!(engine.scan_versions(range(), 0, 20).is_err());
        // A valid footer with an invalid data entry fails during iteration.
        contents[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, contents).unwrap();
        assert!(engine.get(b"k", 20).is_err());
        let mut scan = engine.scan(range(), 20).unwrap();
        assert!(scan.next().unwrap().is_err());
        assert!(scan.next().is_none());
        assert!(engine.scan_versions(range(), 0, 20).is_err());
        // A truncated published file is corruption, not a valid empty table.
        std::fs::write(&path, b"truncated").unwrap();
        assert!(engine.get(b"k", 20).is_err());
        assert!(engine.scan(range(), 20).is_err());
        assert!(engine.scan_versions(range(), 0, 20).is_err());
    }

    #[test]
    fn test_scan_versions_includes_tombstones_after_flush() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let key = Bytes::from("k1");

        let put = TxnId::new();
        engine
            .write_intent(put, key.clone(), Bytes::from("v1"))
            .unwrap();
        engine.commit(put, 10).unwrap();
        engine.flush().unwrap();

        let delete = TxnId::new();
        engine.delete_intent(delete, key.clone()).unwrap();
        engine.commit(delete, 20).unwrap();
        engine.flush().unwrap();

        let versions: Vec<_> = engine
            .scan_versions(
                KeyRange {
                    start: Bytes::from("k"),
                    end: Bytes::from("l"),
                },
                10,
                30,
            )
            .unwrap()
            .map(|item| item.unwrap())
            .collect();

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].key, key);
        assert_eq!(versions[0].version, 20);
        assert!(versions[0].value.is_none());
    }

    #[test]
    fn test_custom_lsm_abort() {
        let engine = LsmKvEngine::new();
        let txn = TxnId::new();
        engine
            .write_intent(txn, Bytes::from("k1"), Bytes::from("v1"))
            .unwrap();
        assert!(engine.get(b"k1", 10).unwrap().is_none());

        engine.abort(txn).unwrap();
        assert!(engine.get(b"k1", 10).unwrap().is_none());
    }

    #[test]
    fn test_custom_lsm_delete_intent() {
        let engine = LsmKvEngine::new();
        let txn1 = TxnId::new();
        engine
            .write_intent(txn1, Bytes::from("k1"), Bytes::from("v1"))
            .unwrap();
        engine.commit(txn1, 10).unwrap();

        assert_eq!(engine.get(b"k1", 15).unwrap().unwrap(), Bytes::from("v1"));

        let txn2 = TxnId::new();
        engine.delete_intent(txn2, Bytes::from("k1")).unwrap();
        engine.commit(txn2, 20).unwrap();

        assert!(engine.get(b"k1", 25).unwrap().is_none()); // Tombstoned
        assert_eq!(engine.get(b"k1", 15).unwrap().unwrap(), Bytes::from("v1")); // Still visible in past snapshot
    }

    #[test]
    fn test_custom_lsm_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();

        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        let txn1 = TxnId::new();

        {
            let engine = LsmKvEngine::with_wal(temp_dir.path(), None).unwrap();
            engine.write_intent(txn1, k1.clone(), v1.clone()).unwrap();
            engine.commit(txn1, 10).unwrap();
        } // Engine drops, releasing file locks

        // Re-instantiate against same directory should recover MemTable from WAL
        let recovered_engine = LsmKvEngine::with_wal(temp_dir.path(), None).unwrap();
        let res = recovered_engine.get(k1.as_ref(), 15).unwrap();
        assert_eq!(res.unwrap(), v1);
    }

    #[test]
    fn test_custom_lsm_post_flush_wal_replay() {
        let temp_dir = TempDir::new().unwrap();

        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        let txn1 = TxnId::new();

        let k2 = Bytes::from("k2");
        let v2 = Bytes::from("v2");
        let txn2 = TxnId::new();

        {
            let engine = LsmKvEngine::with_wal(temp_dir.path(), None).unwrap();
            engine.write_intent(txn1, k1.clone(), v1.clone()).unwrap();
            engine.commit(txn1, 10).unwrap();

            // Flush forces memtable to SST and rotates WAL
            engine.flush().unwrap();

            // Write to new WAL
            engine.write_intent(txn2, k2.clone(), v2.clone()).unwrap();
            engine.commit(txn2, 20).unwrap();
        }

        // Re-instantiate should recover k1 from SST and k2 from the new WAL
        let recovered_engine = LsmKvEngine::with_wal(temp_dir.path(), None).unwrap();

        let res1 = recovered_engine.get(k1.as_ref(), 25).unwrap();
        assert_eq!(res1.unwrap(), v1);

        let res2 = recovered_engine.get(k2.as_ref(), 25).unwrap();
        assert_eq!(res2.unwrap(), v2);
    }

    #[test]
    fn torn_wal_tail_does_not_prevent_recovery() {
        let dir = TempDir::new().unwrap();
        let (k1, v1, txn) = (Bytes::from("k1"), Bytes::from("v1"), TxnId::new());
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            engine.write_intent(txn, k1.clone(), v1.clone()).unwrap();
            engine.commit(txn, 10).unwrap();
        }
        // Simulate a crash mid-append: a bogus partial frame at the WAL tail
        // (claims a 1000-byte record but only a few bytes follow).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join("1.log"))
                .unwrap();
            f.write_all(&1000u32.to_le_bytes()).unwrap();
            f.write_all(&[1, 2, 3, 4]).unwrap();
            f.sync_all().unwrap();
        }
        // Recovery truncates at the torn record and still returns the commit.
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(recovered.get(k1.as_ref(), 15).unwrap().unwrap(), v1);
    }

    #[test]
    fn partial_sstable_tmp_is_ignored_on_recovery() {
        let dir = TempDir::new().unwrap();
        let txn = TxnId::new();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            engine
                .write_intent(txn, Bytes::from("k"), Bytes::from("v"))
                .unwrap();
            engine.commit(txn, 10).unwrap();
        }
        // A crash mid-flush leaves a partial `*.sst.tmp`; recovery must ignore it
        // and rebuild from the (intact) WAL rather than loading garbage.
        std::fs::write(dir.path().join("99.sst.tmp"), b"garbage partial sstable").unwrap();
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(recovered.get(b"k", 15).unwrap().unwrap(), Bytes::from("v"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_mvcc_read_your_writes(key in any::<Vec<u8>>(), val in any::<Vec<u8>>()) {
            if key.starts_with(b"intent:") || key.is_empty() { return Ok(()); }
            let engine = LsmKvEngine::new();
            let k = Bytes::from(key);
            let v = Bytes::from(val);
            let txn = TxnId::new();
            engine.write_intent(txn, k.clone(), v.clone()).unwrap();
            engine.commit(txn, 10).unwrap();

            let res = engine.get(&k, 15).unwrap();
            prop_assert_eq!(res, Some(v));
        }

        #[test]
        fn test_mvcc_snapshot_isolation(val1 in any::<Vec<u8>>(), val2 in any::<Vec<u8>>()) {
            let engine = LsmKvEngine::new();
            let k = Bytes::from("prop_key");

            let txn1 = TxnId::new();
            engine.write_intent(txn1, k.clone(), Bytes::from(val1.clone())).unwrap();
            engine.commit(txn1, 10).unwrap();

            let txn2 = TxnId::new();
            engine.write_intent(txn2, k.clone(), Bytes::from(val2.clone())).unwrap();
            engine.commit(txn2, 20).unwrap();

            prop_assert_eq!(engine.get(&k, 15).unwrap(), Some(Bytes::from(val1)));
            prop_assert_eq!(engine.get(&k, 25).unwrap(), Some(Bytes::from(val2)));
            prop_assert_eq!(engine.get(&k, 5).unwrap(), None);
        }
    }
}

#[cfg(test)]
mod sst_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_flush_and_read_from_sst() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        let txn1 = TxnId::new();
        engine
            .write_intent(txn1, Bytes::from("key1"), Bytes::from("val1"))
            .unwrap();
        engine.commit(txn1, 10).unwrap();

        // Value is in memtable
        assert_eq!(
            engine.get(b"key1", 10).unwrap().unwrap(),
            Bytes::from("val1")
        );

        // Flush moves it to SSTable
        engine.flush().unwrap();

        // Check memtable is empty
        assert!(engine.memtable.read().unwrap().is_empty());

        // Value is still readable! Wait... does get() read from sstables?
        // Ah, our get() method only reads from memtable right now. We need to fix that!
    }

    #[test]
    fn block_indexed_sstable_round_trips_many_keys() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        // Enough keys to span several ~4 KiB data blocks, exercising the sparse
        // index binary search and the bloom filter.
        let n = 500u32;
        let txn = TxnId::new();
        for i in 0..n {
            engine
                .write_intent(
                    txn,
                    Bytes::from(format!("key{i:05}")),
                    Bytes::from(format!("val{i:05}")),
                )
                .unwrap();
        }
        engine.commit(txn, 10).unwrap();
        engine.flush().unwrap();
        assert!(
            engine.memtable.read().unwrap().is_empty(),
            "flushed to SSTable"
        );

        // Point lookups across the block range resolve via index + bloom.
        for i in [0u32, 1, 250, 499] {
            assert_eq!(
                engine
                    .get(format!("key{i:05}").as_bytes(), 15)
                    .unwrap()
                    .unwrap(),
                Bytes::from(format!("val{i:05}"))
            );
        }
        // Absent keys (bloom-negative or index-miss) return None, no false hits.
        assert!(engine.get(b"key99999", 15).unwrap().is_none());
        assert!(engine.get(b"aaa", 15).unwrap().is_none());
        assert!(engine.get(b"zzz", 15).unwrap().is_none());

        // Scans still yield sorted entries across blocks.
        let scanned: Vec<Bytes> = engine
            .scan(
                KeyRange {
                    start: Bytes::from("key00100"),
                    end: Bytes::from("key00103"),
                },
                15,
            )
            .unwrap()
            .filter_map(|r| r.ok())
            .map(|p| p.key)
            .collect();
        assert_eq!(
            scanned,
            vec![
                Bytes::from("key00100"),
                Bytes::from("key00101"),
                Bytes::from("key00102"),
            ]
        );
    }

    #[test]
    fn flush_retains_uncommitted_intents() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        let t1 = TxnId::new();
        engine
            .write_intent(t1, Bytes::from("committed"), Bytes::from("v"))
            .unwrap();
        engine.commit(t1, 10).unwrap();

        // An intent that is still in flight when a flush happens.
        let t2 = TxnId::new();
        engine
            .write_intent(t2, Bytes::from("pending"), Bytes::from("p"))
            .unwrap();
        engine.flush().unwrap();

        // Committed data is flushed; the uncommitted intent is invisible but must
        // be retained so its commit can still land.
        assert!(engine.get(b"committed", 15).unwrap().is_some());
        assert!(engine.get(b"pending", 15).unwrap().is_none());

        engine.commit(t2, 20).unwrap();
        assert_eq!(
            engine.get(b"pending", 25).unwrap().unwrap(),
            Bytes::from("p"),
            "intent survived the flush and committed"
        );
    }

    #[test]
    fn size_triggered_flush_persists_and_bounds_memtable() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        engine.set_flush_threshold(512);

        for i in 0..100u32 {
            let txn = TxnId::new();
            engine
                .write_intent(
                    txn,
                    Bytes::from(format!("key{i:03}")),
                    Bytes::from(vec![b'x'; 32]),
                )
                .unwrap();
            engine.commit(txn, (i as u64 + 1) * 10).unwrap();
        }

        assert!(
            !engine.sstables.read().unwrap().is_empty(),
            "size-triggered flush produced SSTables"
        );
        assert!(
            engine.memtable_bytes.load(Ordering::Relaxed) < 100 * 64,
            "memtable stayed bounded well under the total written"
        );
        for i in 0..100u32 {
            assert_eq!(
                engine
                    .get(format!("key{i:03}").as_bytes(), 100_000)
                    .unwrap()
                    .unwrap(),
                Bytes::from(vec![b'x'; 32])
            );
        }
    }

    #[test]
    fn compaction_merges_accumulated_sstables() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        // One SSTable per flush; the COMPACTION_TRIGGER-th flush compacts them.
        for i in 0..COMPACTION_TRIGGER {
            let txn = TxnId::new();
            engine
                .write_intent(
                    txn,
                    Bytes::from(format!("k{i}")),
                    Bytes::from(format!("v{i}")),
                )
                .unwrap();
            engine.commit(txn, (i as u64 + 1) * 10).unwrap();
            engine.flush().unwrap();
        }
        assert_eq!(
            engine.sstables.read().unwrap().len(),
            1,
            "accumulated SSTables compacted into one"
        );
        for i in 0..COMPACTION_TRIGGER {
            assert_eq!(
                engine
                    .get(format!("k{i}").as_bytes(), 100_000)
                    .unwrap()
                    .unwrap(),
                Bytes::from(format!("v{i}"))
            );
        }
    }

    #[test]
    fn manifest_recovery_after_flush_and_compaction() {
        let dir = TempDir::new().unwrap();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            for i in 0..(COMPACTION_TRIGGER + 1) {
                let txn = TxnId::new();
                engine
                    .write_intent(
                        txn,
                        Bytes::from(format!("m{i}")),
                        Bytes::from(format!("v{i}")),
                    )
                    .unwrap();
                engine.commit(txn, (i as u64 + 1) * 10).unwrap();
                engine.flush().unwrap();
            }
            // Also leave a key only in the active WAL (no flush after it).
            let txn = TxnId::new();
            engine
                .write_intent(txn, Bytes::from("wal_only"), Bytes::from("w"))
                .unwrap();
            engine.commit(txn, 100_000).unwrap();
        }

        // Reopen: the manifest names the live (compacted) SSTables, and the active
        // WAL is replayed for the un-flushed key.
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        for i in 0..(COMPACTION_TRIGGER + 1) {
            assert_eq!(
                engine
                    .get(format!("m{i}").as_bytes(), 1_000_000)
                    .unwrap()
                    .unwrap(),
                Bytes::from(format!("v{i}"))
            );
        }
        assert_eq!(
            engine.get(b"wal_only", 1_000_000).unwrap().unwrap(),
            Bytes::from("w")
        );
    }

    #[test]
    fn orphan_sstable_not_in_manifest_is_ignored() {
        let dir = TempDir::new().unwrap();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            let txn = TxnId::new();
            engine
                .write_intent(txn, Bytes::from("real"), Bytes::from("v"))
                .unwrap();
            engine.commit(txn, 10).unwrap();
            engine.flush().unwrap();
        }
        // Simulate a crash mid-flush/compaction: a fully-built SSTable that was
        // never recorded in the manifest (the manifest is the commit point).
        let mut ghost = std::collections::BTreeMap::new();
        let mut chain = VersionChain::new();
        let t = TxnId::new();
        chain.write_intent(t, b"ghost-value".to_vec()).unwrap();
        chain.commit(t, 5).unwrap();
        ghost.insert(Bytes::from("ghost"), chain);
        Sstable::build(dir.path().join("999.sst"), &ghost).unwrap();

        // Recovery trusts the manifest: the orphan is ignored, the real data stays.
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert!(
            engine.get(b"ghost", 100).unwrap().is_none(),
            "orphan SSTable ignored"
        );
        assert_eq!(engine.get(b"real", 100).unwrap().unwrap(), Bytes::from("v"));
    }

    #[test]
    fn corrupt_manifest_recovers_via_directory_scan() {
        let dir = TempDir::new().unwrap();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            let txn = TxnId::new();
            engine
                .write_intent(txn, Bytes::from("survivor"), Bytes::from("v"))
                .unwrap();
            engine.commit(txn, 10).unwrap();
            engine.flush().unwrap();
        }
        // A torn manifest must not make the engine unstartable: recovery falls
        // back to scanning the SSTable files on disk.
        std::fs::write(dir.path().join("MANIFEST"), b"{ this is not valid json").unwrap();

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"survivor", 100).unwrap().unwrap(),
            Bytes::from("v"),
            "recovered despite a corrupt manifest"
        );
    }

    #[test]
    fn commit_survives_when_intent_predates_a_flush() {
        // Regression: an intent whose `WriteIntent` was rotated into an older WAL
        // segment by a flush, with its `CommitTxn` landing in the new active
        // segment, must survive crash recovery. Replaying only the active segment
        // would drop it — a committed-data-loss bug.
        let dir = TempDir::new().unwrap();
        let txn = TxnId::new();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

            // Open an intent but do NOT commit it yet.
            engine
                .write_intent(txn, Bytes::from("pending"), Bytes::from("v1"))
                .unwrap();

            // Flush: "pending" still carries an uncommitted intent, so it is
            // retained in the memtable and its `WriteIntent` stays in the old
            // segment while the WAL rotates to a fresh active segment.
            engine.flush().unwrap();

            // Now commit — the `CommitTxn` record lands in the NEW segment.
            engine.commit(txn, 30).unwrap();
            // Engine drops here, simulating a crash/restart (commit() fsynced).
        }

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"pending", 100).unwrap(),
            Some(Bytes::from("v1")),
            "committed value lost: recovery did not replay the predecessor segment"
        );
    }

    #[test]
    fn archived_wal_reclamation_preserves_cross_segment_commits() {
        let dir = tempfile::tempdir().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let first = engine.active_wal_id.load(Ordering::Relaxed);
        let txn = TxnId::new();
        engine
            .write_intent(
                txn,
                Bytes::from_static(b"vote"),
                Bytes::from_static(b"term-7"),
            )
            .unwrap();
        engine.flush().unwrap(); // Retain the intent and its predecessor WAL.
        engine.commit(txn, 7).unwrap(); // Commit lands in the successor segment.
        assert!(!engine.reclaim_archived_wal_segment(first).unwrap());
        drop(engine);

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"vote", u64::MAX).unwrap(),
            Some(Bytes::from_static(b"term-7"))
        );
        engine.flush().unwrap(); // A complete checkpoint now covers the commit.
        assert!(engine.reclaim_archived_wal_segment(first).unwrap());
        assert!(
            !engine
                .reclaim_archived_wal_segment(engine.active_wal_id.load(Ordering::Relaxed))
                .unwrap()
        );
        drop(engine);
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"vote", u64::MAX).unwrap(),
            Some(Bytes::from_static(b"term-7"))
        );
    }

    #[test]
    fn wal_reclamation_uses_published_manifest_and_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let first = engine.active_wal_id.load(Ordering::Relaxed);
        engine
            .write_intent(
                TxnId::new(),
                Bytes::from_static(b"pending"),
                Bytes::from_static(b"value"),
            )
            .unwrap();
        engine.flush().unwrap();
        assert_ne!(first, engine.active_wal_id.load(Ordering::Relaxed));
        // Model counters advancing ahead of a failed manifest publication.
        engine.min_replay_wal.store(first + 10, Ordering::Relaxed);
        assert!(!engine.reclaim_archived_wal_segment(first).unwrap());
        let path = dir.path().join(format!("{first}.log"));
        assert!(path.exists());
        std::fs::write(dir.path().join(MANIFEST_FILE), b"invalid").unwrap();
        assert!(engine.reclaim_archived_wal_segment(first).is_err());
        assert!(path.exists());
    }

    #[test]
    fn replay_floor_advances_when_no_intent_is_retained() {
        // A clean flush (no retained intents) should advance the replay floor so
        // recovery need not replay older segments — but data must still survive,
        // because the committed data is durably in an SSTable by then.
        let commit = |engine: &LsmKvEngine, k: &str, v: &str, ts: Timestamp| {
            let txn = TxnId::new();
            engine
                .write_intent(txn, Bytes::from(k.to_owned()), Bytes::from(v.to_owned()))
                .unwrap();
            engine.commit(txn, ts).unwrap();
        };
        let dir = TempDir::new().unwrap();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            commit(&engine, "k1", "a", 10);
            engine.flush().unwrap(); // no open intents -> floor advances
            commit(&engine, "k2", "b", 20);
            engine.flush().unwrap();
        }
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(engine.get(b"k1", 100).unwrap(), Some(Bytes::from("a")));
        assert_eq!(engine.get(b"k2", 100).unwrap(), Some(Bytes::from("b")));
    }

    #[test]
    fn point_read_does_not_resurrect_a_deleted_row() {
        // Regression: `get` used to fall through to older sources whenever the
        // newer chain had no visible *value*, conflating "no version exists"
        // with "the newest visible version is a tombstone" — a delete followed
        // by a flush resurrected the old row until compaction hid it.
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();

        let put = TxnId::new();
        engine
            .write_intent(put, Bytes::from("k"), Bytes::from("v1"))
            .unwrap();
        engine.commit(put, 10).unwrap();
        engine.flush().unwrap();

        let delete = TxnId::new();
        engine.delete_intent(delete, Bytes::from("k")).unwrap();
        engine.commit(delete, 20).unwrap();

        // Before the tombstone is flushed: the memtable tombstone must hide the
        // SSTable value (and snapshot reads still see the old value).
        assert_eq!(engine.get(b"k", 25).unwrap(), None, "tombstone in memtable");
        assert_eq!(engine.get(b"k", 15).unwrap(), Some(Bytes::from("v1")));

        // After the flush: the newer SSTable's tombstone must hide the older
        // SSTable's value.
        engine.flush().unwrap();
        assert_eq!(engine.get(b"k", 25).unwrap(), None, "tombstone in sstable");
        assert_eq!(engine.get(b"k", 15).unwrap(), Some(Bytes::from("v1")));

        // After compaction (the merged chain keeps the tombstone; GC watermark
        // is 0, so nothing is reclaimed).
        engine.compact().unwrap();
        assert_eq!(engine.get(b"k", 25).unwrap(), None, "after compaction");
        assert_eq!(engine.get(b"k", 15).unwrap(), Some(Bytes::from("v1")));
        drop(engine);

        // After reopen: replay + SSTables must agree the key is deleted.
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(engine.get(b"k", 25).unwrap(), None, "after reopen");
        assert_eq!(engine.get(b"k", 15).unwrap(), Some(Bytes::from("v1")));
    }

    #[test]
    fn commit_survives_when_append_races_flush_floor_advance() {
        // Regression: an intent whose `WriteIntent` append completed but whose
        // memtable registration was still pending when a flush rotated the WAL
        // used to lose the replay floor: crash replay started at the new
        // segment, saw the `CommitTxn` without its intent, and dropped a
        // committed transaction. The pause hook makes the interleaving
        // deterministic: the writer is parked exactly in the append→register
        // window while the flush runs.
        let dir = TempDir::new().unwrap();
        let txn = TxnId::new();
        {
            let engine = Arc::new(LsmKvEngine::with_wal(dir.path(), None).unwrap());
            let floor_before = engine.min_replay_wal.load(Ordering::Relaxed);

            // A committed key so the flush has work to do.
            let stable = TxnId::new();
            engine
                .write_intent(stable, Bytes::from("stable"), Bytes::from("s"))
                .unwrap();
            engine.commit(stable, 10).unwrap();

            // Park the writer after its `WriteIntent` WAL append, before the
            // intent is registered.
            engine.arm_append_pause();
            let writer = {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    engine.write_intent(txn, Bytes::from("raced"), Bytes::from("v1"))
                })
            };
            engine.wait_for_parked_appends(1);

            // The flush cannot see the unregistered intent; it must still keep
            // the replay floor because the append is in flight.
            engine.flush().unwrap();
            assert_eq!(
                engine.min_replay_wal.load(Ordering::Relaxed),
                floor_before,
                "flush advanced the replay floor while a WAL append was in flight"
            );

            engine.release_parked_appends();
            writer.join().unwrap().unwrap();
            // The commit lands in the new active segment.
            engine.commit(txn, 30).unwrap();
            // Engine drops here, simulating a crash/restart (commit() fsynced).
        }

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"raced", 100).unwrap(),
            Some(Bytes::from("v1")),
            "committed value lost: replay dropped the commit whose intent \
             append raced the flush"
        );
        assert_eq!(engine.get(b"stable", 100).unwrap(), Some(Bytes::from("s")));
    }

    #[test]
    fn gc_keeps_the_memtable_tombstone_until_compaction() {
        // Regression: executor-driven GC (watermark past the delete) erased the
        // newest-visible tombstone from the memtable chain entirely. The empty
        // chain made `get` fall through to the SSTable below, resurrecting the
        // deleted row. Memtable GC must retain that tombstone; only compaction
        // — which merges every source — may reclaim it fully.
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let put = |key: &str, value: &str, ts: Timestamp| {
            let txn = TxnId::new();
            engine
                .write_intent(
                    txn,
                    Bytes::from(key.to_owned()),
                    Bytes::from(value.to_owned()),
                )
                .unwrap();
            engine.commit(txn, ts).unwrap();
        };

        // k@5 flushed to an SSTable; k@10 and the delete@20 stay in the memtable.
        put("k", "v1", 5);
        engine.flush().unwrap();
        put("k", "v2", 10);
        let delete = TxnId::new();
        engine
            .delete_intent(delete, Bytes::from_static(b"k"))
            .unwrap();
        engine.commit(delete, 20).unwrap();

        // GC at a watermark past the delete: k@10 goes, the tombstone must stay.
        engine.garbage_collect(20).unwrap();
        assert_eq!(
            engine.get(b"k", 30).unwrap(),
            None,
            "tombstone erased by GC: deleted row resurrected from the SSTable"
        );
        assert_eq!(
            engine.get(b"k", 6).unwrap(),
            Some(Bytes::from("v1")),
            "snapshot reads still see the SSTable value"
        );

        // Flush the tombstone into an SSTable, then compact. The compaction
        // merges ALL sources, so no older source remains that could resurrect
        // the key and the tombstone is reclaimed outright — this is what
        // bounds tombstone accumulation.
        engine.flush().unwrap();
        engine.compact().unwrap();
        assert_eq!(engine.get(b"k", 30).unwrap(), None, "after compaction");
        let sstables = engine.sstables.read().unwrap();
        assert_eq!(sstables.len(), 1, "the two SSTables merged into one");
        assert!(
            sstables[0].get(b"k").unwrap().is_none(),
            "compaction did not fully reclaim the deleted key"
        );
    }

    #[test]
    fn commit_cannot_fsync_into_an_unpublished_wal_segment() {
        // Regression: flush swapped the in-memory WAL to a new segment and only
        // then published the manifest naming it. A commit landing in that gap
        // appended+fsynced its `CommitTxn` into the new segment, which crash
        // recovery (walking the manifest's lineage) would never open — a
        // durable commit one manifest failure away from being lost after an
        // ack. Flush now holds the WAL write lock across the swap and the
        // manifest publication, so no append can reach a segment recovery does
        // not yet know about. The pause hook parks the flush exactly in that
        // window.
        let dir = TempDir::new().unwrap();
        let raced = TxnId::new();
        {
            let engine = Arc::new(LsmKvEngine::with_wal(dir.path(), None).unwrap());

            // A committed key so the flush has work to do, and an open intent
            // whose commit will race the flush.
            let stable = TxnId::new();
            engine
                .write_intent(stable, Bytes::from("stable"), Bytes::from("s"))
                .unwrap();
            engine.commit(stable, 10).unwrap();
            engine
                .write_intent(raced, Bytes::from("raced"), Bytes::from("v1"))
                .unwrap();

            // Park the flush after the WAL swap, before the manifest save.
            engine.arm_flush_pause();
            let flusher = {
                let engine = engine.clone();
                std::thread::spawn(move || engine.flush())
            };
            engine.wait_for_parked_flush(1);
            let unpublished = engine.active_wal_id.load(Ordering::Relaxed);

            // The racing commit must block on the WAL lock the parked flush
            // holds: it can neither be acknowledged nor leave a durable record
            // in the unpublished segment.
            let (tx, rx) = std::sync::mpsc::channel();
            let committer = {
                let engine = engine.clone();
                std::thread::spawn(move || {
                    let result = engine.commit(raced, 30);
                    let _ = tx.send(result);
                })
            };
            assert!(
                rx.recv_timeout(std::time::Duration::from_millis(200))
                    .is_err(),
                "commit acknowledged before the manifest named its WAL segment"
            );
            // Poll the unpublished segment: a `CommitTxn` frame appearing here
            // means a commit became durable where recovery cannot find it.
            let probe =
                FileWalEngine::with_encryption(dir.path().join(format!("{unpublished}.log")), None)
                    .unwrap();
            let mut leaked = false;
            for _ in 0..50 {
                let records = probe.recover().unwrap();
                if records
                    .iter()
                    .any(|r| matches!(r, WalRecord::V1(WalRecordV1::CommitTxn { .. })))
                {
                    leaked = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            assert!(
                !leaked,
                "commit fsynced into a WAL segment the manifest does not yet name"
            );

            engine.release_parked_flush();
            flusher.join().unwrap().unwrap();
            committer.join().unwrap();
            // The commit is acknowledged only after the manifest publication.
            rx.recv().unwrap().unwrap();
            // Engine drops here, simulating a crash/restart (commit() fsynced).
        }

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            engine.get(b"raced", 100).unwrap(),
            Some(Bytes::from("v1")),
            "acknowledged commit lost across crash recovery"
        );
        assert_eq!(engine.get(b"stable", 100).unwrap(), Some(Bytes::from("s")));
    }

    #[test]
    fn pending_intents_keep_their_age_across_restart() {
        // The janitor's TTL accounting must survive a restart: an intent's age
        // comes from the WAL record's HLC-scale write stamp, not process uptime.
        let dir = TempDir::new().unwrap();
        let txn = TxnId::new();
        let stamped = {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            engine
                .write_intent(txn, Bytes::from("orphan"), Bytes::from("v"))
                .unwrap();
            let listed = engine.pending_intents(b"").unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].txn_id, txn);
            listed[0].oldest_ts
            // Engine drops here, simulating a crash with the intent uncommitted.
        };

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let listed = engine.pending_intents(b"").unwrap();
        assert_eq!(listed.len(), 1, "the intent must survive crash recovery");
        assert_eq!(
            listed[0].oldest_ts, stamped,
            "the intent's write stamp must survive the restart"
        );
        assert!(listed[0].oldest_ts <= wallclock_micros());

        // Aborting the orphan unblocks a writer that was conflicting on it.
        let writer = TxnId::new();
        assert!(matches!(
            engine.write_intent(writer, Bytes::from("orphan"), Bytes::from("new")),
            Err(nodus_storage_api::KvError::WriteConflict(_))
        ));
        engine.abort(txn).unwrap();
        engine
            .write_intent(writer, Bytes::from("orphan"), Bytes::from("new"))
            .unwrap();
        engine.commit(writer, stamped + 1).unwrap();
        assert_eq!(
            engine.get(b"orphan", stamped + 1).unwrap(),
            Some(Bytes::from("new"))
        );
    }

    #[test]
    fn rejected_intent_batch_never_replays_or_overwrites_after_reopen() {
        let dir = TempDir::new().unwrap();
        let blocker = TxnId::new();
        let rejected = TxnId::new();
        let valid = TxnId::new();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            engine
                .write_intent(blocker, Bytes::from("blocked"), Bytes::from("old"))
                .unwrap();

            let result = engine.write_intent_batch(
                rejected,
                vec![
                    IntentOperation::Put {
                        key: Bytes::from("fresh"),
                        value: Bytes::from("rejected"),
                    },
                    IntentOperation::Delete {
                        key: Bytes::from("blocked"),
                    },
                ],
            );

            assert!(matches!(
                result,
                Err(nodus_storage_api::KvError::WriteConflict(id)) if id == rejected
            ));
            assert!(engine.pending_intent_keys(rejected).is_empty());
            assert!(
                !engine
                    .memtable
                    .read()
                    .unwrap()
                    .contains_key(b"fresh".as_slice())
            );
        }

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert!(engine.pending_intent_keys(rejected).is_empty());
        assert_eq!(
            engine.pending_intent_keys(blocker),
            vec![Bytes::from("blocked")]
        );
        assert_eq!(engine.get(b"fresh", 100).unwrap(), None);
        engine.abort(blocker).unwrap();
        engine
            .write_intent_batch(
                valid,
                vec![
                    IntentOperation::Put {
                        key: Bytes::from("fresh"),
                        value: Bytes::from("valid"),
                    },
                    IntentOperation::Put {
                        key: Bytes::from("blocked"),
                        value: Bytes::from("valid"),
                    },
                ],
            )
            .unwrap();
        engine.commit(valid, 20).unwrap();
        engine.commit(rejected, 30).unwrap();
        drop(engine);

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert!(engine.pending_intent_keys(rejected).is_empty());
        assert_eq!(
            engine.get(b"fresh", 100).unwrap(),
            Some(Bytes::from("valid"))
        );
        assert_eq!(
            engine.get(b"blocked", 100).unwrap(),
            Some(Bytes::from("valid"))
        );
    }

    #[test]
    fn intent_batch_same_txn_duplicate_keys_keep_last_operation() {
        let engine = LsmKvEngine::new();
        let txn = TxnId::new();
        engine
            .write_intent_batch(
                txn,
                vec![
                    IntentOperation::Put {
                        key: Bytes::from("key"),
                        value: Bytes::from("first"),
                    },
                    IntentOperation::Delete {
                        key: Bytes::from("key"),
                    },
                    IntentOperation::Put {
                        key: Bytes::from("key"),
                        value: Bytes::from("last"),
                    },
                ],
            )
            .unwrap();
        engine.commit(txn, 10).unwrap();

        assert_eq!(engine.get(b"key", 10).unwrap(), Some(Bytes::from("last")));
        assert_eq!(
            engine.memtable.read().unwrap()[b"key".as_slice()]
                .versions
                .len(),
            1
        );
    }

    #[test]
    fn commit_batch_recovers_all_transactions_after_intent_flushes() {
        let dir = TempDir::new().unwrap();
        let first = TxnId::new();
        let second = TxnId::new();
        {
            let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            engine.set_flush_threshold(1);
            for (txn, prefix) in [(first, "a"), (second, "b")] {
                engine
                    .write_intent_batch(
                        txn,
                        vec![
                            IntentOperation::Put {
                                key: Bytes::from(format!("{prefix}1")),
                                value: Bytes::from("1"),
                            },
                            IntentOperation::Put {
                                key: Bytes::from(format!("{prefix}2")),
                                value: Bytes::from("2"),
                            },
                        ],
                    )
                    .unwrap();
            }
            assert!(engine.active_wal_id.load(Ordering::Relaxed) > 1);
            engine
                .commit_batch(vec![(first, 10), (second, 20)])
                .unwrap();
            engine.commit_batch(Vec::new()).unwrap();
        }

        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(engine.get(b"a1", 10).unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get(b"a2", 10).unwrap(), Some(Bytes::from("2")));
        assert_eq!(engine.get(b"b1", 20).unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get(b"b2", 20).unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn telemetry_reports_real_storage_operations_and_skips_noops() {
        let dir = TempDir::new().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observer = events.clone();
        engine
            .set_telemetry_observer(move |event| observer.lock().unwrap().push(event))
            .unwrap();

        for (key, timestamp) in [("first", 10), ("second", 20)] {
            let txn = TxnId::new();
            engine
                .write_intent(txn, Bytes::from(key), Bytes::from("value"))
                .unwrap();
            engine.commit(txn, timestamp).unwrap();
            engine.flush().unwrap();
        }
        engine.compact().unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();

        let events = events.lock().unwrap();
        let flushes = events
            .iter()
            .filter(|event| matches!(event, LsmTelemetryEvent::Flush { .. }))
            .collect::<Vec<_>>();
        assert_eq!(flushes.len(), 2);
        assert!(
            flushes
                .iter()
                .all(|event| matches!(event, LsmTelemetryEvent::Flush { success: true, .. }))
        );
        let compactions = events
            .iter()
            .filter(|event| matches!(event, LsmTelemetryEvent::Compaction { .. }))
            .collect::<Vec<_>>();
        assert_eq!(compactions.len(), 1);
        assert!(matches!(
            compactions[0],
            LsmTelemetryEvent::Compaction { success: true, .. }
        ));
        let sstable_counts = events
            .iter()
            .filter_map(|event| match event {
                LsmTelemetryEvent::Sstables { count } => Some(*count),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sstable_counts, vec![0, 1, 2, 1]);
        assert!(events.iter().any(|event| matches!(
            event,
            LsmTelemetryEvent::MemtableBytes { bytes } if *bytes > 0
        )));
    }
}
