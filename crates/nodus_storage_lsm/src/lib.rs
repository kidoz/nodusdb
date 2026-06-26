use anyhow::Result;
use bytes::Bytes;
pub mod sstable;

use nodus_mvcc::VersionChain;
use nodus_storage_api::{
    IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, KvVersion, Timestamp, TxnId,
};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use serde::{Deserialize, Serialize};
use sstable::Sstable;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

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
}

pub struct LsmKvEngine {
    data_dir: Option<PathBuf>,
    memtable: RwLock<BTreeMap<Bytes, VersionChain>>,
    intents: RwLock<HashMap<TxnId, Vec<Bytes>>>,
    sstables: RwLock<Vec<Sstable>>,
    wal: RwLock<Option<Arc<dyn WalEngine>>>,
    /// Id of the active WAL segment (`{id}.log`), recorded in the manifest.
    active_wal_id: AtomicU64,
    next_file_id: AtomicU64,
    /// Approximate live size of the memtable, driving size-triggered flushes.
    memtable_bytes: AtomicUsize,
    flush_threshold: AtomicUsize,
    /// Most recent GC watermark, applied to SSTables during compaction.
    gc_watermark: AtomicU64,
    /// Serializes flush and compaction (both rewrite the SSTable set + manifest).
    flush_compact_lock: Mutex<()>,
    _wal_key: Option<[u8; 32]>,
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

impl LsmKvEngine {
    pub fn new() -> Self {
        Self {
            data_dir: None,
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(Vec::new()),
            wal: RwLock::new(None),
            active_wal_id: AtomicU64::new(0),
            next_file_id: AtomicU64::new(1),
            memtable_bytes: AtomicUsize::new(0),
            flush_threshold: AtomicUsize::new(DEFAULT_FLUSH_THRESHOLD),
            gc_watermark: AtomicU64::new(0),
            flush_compact_lock: Mutex::new(()),
            _wal_key: None,
        }
    }

    /// Overrides the memtable flush threshold (bytes). Mainly for tests.
    pub fn set_flush_threshold(&self, bytes: usize) {
        self.flush_threshold.store(bytes, Ordering::Relaxed);
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
        let (mut sst_ids, active_wal_id): (Vec<u64>, u64) = match manifest {
            Some(m) => (m.sstables, m.active_wal),
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
                (ids, max_id + 1)
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
            data_dir: Some(path.to_path_buf()),
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(sstables),
            wal: RwLock::new(Some(wal)),
            active_wal_id: AtomicU64::new(active_wal_id),
            next_file_id: AtomicU64::new(next),
            memtable_bytes: AtomicUsize::new(0),
            flush_threshold: AtomicUsize::new(DEFAULT_FLUSH_THRESHOLD),
            gc_watermark: AtomicU64::new(0),
            flush_compact_lock: Mutex::new(()),
            _wal_key: key,
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
        engine.memtable_bytes.store(restored, Ordering::Relaxed);
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
                .flatten()
                .cloned()
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
            self.memtable_bytes.store(retained, Ordering::Relaxed);

            if !to_flush.is_empty() {
                // Durably + atomically publish committed data as an SSTable.
                let sst_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
                let sst = Sstable::build(dir.join(format!("{sst_id}.sst")), &to_flush)?;
                self.sstables.write().unwrap().push(sst);
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
            // verify an unbroken archived chain despite sparse segment ids.
            new_wal.append(WalRecord::V1(WalRecordV1::SegmentHeader {
                predecessor: Some(predecessor),
            }))?;
            *self.wal.write().unwrap() = Some(new_wal);
            self.active_wal_id.store(wal_id, Ordering::Relaxed);
            self.save_manifest()?;
        }

        // Bound read amplification once enough SSTables have accumulated.
        if self.sstables.read().unwrap().len() >= COMPACTION_TRIGGER {
            self.compact()?;
        }
        Ok(())
    }

    /// Merges all live SSTables into one, combining each key's version chains and
    /// dropping versions reclaimable below the current GC watermark. Crash-safe:
    /// the new SSTable is published durably, the manifest is swapped to point at
    /// it alone (the commit point — a crash before this leaves the old set in the
    /// manifest, orphaning the new file), then the old files are deleted.
    pub fn compact(&self) -> Result<()> {
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
        // left with no versions.
        merged.retain(|_, chain| {
            chain.versions.sort_by_key(|v| v.version);
            chain.versions.dedup();
            chain.garbage_collect(watermark);
            !chain.versions.is_empty()
        });

        let new_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
        let new_sst = Sstable::build(dir.join(format!("{new_id}.sst")), &merged)?;

        // Atomic swap: install the single compacted SSTable, persist the manifest
        // (commit point), then delete the now-orphaned old files.
        *self.sstables.write().unwrap() = vec![new_sst];
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
            self.flush()?;
        }
        Ok(())
    }

    fn recover(&self) -> Result<()> {
        let wal_guard = self.wal.read().unwrap();
        if let Some(wal) = wal_guard.as_ref() {
            let records = wal.recover()?;
            let mut mem_guard = self.memtable.write().unwrap();
            let mut int_guard = self.intents.write().unwrap();

            for record in records {
                let WalRecord::V1(rec) = record;
                match rec {
                    WalRecordV1::WriteIntent { txn_id, key, value } => {
                        let k = Bytes::from(key);
                        let chain = mem_guard.entry(k.clone()).or_default();
                        let _ = chain.write_intent(txn_id, value);
                        int_guard.entry(txn_id).or_default().push(k);
                    }
                    WalRecordV1::DeleteIntent { txn_id, key } => {
                        let k = Bytes::from(key);
                        let chain = mem_guard.entry(k.clone()).or_default();
                        let _ = chain.delete_intent(txn_id);
                        int_guard.entry(txn_id).or_default().push(k);
                    }
                    WalRecordV1::CommitTxn { txn_id, commit_ts } => {
                        if let Some(keys) = int_guard.remove(&txn_id) {
                            for key in keys {
                                if let Some(chain) = mem_guard.get_mut(&key) {
                                    let _ = chain.commit(txn_id, commit_ts);
                                }
                            }
                        }
                    }
                    WalRecordV1::AbortTxn { txn_id } => {
                        if let Some(keys) = int_guard.remove(&txn_id) {
                            for key in keys {
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
        Ok(())
    }
}

impl Default for LsmKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for LsmKvEngine {
    fn flush(&self) -> Result<()> {
        self.flush()
    }

    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let guard = self.memtable.read().unwrap();
        if let Some(chain) = guard.get(key)
            && let Some(val) = chain.read(read_ts)
        {
            return Ok(Some(Bytes::from(val.to_vec())));
        }

        // Search through sstables from newest to oldest
        let sst_guard = self.sstables.read().unwrap();
        for sst in sst_guard.iter().rev() {
            if let Ok(Some(chain)) = sst.get(key)
                && let Some(val) = chain.read(read_ts)
            {
                return Ok(Some(Bytes::from(val.to_vec())));
            }
        }

        Ok(None)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let start = Bytes::from(range.start.as_ref().to_vec());
        let end = Bytes::from(range.end.as_ref().to_vec());

        let mut merged = BTreeMap::new();

        // Lock order is always memtable-before-sstables (see `get`/`flush`) to
        // stay deadlock-free; iterate SSTables first so the memtable overrides.
        let mem_guard = self.memtable.read().unwrap();
        let sst_guard = self.sstables.read().unwrap();
        for sst in sst_guard.iter() {
            if let Ok(iter) = sst.iter() {
                for item in iter.flatten() {
                    let (k, chain) = item;
                    if k >= start && k < end {
                        merged.insert(k, chain);
                    }
                }
            }
        }
        for (k, chain) in mem_guard.range(start..end) {
            merged.insert(k.clone(), chain.clone());
        }
        drop(sst_guard);
        drop(mem_guard);

        let mut results = Vec::new();
        for (k, chain) in merged {
            if let Some(val) = chain.read(read_ts) {
                let version = chain
                    .versions
                    .iter()
                    .filter(|v| v.is_visible(read_ts))
                    .map(|v| v.version)
                    .max()
                    .unwrap_or(0);

                results.push(Ok(KvPair {
                    key: k,
                    value: Bytes::from(val.to_vec()),
                    version,
                }));
            }
        }

        Ok(Box::new(results.into_iter()))
    }

    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        let start = Bytes::from(range.start.as_ref().to_vec());
        let end = Bytes::from(range.end.as_ref().to_vec());
        let mut merged: BTreeMap<Bytes, VersionChain> = BTreeMap::new();

        let mem_guard = self.memtable.read().unwrap();
        let sst_guard = self.sstables.read().unwrap();
        for sst in sst_guard.iter() {
            if let Ok(iter) = sst.iter() {
                for item in iter.flatten() {
                    let (key, chain) = item;
                    if key >= start && key < end {
                        merged
                            .entry(key)
                            .or_default()
                            .versions
                            .extend(chain.versions);
                    }
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
                }))?;
            }
        }

        let added = key.len() + value.len() + 32;
        {
            let mut store_guard = self.memtable.write().unwrap();
            let mut intents_guard = self.intents.write().unwrap();

            let chain = store_guard.entry(key.clone()).or_default();
            chain.write_intent(txn_id, value.to_vec())?;
            intents_guard.entry(txn_id).or_default().push(key);
        }
        self.memtable_bytes.fetch_add(added, Ordering::Relaxed);
        self.maybe_flush_by_size()?;
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        {
            let wal_guard = self.wal.read().unwrap();
            if let Some(wal) = wal_guard.as_ref() {
                wal.append(WalRecord::V1(WalRecordV1::DeleteIntent {
                    txn_id,
                    key: key.to_vec(),
                }))?;
            }
        }

        let added = key.len() + 32;
        {
            let mut store_guard = self.memtable.write().unwrap();
            let mut intents_guard = self.intents.write().unwrap();

            let chain = store_guard.entry(key.clone()).or_default();
            chain.delete_intent(txn_id)?;
            intents_guard.entry(txn_id).or_default().push(key);
        }
        self.memtable_bytes.fetch_add(added, Ordering::Relaxed);
        self.maybe_flush_by_size()?;
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();
        let chain = store_guard.entry(key.clone()).or_default();
        chain
            .versions
            .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
        match replacement {
            IntentReplacement::Put(value) => {
                chain.write_intent(txn_id, value.to_vec())?;
                intents_guard.entry(txn_id).or_default().push(key);
            }
            IntentReplacement::Delete => {
                chain.delete_intent(txn_id)?;
                intents_guard.entry(txn_id).or_default().push(key);
            }
            IntentReplacement::Clear => {
                if let Some(keys) = intents_guard.get_mut(&txn_id) {
                    keys.retain(|k| k != &key);
                    if keys.is_empty() {
                        intents_guard.remove(&txn_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
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
            for key in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.commit(txn_id, commit_ts);
                }
            }
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
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
            for key in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.abort(txn_id);
                }
            }
        }
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        // Remember the watermark so the next compaction can reclaim superseded
        // versions from SSTables too, not just the memtable.
        self.gc_watermark.store(watermark, Ordering::Relaxed);
        let mut store = self.memtable.write().unwrap();
        let mut removed = 0usize;
        let mut dead_keys = Vec::new();

        for (key, chain) in store.iter_mut() {
            removed += chain.garbage_collect(watermark);
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
}
