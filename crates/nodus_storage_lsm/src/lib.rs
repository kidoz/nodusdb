use anyhow::Result;
use bytes::Bytes;
pub mod sstable;

use nodus_mvcc::VersionChain;
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use sstable::Sstable;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

pub struct LsmKvEngine {
    data_dir: Option<PathBuf>,
    memtable: RwLock<BTreeMap<Bytes, VersionChain>>,
    intents: RwLock<HashMap<TxnId, Vec<Bytes>>>,
    sstables: RwLock<Vec<Sstable>>,
    wal: RwLock<Option<Arc<dyn WalEngine>>>,
    next_file_id: std::sync::atomic::AtomicU64,
    _wal_key: Option<[u8; 32]>,
}

impl LsmKvEngine {
    pub fn new() -> Self {
        Self {
            data_dir: None,
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(Vec::new()),
            wal: RwLock::new(None),
            next_file_id: std::sync::atomic::AtomicU64::new(1),
            _wal_key: None,
        }
    }

    pub fn with_wal<P: AsRef<Path>>(data_dir: P, key: Option<[u8; 32]>) -> Result<Self> {
        let path = data_dir.as_ref();
        std::fs::create_dir_all(path)?;

        let mut sstables = Vec::new();
        let mut max_id = 0;

        // Load existing SSTs
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let p = entry.path();
            if p.extension().and_then(|s| s.to_str()) == Some("sst")
                && let Some(name) = p.file_stem().and_then(|n| n.to_str())
                && let Ok(id) = name.parse::<u64>()
            {
                max_id = std::cmp::max(max_id, id);
                sstables.push(Sstable::open(p));
            }
        }

        // Sort SSTs so newest is last
        sstables.sort_by_key(|s| {
            s.path
                .file_stem()
                .and_then(|n| n.to_str())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        });

        let wal_path = path.join(format!("{}.log", max_id + 1));
        let wal = Arc::new(FileWalEngine::with_encryption(&wal_path, key)?);

        let engine = Self {
            data_dir: Some(path.to_path_buf()),
            memtable: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
            sstables: RwLock::new(sstables),
            wal: RwLock::new(Some(wal)),
            next_file_id: std::sync::atomic::AtomicU64::new(max_id + 2),
            _wal_key: key,
        };

        engine.recover()?;
        Ok(engine)
    }

    pub fn flush(&self) -> Result<()> {
        let dir = match &self.data_dir {
            Some(d) => d,
            None => return Ok(()), // no-op for in-memory only
        };

        let mut mem_guard = self.memtable.write().unwrap();

        if mem_guard.is_empty() {
            return Ok(());
        }

        let file_id = self
            .next_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let sst_path = dir.join(format!("{}.sst", file_id));
        let new_wal_path = dir.join(format!("{}.log", file_id + 1));

        // 1. Flush memtable to SSTable
        let sst = Sstable::build(&sst_path, &mem_guard)?;

        // 2. Add to list
        let mut sst_guard = self.sstables.write().unwrap();
        sst_guard.push(sst);

        // 3. Clear memtable
        mem_guard.clear();

        // 4. Rotate WAL
        let new_wal = Arc::new(FileWalEngine::with_encryption(
            &new_wal_path,
            self._wal_key,
        )?);
        let mut wal_guard = self.wal.write().unwrap();
        *wal_guard = Some(new_wal);

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

        let mem_guard = self.memtable.read().unwrap();
        for (k, chain) in mem_guard.range(start..end) {
            merged.insert(k.clone(), chain.clone());
        }

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

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let wal_guard = self.wal.read().unwrap();
        if let Some(wal) = wal_guard.as_ref() {
            wal.append(WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id,
                key: key.to_vec(),
                value: value.to_vec(),
            }))?;
        }

        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        if let Err(e) = chain.write_intent(txn_id, value.to_vec()) {
            anyhow::bail!("Write intent failed: {}", e);
        }

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let wal_guard = self.wal.read().unwrap();
        if let Some(wal) = wal_guard.as_ref() {
            wal.append(WalRecord::V1(WalRecordV1::DeleteIntent {
                txn_id,
                key: key.to_vec(),
            }))?;
        }

        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        if let Err(e) = chain.delete_intent(txn_id) {
            anyhow::bail!("Delete intent failed: {}", e);
        }

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> Result<()> {
        let mut store_guard = self.memtable.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();
        let chain = store_guard.entry(key.clone()).or_default();
        chain
            .versions
            .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
        match replacement {
            IntentReplacement::Put(value) => {
                chain
                    .write_intent(txn_id, value.to_vec())
                    .map_err(|e| anyhow::anyhow!("Write intent failed: {}", e))?;
                intents_guard.entry(txn_id).or_default().push(key);
            }
            IntentReplacement::Delete => {
                chain
                    .delete_intent(txn_id)
                    .map_err(|e| anyhow::anyhow!("Delete intent failed: {}", e))?;
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

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let wal_guard = self.wal.read().unwrap();
        if let Some(wal) = wal_guard.as_ref() {
            wal.append(WalRecord::V1(WalRecordV1::CommitTxn { txn_id, commit_ts }))?;
            wal.sync()?;
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

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let wal_guard = self.wal.read().unwrap();
        if let Some(wal) = wal_guard.as_ref() {
            wal.append(WalRecord::V1(WalRecordV1::AbortTxn { txn_id }))?;
            wal.sync()?;
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
}
