use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::MvccValue;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use std::collections::BTreeMap;
use std::sync::RwLock;

// MVP Custom LSM-Tree implementation.
// For a true on-disk LSM, we would need:
// 1. MemTable (in-memory, fast writes)
// 2. WAL (Write-Ahead Log, for crash recovery of the MemTable)
// 3. SSTables (immutable files on disk, organized in levels)
// 4. Compaction (background threads merging SSTables)
//
// For this MVP, we simulate the layers to outline the architecture
// while storing the data in memory. The logic clearly indicates
// where disk flush and compaction boundaries reside.

pub struct LsmKvEngine {
    // Represents the active, mutable in-memory table.
    memtable: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    // In a real implementation, we would have `sstables: RwLock<Vec<SSTable>>`
    // where an SSTable points to a file on disk.
}

impl LsmKvEngine {
    pub fn new() -> Self {
        Self {
            memtable: RwLock::new(BTreeMap::new()),
        }
    }

    // Simulates an LSM read. It would normally check MemTable -> L0 -> L1 -> etc.
    fn lsm_read(&self, key: &[u8]) -> Result<Vec<MvccValue>> {
        let guard = self.memtable.read().unwrap();
        // 1. Check MemTable
        if let Some(data) = guard.get(key) {
            let versions: Vec<MvccValue> = serde_json::from_slice(data)?;
            return Ok(versions);
        }
        // 2. In reality, check Immutable MemTables, then SSTables
        Ok(Vec::new())
    }

    // Simulates writing to the WAL and MemTable.
    fn lsm_write(&self, key: &[u8], versions: &[MvccValue]) -> Result<()> {
        let data = serde_json::to_vec(versions)?;
        // 1. Write to WAL (omitted for MVP simulation)
        // 2. Write to MemTable
        let mut guard = self.memtable.write().unwrap();
        guard.insert(key.to_vec(), data);

        // 3. Check if MemTable is full, triggering flush to SSTable
        Ok(())
    }

    fn get_intents(&self, txn_id: TxnId) -> Result<Vec<Bytes>> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());

        let guard = self.memtable.read().unwrap();
        if let Some(data) = guard.get(&txn_key) {
            let keys: Vec<Vec<u8>> = serde_json::from_slice(data)?;
            Ok(keys.into_iter().map(Bytes::from).collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn save_intents(&self, txn_id: TxnId, keys: &[Bytes]) -> Result<()> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());

        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        let data = serde_json::to_vec(&raw_keys)?;

        let mut guard = self.memtable.write().unwrap();
        guard.insert(txn_key, data);
        Ok(())
    }

    fn remove_intents(&self, txn_id: TxnId) -> Result<()> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());
        let mut guard = self.memtable.write().unwrap();
        guard.remove(&txn_key);
        Ok(())
    }
}

impl Default for LsmKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for LsmKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let versions = self.lsm_read(key)?;
        for v in versions.iter().rev() {
            if v.is_visible(read_ts) {
                return Ok(v.value.as_ref().map(|val| Bytes::from(val.clone())));
            }
        }
        Ok(None)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();

        let mut results = Vec::new();

        // Simulating an LSM Iterator that merges MemTable and SSTables.
        let guard = self.memtable.read().unwrap();
        for (k, v) in guard.range(start..end) {
            if k.starts_with(b"intent:") {
                continue;
            }

            let versions: Vec<MvccValue> = serde_json::from_slice(v)?;
            for version in versions.iter().rev() {
                if version.is_visible(read_ts) {
                    if let Some(val) = &version.value {
                        results.push(Ok(KvPair {
                            key: Bytes::copy_from_slice(k),
                            value: Bytes::from(val.clone()),
                            version: version.version,
                        }));
                    }
                    break;
                }
            }
        }

        Ok(Box::new(results.into_iter()))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let mut versions = self.lsm_read(key.as_ref())?;
        versions.push(MvccValue {
            value: Some(value.to_vec()),
            version: u64::MAX,
            txn_id: Some(txn_id),
            is_intent: true,
        });
        self.lsm_write(key.as_ref(), &versions)?;

        let mut keys = self.get_intents(txn_id)?;
        keys.push(key);
        self.save_intents(txn_id, &keys)?;

        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let mut versions = self.lsm_read(key.as_ref())?;
        versions.push(MvccValue {
            value: None,
            version: u64::MAX,
            txn_id: Some(txn_id),
            is_intent: true,
        });
        self.lsm_write(key.as_ref(), &versions)?;

        let mut keys = self.get_intents(txn_id)?;
        keys.push(key);
        self.save_intents(txn_id, &keys)?;

        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let keys = self.get_intents(txn_id)?;
        if !keys.is_empty() {
            for key in keys {
                let mut versions = self.lsm_read(key.as_ref())?;
                let mut modified = false;
                for v in versions.iter_mut() {
                    if v.txn_id == Some(txn_id) && v.is_intent {
                        v.is_intent = false;
                        v.version = commit_ts;
                        modified = true;
                    }
                }
                if modified {
                    versions.sort_by_key(|v| v.version);
                    self.lsm_write(key.as_ref(), &versions)?;
                }
            }
            self.remove_intents(txn_id)?;
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let keys = self.get_intents(txn_id)?;
        if !keys.is_empty() {
            for key in keys {
                let mut versions = self.lsm_read(key.as_ref())?;
                let original_len = versions.len();
                versions.retain(|v| v.txn_id != Some(txn_id) || !v.is_intent);
                if versions.len() != original_len {
                    self.lsm_write(key.as_ref(), &versions)?;
                }
            }
            self.remove_intents(txn_id)?;
        }
        Ok(())
    }

    #[allow(clippy::if_same_then_else)]
    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let mut removed_count = 0;

        let all_keys: Vec<Vec<u8>> = {
            let guard = self.memtable.read().unwrap();
            guard
                .keys()
                .filter(|k| !k.starts_with(b"intent:"))
                .cloned()
                .collect()
        };

        // In a true LSM, GC occurs during the background Compaction phase!
        // When merging SSTables, it drops versions older than the watermark.
        // For MVP, we rewrite the keys manually.
        for key in all_keys {
            let versions = self.lsm_read(&key)?;
            let mut new_versions = Vec::new();
            let mut found_committed_before_watermark = false;

            for v in versions.into_iter().rev() {
                if v.is_intent || v.version > watermark || !found_committed_before_watermark {
                    if !v.is_intent && v.version <= watermark {
                        found_committed_before_watermark = true;
                    }
                    new_versions.push(v);
                } else {
                    removed_count += 1;
                }
            }

            new_versions.reverse();

            if new_versions.len() == 1
                && !new_versions[0].is_intent
                && new_versions[0].version <= watermark
                && new_versions[0].value.is_none()
            {
                let mut guard = self.memtable.write().unwrap();
                guard.remove(&key);
                removed_count += 1;
            } else {
                self.lsm_write(&key, &new_versions)?;
            }
        }

        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
