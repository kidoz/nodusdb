use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::MvccValue;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct LsmKvEngine {
    memtable: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    wal: Option<Arc<dyn WalEngine>>,
}

impl LsmKvEngine {
    pub fn new() -> Self {
        Self {
            memtable: RwLock::new(BTreeMap::new()),
            wal: None,
        }
    }

    pub fn with_wal<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let path = data_dir.as_ref();
        std::fs::create_dir_all(path)?;
        let wal_path = path.join("wal.log");
        let wal = Arc::new(FileWalEngine::new(&wal_path)?);

        let engine = Self {
            memtable: RwLock::new(BTreeMap::new()),
            wal: Some(wal),
        };

        engine.recover()?;
        Ok(engine)
    }

    fn recover(&self) -> Result<()> {
        if let Some(wal) = &self.wal {
            let records = wal.recover()?;
            let mut guard = self.memtable.write().unwrap();

            for record in records {
                let WalRecord::V1(rec) = record;
                match rec {
                    WalRecordV1::WriteIntent { txn_id, key, value } => {
                        let mut versions = Self::read_versions_from_map(&guard, &key)?;
                        versions.push(MvccValue {
                            value: Some(value),
                            version: u64::MAX,
                            txn_id: Some(txn_id),
                            is_intent: true,
                        });
                        Self::write_versions_to_map(&mut guard, &key, &versions)?;

                        let mut txn_key = b"intent:".to_vec();
                        txn_key.extend_from_slice(txn_id.0.as_bytes());
                        let mut keys = Self::read_intents_from_map(&guard, &txn_key)?;
                        keys.push(key);
                        Self::write_intents_to_map(&mut guard, &txn_key, &keys)?;
                    }
                    WalRecordV1::DeleteIntent { txn_id, key } => {
                        let mut versions = Self::read_versions_from_map(&guard, &key)?;
                        versions.push(MvccValue {
                            value: None,
                            version: u64::MAX,
                            txn_id: Some(txn_id),
                            is_intent: true,
                        });
                        Self::write_versions_to_map(&mut guard, &key, &versions)?;

                        let mut txn_key = b"intent:".to_vec();
                        txn_key.extend_from_slice(txn_id.0.as_bytes());
                        let mut keys = Self::read_intents_from_map(&guard, &txn_key)?;
                        keys.push(key);
                        Self::write_intents_to_map(&mut guard, &txn_key, &keys)?;
                    }
                    WalRecordV1::CommitTxn { txn_id, commit_ts } => {
                        let mut txn_key = b"intent:".to_vec();
                        txn_key.extend_from_slice(txn_id.0.as_bytes());
                        let keys = Self::read_intents_from_map(&guard, &txn_key)?;
                        for key in keys {
                            let mut versions = Self::read_versions_from_map(&guard, &key)?;
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
                                Self::write_versions_to_map(&mut guard, &key, &versions)?;
                            }
                        }
                        guard.remove(&txn_key);
                    }
                    WalRecordV1::AbortTxn { txn_id } => {
                        let mut txn_key = b"intent:".to_vec();
                        txn_key.extend_from_slice(txn_id.0.as_bytes());
                        let keys = Self::read_intents_from_map(&guard, &txn_key)?;
                        for key in keys {
                            let mut versions = Self::read_versions_from_map(&guard, &key)?;
                            let original_len = versions.len();
                            versions.retain(|v| v.txn_id != Some(txn_id) || !v.is_intent);
                            if versions.len() != original_len {
                                Self::write_versions_to_map(&mut guard, &key, &versions)?;
                            }
                        }
                        guard.remove(&txn_key);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn read_versions_from_map(
        map: &BTreeMap<Vec<u8>, Vec<u8>>,
        key: &[u8],
    ) -> Result<Vec<MvccValue>> {
        if let Some(data) = map.get(key) {
            let versions: Vec<MvccValue> = serde_json::from_slice(data)?;
            Ok(versions)
        } else {
            Ok(Vec::new())
        }
    }

    fn write_versions_to_map(
        map: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        key: &[u8],
        versions: &[MvccValue],
    ) -> Result<()> {
        let data = serde_json::to_vec(versions)?;
        map.insert(key.to_vec(), data);
        Ok(())
    }

    fn read_intents_from_map(
        map: &BTreeMap<Vec<u8>, Vec<u8>>,
        txn_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        if let Some(data) = map.get(txn_key) {
            let keys: Vec<Vec<u8>> = serde_json::from_slice(data)?;
            Ok(keys)
        } else {
            Ok(Vec::new())
        }
    }

    fn write_intents_to_map(
        map: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        txn_key: &[u8],
        keys: &[Vec<u8>],
    ) -> Result<()> {
        let data = serde_json::to_vec(keys)?;
        map.insert(txn_key.to_vec(), data);
        Ok(())
    }

    fn lsm_read(&self, key: &[u8]) -> Result<Vec<MvccValue>> {
        let guard = self.memtable.read().unwrap();
        Self::read_versions_from_map(&guard, key)
    }

    fn lsm_write(&self, key: &[u8], versions: &[MvccValue]) -> Result<()> {
        let mut guard = self.memtable.write().unwrap();
        Self::write_versions_to_map(&mut guard, key, versions)
    }

    fn get_intents(&self, txn_id: TxnId) -> Result<Vec<Bytes>> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());

        let guard = self.memtable.read().unwrap();
        let keys = Self::read_intents_from_map(&guard, &txn_key)?;
        Ok(keys.into_iter().map(Bytes::from).collect())
    }

    fn save_intents(&self, txn_id: TxnId, keys: &[Bytes]) -> Result<()> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());

        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        let mut guard = self.memtable.write().unwrap();
        Self::write_intents_to_map(&mut guard, &txn_key, &raw_keys)
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
        if let Some(wal) = &self.wal {
            wal.append(WalRecord::V1(WalRecordV1::WriteIntent {
                txn_id,
                key: key.to_vec(),
                value: value.to_vec(),
            }))?;
        }

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
        if let Some(wal) = &self.wal {
            wal.append(WalRecord::V1(WalRecordV1::DeleteIntent {
                txn_id,
                key: key.to_vec(),
            }))?;
        }

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
        if let Some(wal) = &self.wal {
            wal.append(WalRecord::V1(WalRecordV1::CommitTxn { txn_id, commit_ts }))?;
            wal.sync()?;
        }

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
        if let Some(wal) = &self.wal {
            wal.append(WalRecord::V1(WalRecordV1::AbortTxn { txn_id }))?;
            wal.sync()?;
        }

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
            let engine = LsmKvEngine::with_wal(temp_dir.path()).unwrap();
            engine.write_intent(txn1, k1.clone(), v1.clone()).unwrap();
            engine.commit(txn1, 10).unwrap();
        } // Engine drops, releasing file locks

        // Re-instantiate against same directory should recover MemTable from WAL
        let recovered_engine = LsmKvEngine::with_wal(temp_dir.path()).unwrap();
        let res = recovered_engine.get(k1.as_ref(), 15).unwrap();
        assert_eq!(res.unwrap(), v1);
    }
}
