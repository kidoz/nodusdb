use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::MvccValue;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;

const MVCC_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("mvcc_data");
const INTENT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("intents");

pub struct RedbKvEngine {
    db: Arc<Database>,
}

impl RedbKvEngine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(MVCC_TABLE)?;
            let _ = write_txn.open_table(INTENT_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    fn get_versions(&self, table: &redb::Table<&[u8], &[u8]>, key: &[u8]) -> Result<Vec<MvccValue>> {
        if let Some(guard) = table.get(key)? {
            let data = guard.value();
            let versions: Vec<MvccValue> = serde_json::from_slice(data)?;
            Ok(versions)
        } else {
            Ok(Vec::new())
        }
    }

    fn save_versions(&self, table: &mut redb::Table<&[u8], &[u8]>, key: &[u8], versions: &Vec<MvccValue>) -> Result<()> {
        let data = serde_json::to_vec(versions)?;
        table.insert(key, data.as_slice())?;
        Ok(())
    }

    fn get_intents(&self, table: &redb::Table<&[u8], &[u8]>, txn_id: TxnId) -> Result<Vec<Bytes>> {
        let txn_key = txn_id.0.as_bytes();
        if let Some(guard) = table.get(txn_key.as_slice())? {
            let data = guard.value();
            let keys: Vec<Vec<u8>> = serde_json::from_slice(data)?;
            Ok(keys.into_iter().map(Bytes::from).collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn save_intents(&self, table: &mut redb::Table<&[u8], &[u8]>, txn_id: TxnId, keys: &Vec<Bytes>) -> Result<()> {
        let txn_key = txn_id.0.as_bytes();
        let raw_keys: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        let data = serde_json::to_vec(&raw_keys)?;
        table.insert(txn_key.as_slice(), data.as_slice())?;
        Ok(())
    }
}

impl KvEngine for RedbKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MVCC_TABLE)?;
        
        let versions = self.get_versions(&table, key)?;
        for v in versions.iter().rev() {
            if v.is_visible(read_ts) {
                return Ok(v.value.as_ref().map(|val| Bytes::from(val.clone())));
            }
        }
        Ok(None)
    }

    fn scan(&self, range: KeyRange, read_ts: Timestamp) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(MVCC_TABLE)?;

        let start = range.start.as_ref();
        let end = range.end.as_ref();
        
        let mut results = Vec::new();
        let mut iter = table.range(start..end)?;

        while let Some(res) = iter.next() {
            let (k_guard, v_guard) = res?;
            let k = k_guard.value();
            let data = v_guard.value();
            
            let versions: Vec<MvccValue> = serde_json::from_slice(data)?;
            for v in versions.iter().rev() {
                if v.is_visible(read_ts) {
                    if let Some(val) = &v.value {
                        results.push(Ok(KvPair {
                            key: Bytes::copy_from_slice(k),
                            value: Bytes::from(val.clone()),
                            version: v.version,
                        }));
                    }
                    break;
                }
            }
        }

        Ok(Box::new(results.into_iter()))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut mvcc_table = write_txn.open_table(MVCC_TABLE)?;
            let mut intent_table = write_txn.open_table(INTENT_TABLE)?;

            let mut versions = self.get_versions(&mvcc_table, key.as_ref())?;
            versions.push(MvccValue {
                value: Some(value.to_vec()),
                version: u64::MAX,
                txn_id: Some(txn_id),
                is_intent: true,
            });
            self.save_versions(&mut mvcc_table, key.as_ref(), &versions)?;

            let mut keys = self.get_intents(&intent_table, txn_id)?;
            keys.push(key);
            self.save_intents(&mut intent_table, txn_id, &keys)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut mvcc_table = write_txn.open_table(MVCC_TABLE)?;
            let mut intent_table = write_txn.open_table(INTENT_TABLE)?;

            let mut versions = self.get_versions(&mvcc_table, key.as_ref())?;
            versions.push(MvccValue {
                value: None,
                version: u64::MAX,
                txn_id: Some(txn_id),
                is_intent: true,
            });
            self.save_versions(&mut mvcc_table, key.as_ref(), &versions)?;

            let mut keys = self.get_intents(&intent_table, txn_id)?;
            keys.push(key);
            self.save_intents(&mut intent_table, txn_id, &keys)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut mvcc_table = write_txn.open_table(MVCC_TABLE)?;
            let mut intent_table = write_txn.open_table(INTENT_TABLE)?;

            let keys = self.get_intents(&intent_table, txn_id)?;
            if !keys.is_empty() {
                for key in keys {
                    let mut versions = self.get_versions(&mvcc_table, key.as_ref())?;
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
                        self.save_versions(&mut mvcc_table, key.as_ref(), &versions)?;
                    }
                }
                intent_table.remove(txn_id.0.as_bytes().as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut mvcc_table = write_txn.open_table(MVCC_TABLE)?;
            let mut intent_table = write_txn.open_table(INTENT_TABLE)?;

            let keys = self.get_intents(&intent_table, txn_id)?;
            if !keys.is_empty() {
                for key in keys {
                    let mut versions = self.get_versions(&mvcc_table, key.as_ref())?;
                    let original_len = versions.len();
                    versions.retain(|v| v.txn_id != Some(txn_id) || !v.is_intent);
                    if versions.len() != original_len {
                        self.save_versions(&mut mvcc_table, key.as_ref(), &versions)?;
                    }
                }
                intent_table.remove(txn_id.0.as_bytes().as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let write_txn = self.db.begin_write()?;
        let mut removed_count = 0;
        {
            let mut mvcc_table = write_txn.open_table(MVCC_TABLE)?;
            
            // Note: In Redb we have to collect keys first or use a cursor safely.
            // For MVP, we'll collect all keys, then mutate.
            let mut all_keys = Vec::new();
            {
                let mut iter = mvcc_table.range::<&[u8]>(..)?;
                while let Some(res) = iter.next() {
                    let (k_guard, _) = res?;
                    all_keys.push(k_guard.value().to_vec());
                }
            }

            for key in all_keys {
                let mut versions = self.get_versions(&mvcc_table, &key)?;
                
                // Keep all intents, and the latest committed version before the watermark.
                let mut new_versions = Vec::new();
                let mut found_committed_before_watermark = false;

                // Process backwards from latest
                for v in versions.into_iter().rev() {
                    if v.is_intent {
                        new_versions.push(v);
                    } else if v.version > watermark {
                        new_versions.push(v);
                    } else if !found_committed_before_watermark {
                        new_versions.push(v);
                        found_committed_before_watermark = true;
                    } else {
                        removed_count += 1;
                    }
                }

                new_versions.reverse();

                // If the only remaining version is a tombstone before the watermark, we can delete the key entirely
                if new_versions.len() == 1 && !new_versions[0].is_intent && new_versions[0].version <= watermark && new_versions[0].value.is_none() {
                    mvcc_table.remove(key.as_slice())?;
                    removed_count += 1;
                } else {
                    self.save_versions(&mut mvcc_table, &key, &new_versions)?;
                }
            }
        }
        write_txn.commit()?;
        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_redb_mvcc_visibility() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let engine = RedbKvEngine::new(&db_path).unwrap();

        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        
        let txn = TxnId::new();
        engine.write_intent(txn, k1.clone(), v1.clone()).unwrap();
        
        // Cannot read intent before commit
        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert!(res.is_none());

        engine.commit(txn, 10).unwrap();

        // Visible after commit at correct timestamp
        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert_eq!(res.unwrap(), v1);

        // Not visible at older timestamp
        let res = engine.get(k1.as_ref(), 9).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_garbage_collect_prunes_old_versions() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_gc.redb");
        let engine = RedbKvEngine::new(&db_path).unwrap();
        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        let v2 = Bytes::from("v2");
        
        let txn1 = TxnId::new();
        let txn2 = TxnId::new();

        engine.write_intent(txn1, k1.clone(), v1.clone()).unwrap();
        engine.commit(txn1, 10).unwrap();
        engine.write_intent(txn2, k1.clone(), v2.clone()).unwrap();
        engine.commit(txn2, 20).unwrap();

        // Before GC, both reads work
        assert_eq!(engine.get(k1.as_ref(), 15).unwrap().unwrap(), v1);
        assert_eq!(engine.get(k1.as_ref(), 25).unwrap().unwrap(), v2);

        // GC at timestamp 15
        let removed = engine.garbage_collect(15).unwrap();
        assert_eq!(removed, 0); // Both needed: v1 is the latest before 15, v2 is > 15.

        // GC at timestamp 25
        let removed = engine.garbage_collect(25).unwrap();
        assert_eq!(removed, 1); // v1 can be pruned.

        // Older read fails now, latest works
        assert!(engine.get(k1.as_ref(), 15).unwrap().is_none());
        assert_eq!(engine.get(k1.as_ref(), 25).unwrap().unwrap(), v2);
    }
}
