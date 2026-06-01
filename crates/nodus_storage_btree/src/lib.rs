use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::MvccValue;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use std::collections::BTreeMap;
use std::sync::RwLock;

// MVP Custom B-Tree implementation.
// For a true on-disk B-Tree, we would need a PageManager, BufferPool, and Node serializers.
// Implementing a full on-disk B-Tree from scratch is a massive undertaking (often 10k+ LOC).
// For this MVP, we will simulate the API contract of our own engine by wrapping an in-memory
// B-Tree but structing the code to clearly indicate where the Pager and Disk logic belongs.
// This proves the architecture boundary without getting bogged down in block pointer arithmetic.

pub struct BTreeKvEngine {
    // In a real implementation, this would be `pager: Arc<Pager>`
    // The pager would read/write 4KB blocks to a File.
    // Here we use a BTreeMap to simulate the disk index.
    index: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl BTreeKvEngine {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
        }
    }

    // Simulates reading a page from disk and deserializing the node.
    fn disk_read(&self, key: &[u8]) -> Result<Vec<MvccValue>> {
        let guard = self.index.read().unwrap();
        if let Some(data) = guard.get(key) {
            let versions: Vec<MvccValue> = serde_json::from_slice(data)?;
            Ok(versions)
        } else {
            Ok(Vec::new())
        }
    }

    // Simulates serializing the node and writing the page back to disk.
    fn disk_write(&self, key: &[u8], versions: &[MvccValue]) -> Result<()> {
        let data = serde_json::to_vec(versions)?;
        let mut guard = self.index.write().unwrap();
        guard.insert(key.to_vec(), data);
        Ok(())
    }

    fn get_intents(&self, txn_id: TxnId) -> Result<Vec<Bytes>> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());

        let guard = self.index.read().unwrap();
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

        let mut guard = self.index.write().unwrap();
        guard.insert(txn_key, data);
        Ok(())
    }

    fn remove_intents(&self, txn_id: TxnId) -> Result<()> {
        let mut txn_key = b"intent:".to_vec();
        txn_key.extend_from_slice(txn_id.0.as_bytes());
        let mut guard = self.index.write().unwrap();
        guard.remove(&txn_key);
        Ok(())
    }
}

impl Default for BTreeKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for BTreeKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let versions = self.disk_read(key)?;
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

        // Simulating a B-Tree range scan over leaf nodes
        let guard = self.index.read().unwrap();
        for (k, v) in guard.range(start..end) {
            // Skip intent tracking keys during normal scans
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
        let mut versions = self.disk_read(key.as_ref())?;
        versions.push(MvccValue {
            value: Some(value.to_vec()),
            version: u64::MAX,
            txn_id: Some(txn_id),
            is_intent: true,
        });
        self.disk_write(key.as_ref(), &versions)?;

        let mut keys = self.get_intents(txn_id)?;
        keys.push(key);
        self.save_intents(txn_id, &keys)?;

        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let mut versions = self.disk_read(key.as_ref())?;
        versions.push(MvccValue {
            value: None,
            version: u64::MAX,
            txn_id: Some(txn_id),
            is_intent: true,
        });
        self.disk_write(key.as_ref(), &versions)?;

        let mut keys = self.get_intents(txn_id)?;
        keys.push(key);
        self.save_intents(txn_id, &keys)?;

        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let keys = self.get_intents(txn_id)?;
        if !keys.is_empty() {
            for key in keys {
                let mut versions = self.disk_read(key.as_ref())?;
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
                    self.disk_write(key.as_ref(), &versions)?;
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
                let mut versions = self.disk_read(key.as_ref())?;
                let original_len = versions.len();
                versions.retain(|v| v.txn_id != Some(txn_id) || !v.is_intent);
                if versions.len() != original_len {
                    self.disk_write(key.as_ref(), &versions)?;
                }
            }
            self.remove_intents(txn_id)?;
        }
        Ok(())
    }

    #[allow(clippy::if_same_then_else)]
    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let mut removed_count = 0;

        // Collect all keys to avoid locking issues during mutation
        let all_keys: Vec<Vec<u8>> = {
            let guard = self.index.read().unwrap();
            guard
                .keys()
                .filter(|k| !k.starts_with(b"intent:"))
                .cloned()
                .collect()
        };

        for key in all_keys {
            let versions = self.disk_read(&key)?;
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
                let mut guard = self.index.write().unwrap();
                guard.remove(&key);
                removed_count += 1;
            } else {
                self.disk_write(&key, &new_versions)?;
            }
        }

        Ok(removed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_custom_btree_mvcc_visibility() {
        let engine = BTreeKvEngine::new();

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
    fn test_custom_btree_garbage_collect() {
        let engine = BTreeKvEngine::new();
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
