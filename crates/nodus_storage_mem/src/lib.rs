use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::MvccValue;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

pub struct MemKvEngine {
    // simplified: key -> array of values sorted by version desc
    store: RwLock<BTreeMap<Bytes, Vec<MvccValue>>>,
    intents: RwLock<HashMap<TxnId, Vec<Bytes>>>,
}

impl MemKvEngine {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for MemKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let guard = self.store.read().unwrap();
        if let Some(versions) = guard.get(key) {
            for v in versions.iter().rev() {
                if v.is_visible(read_ts) {
                    return Ok(v.value.as_ref().map(|val| Bytes::from(val.clone())));
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
        let guard = self.store.read().unwrap();
        let mut results = Vec::new();

        for (k, versions) in guard.range(range.start..range.end) {
            for v in versions.iter().rev() {
                if v.is_visible(read_ts) {
                    if let Some(val) = &v.value {
                        results.push(Ok(KvPair {
                            key: k.clone(),
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
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let val = MvccValue {
            value: Some(value.to_vec()),
            version: u64::MAX, // max version during intent phase
            txn_id: Some(txn_id),
            is_intent: true,
        };

        store_guard.entry(key.clone()).or_default().push(val);
        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for key in keys {
                if let Some(versions) = store_guard.get_mut(&key) {
                    for v in versions.iter_mut() {
                        if v.txn_id == Some(txn_id) && v.is_intent {
                            v.is_intent = false;
                            v.version = commit_ts;
                        }
                    }
                    // Re-sort just in case (though they should naturally be in order mostly)
                    versions.sort_by_key(|v| v.version);
                }
            }
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for key in keys {
                if let Some(versions) = store_guard.get_mut(&key) {
                    versions.retain(|v| v.txn_id != Some(txn_id) || !v.is_intent);
                }
            }
        }
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let mut store = self.store.write().unwrap();
        let mut removed = 0usize;
        let mut dead_keys = Vec::new();

        for (key, versions) in store.iter_mut() {
            // Newest committed version visible at or below the watermark: this is
            // the oldest version any reader at >= watermark could still need.
            let keep_version = versions
                .iter()
                .filter(|v| !v.is_intent && v.version <= watermark)
                .map(|v| v.version)
                .max();
            let Some(keep_version) = keep_version else {
                continue;
            };

            let before = versions.len();
            // Keep intents and everything at or newer than keep_version.
            versions.retain(|v| v.is_intent || v.version >= keep_version);
            removed += before - versions.len();

            // If the only survivor is a tombstone at/below the watermark, the key
            // is dead and can be dropped entirely.
            if versions.len() == 1 {
                let v = &versions[0];
                if !v.is_intent && v.value.is_none() {
                    dead_keys.push(key.clone());
                }
            }
        }

        for k in dead_keys {
            if let Some(vs) = store.remove(&k) {
                removed += vs.len();
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_visibility() {
        let engine = MemKvEngine::new();
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
        let engine = MemKvEngine::new();
        let k = Bytes::from("k");

        let t1 = TxnId::new();
        engine
            .write_intent(t1, k.clone(), Bytes::from("v1"))
            .unwrap();
        engine.commit(t1, 5).unwrap();

        let t2 = TxnId::new();
        engine
            .write_intent(t2, k.clone(), Bytes::from("v2"))
            .unwrap();
        engine.commit(t2, 10).unwrap();

        // With no reader below 10, the version at ts=5 is reclaimable.
        let removed = engine.garbage_collect(10).unwrap();
        assert_eq!(removed, 1);
        // Latest value still readable.
        assert_eq!(
            engine.get(k.as_ref(), 10).unwrap().unwrap(),
            Bytes::from("v2")
        );

        // Idempotent: a second pass reclaims nothing.
        assert_eq!(engine.garbage_collect(10).unwrap(), 0);
    }
}
